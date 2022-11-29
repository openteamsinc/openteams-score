import os
import subprocess
from datetime import datetime

import numpy as np
import pandas as pd
from dotenv import dotenv_values
from scipy.stats import zscore

from community.score_calculation.community_subscore import Community
from db_connect.libio_rds import LibioData
from popularity.score_calculation.popularity_subscore import (
    get_popularity_json_cols,
    popularity_score,
)
from utils import columns2json, fillna_json_cols, setup_logger
from versioning.versioning import Versioning

security_col = "security_openteams_score"

path = os.environ["LOG_PATH"]
LOGGER = setup_logger("openteams_score", path + "/openteams_score.log")


class OpenTeamsScore(object):

    """
    This class loads all subscores and
    processes them to produce a final
    score of a project that accounts for 5 aspects:
        - community
        - popularity
        - security
        - licensing
        - versioning
    """

    def __init__(
        self,
        security_csv,
        license_csv,
        popularity_csv,
        community_csv,
        version_data_csv,
        version_count_csv,
    ):
        """
        Load all subscore tables.
        """
        self.version_data_csv = version_data_csv
        self.community_csv = community_csv
        self.popularity_csv = popularity_csv
        self.security_csv = security_csv
        self.license_csv = license_csv
        self.version_count_csv = version_count_csv

    def overall_score(self):
        """
        Compute the aggregated OpenTeams score of a project based on 5 subscores with associated coefficients:
            - security   - 0.2
            - license    - 0.2
            - popularity - 0.2
            - community  - 0.2
            - versioning - 0.2
        """
        LOGGER.info("Start calculating aggregated score ...")
        self.compute_subscores()
        joint = self._joint_subscores()
        components = [
            "security_openteams_score",
            "permissiveness_score",
            "popularity_openteams_score",
            "community_openteams_score",
            "versioning_openteams_score",
        ]

        score = 0.2 * joint[components].sum(axis=1)
        joint["openteams_score"] = round(score).astype(int)
        joint["datetime_score_last_calculated"] = datetime.now()
        self.save_scores(joint)
        self.projects = fillna_json_cols(self.projects)
        self.projects = self.projects.drop_duplicates("repository_id").reset_index()
        LOGGER.info("Aggregated score calculation completed successfully")

        return self.projects

    def save_scores(self, joint):
        joint.rename(
            columns={
                "full_name": "project_name",
                "security_openteams_score": "security",
                "community_openteams_score": "community",
                "popularity_openteams_score": "popularity",
                "permissiveness_score": "license",
                "versioning_openteams_score": "version",
            },
            inplace=True,
        )
        components = ["security", "license", "community", "popularity", "version"]
        jsons = [c + "_json" for c in components]
        cols = (
            [
                "repository_id",
                "project_name",
                "openteams_score",
                "datetime_score_last_calculated",
            ]
            + components
            + jsons
        )
        self.projects = joint[cols]

    def compute_subscores(self):
        """
        Compute all 5 components scores.
        """
        LOGGER.info("Start calculating component scores ...")
        self.security_score()
        self.license_score()
        self.community_score()
        self.popularity_score()
        self.versioning_score()
        LOGGER.info("Component scores calculation successfully completed.")

    def _joint_subscores(self):
        """
        Create a dataframe containing all
        component scores.
        """
        LOGGER.info("Start joining component tables ...")
        rid = "repository_id"

        joint = pd.merge(
            self.community[
                [rid, "full_name", "community_openteams_score", "community_json"]
            ],
            self.popularity[[rid, "popularity_openteams_score", "popularity_json"]],
            on="repository_id",
            how="outer",
        )
        joint = pd.merge(
            joint,
            self.security[[rid, "security_openteams_score", "security_json"]],
            on="repository_id",
            how="outer",
        )
        joint = pd.merge(
            joint,
            self.license[[rid, "permissiveness_score", "license_json"]],
            on="repository_id",
            how="outer",
        )
        joint = pd.merge(
            joint,
            self.versioning[[rid, "versioning_openteams_score", "version_json"]],
            on="repository_id",
            how="outer",
        )
        LOGGER.info("Component table joins successfully completed")
        return joint

    def security_score(self):
        """
        Compute the security scan score and
        Up scale it by 10 to be within [0 - 100]
        as for other scores.

        This method uses a subprocess to run the external Scorecard
        app that calculate security scores for each project in the table,
        and thus, it takes a while to run.
        """
        LOGGER.info("Start calculating security score ...")
        self.security = pd.read_csv(self.security_csv)
        scores = self.security.security_score.apply(lambda x: str(x).split("/")[0])
        score_col = "security_openteams_score"
        self.security[score_col] = (scores.astype(float)) * 10
        self.security["security_json"] = columns2json(
            self.security, score_col, security_col
        )
        LOGGER.info("Security score calculation successfully completed.")
        return self.security[score_col]

    def license_score(self):
        """
        This method retrieves the score of each license
        and assigns it to the project to be used in the
        aggregate OpenTeams score.
        """
        LOGGER.info("Start calculating license score ...")

        self.license = pd.read_csv(self.license_csv)
        score_col = "permissiveness_score"
        zscore_ = np.round(zscore(self.license[score_col], nan_policy="omit"), 2)
        self.license[score_col + "_zscore"] = zscore_
        self.license["license_json"] = columns2json(
            self.license, score_col, security_col
        )

        LOGGER.info("License score calculation successfully completed.")
        return self.license["permissiveness_score"]

    def popularity_score(self):
        """
        Computes the popularity subscore by multiplying by the below coefficients.
        """
        LOGGER.info("Start calculating Popularity score ...")

        self.popularity = pd.read_csv(self.popularity_csv)
        df = self.popularity.apply(popularity_score, axis=1)
        score_col = "popularity_openteams_score"

        self.popularity[score_col] = df[score_col]
        for col in get_popularity_json_cols():
            df[col + "_zscore"] = np.round(zscore(df[col], nan_policy="omit"), 2)

        self.popularity["popularity_json"] = columns2json(df, score_col, security_col)
        LOGGER.info("Popularity score calculation successfully completed.")

        return self.popularity[score_col]

    def community_score(self):
        """
        Compute the community subscore with the following rules:
            - Boolean fields (documentation, contribution guidelines
                and readme) counts for 30 points;

            - Non boolean fields (open/closed issues and PRs)
                counts for 70 points.
        """
        LOGGER.info("Start calculating Community score ...")
        community_obj = Community(self.community_csv)
        self.community = pd.read_csv(self.community_csv)
        score_col = "community_openteams_score"
        self.community[score_col] = community_obj.score()

        community_obj.calculate_json_zscore()
        self.community["community_json"] = columns2json(
            community_obj.data, score_col, security_col
        )
        LOGGER.info("Community score calculation successfully completed.")

        return self.community[score_col]

    def versioning_score(self):
        """
        Computes the popularity subscore by multiplying by the below coefficients.
        """
        LOGGER.info("Start calculating Versioning score ...")

        versioning = Versioning(self.version_data_csv, self.version_count_csv)

        score = versioning.score()
        score_col = "versioning_openteams_score"
        self.versioning = versioning.versioning

        self.versioning[score_col] = score
        fname = "/".join(self.version_data_csv.split("/")[:-1]) + "/version.csv"
        self.versioning.to_csv(fname, index=None)

        versioning.compute_component_zscores()
        self.versioning["version_json"] = columns2json(
            versioning.versioning, score_col, security_col
        )
        LOGGER.info("Versioning score calculation successfully completed.")

        return self.versioning[score_col]

    def save_agg_score_to_db(
        self, env, forward=True, table="openteams_score", overwrite=False
    ):
        """
        Save the final/aggregated score to the database
        """
        LOGGER.info("Attempt to save project scores to the database ...")
        try:

            connection = LibioData(env, forward=forward).db
            res = connection.insert(table, self.projects, overwrite=overwrite)
            LOGGER.info(
                "The aggregated score has been successfully saved into the database"
            )

        except Exception as e:
            LOGGER.exception(
                f"An error occurred while connecting and saving scores to the database: {e}"
            )
            raise Exception(e)


if __name__ == "__main__":

    path = "data/sample_data/cache/"
    print(path)
    community_csv = path + "community.csv"
    popularity_csv = path + "popularity.csv"
    security_csv = path + "security.csv"
    license_csv = path + "license.csv"
    versioning_csv = path + "version_data.csv"
    projects_csv = path + "projects.csv"
    phs = OpenTeamsScore(
        security_csv,
        license_csv,
        popularity_csv,
        community_csv,
        versioning_csv,
        projects_csv,
    )

    openteams_score = phs.overall_score()
    print(phs.projects.T)
