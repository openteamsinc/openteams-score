import json
import os
from glob import glob

import pandas as pd
from dotenv import dotenv_values
from google.cloud import bigquery
from google.oauth2 import service_account

sec_fields = [
    "Webhooks",
    "Maintained",
    "Code-Review",
    "CII-Best-Practices",
    "Vulnerabilities",
    "Packaging",
    "Token-Permissions",
    "Dangerous-Workflow",
    "Dependency-Update-Tool",
    "Binary-Artifacts",
    "License",
    "Pinned-Dependencies",
    "Security-Policy",
    "Signed-Releases",
    "Branch-Protection",
    "Fuzzing",
]


class SecurityBigQuery:
    """Class to extract security data from BigQuery security dataset.



    Attributes:
        json_filenames: list(str)
            List of paths to all json files containing BigQuery security dataset.

         projects_csv: str
            Path to the csv file conataining the list of filtered LIBIO projects

        prefix_json: str
            Common prefix of security json data from bigquery dataset
    """

    def __init__(self, json_filenames, projects_csv, sec_fields=sec_fields):
        self.json_files = json_filenames
        self.projects_csv = projects_csv
        self.sec_fields = sec_fields
        self.df_security = None

    def json_security_data_to_df(self, project_names):
        """Extract json security data of LIBIO projects
        found in Bigquery JSON security data and put it into
        a dataframe.

        Args:
            project_names: list(str)
                List of LIBIO project names to look security data for.

        Returns:
            The dataframe of security data of provided project names.
        """
        sec_data = []
        for i, json_file in enumerate(self.json_files):
            file_data = open(json_file, "r")
            lines = file_data.readlines()
            json_data_all = [json.loads(line) for line in lines]
            sec_data += [
                self.json_data2df(d)
                for d in json_data_all
                if d["repo"]["name"][11:] in project_names
            ]
            print(f"{json_file.split('/')[-1]} processed.")

        self.df_security = pd.DataFrame(sec_data)
        return self.df_security

    def json_data2df(self, json_data):
        """Transform a JSON security data into a dictionary
        where keys are security information

        Args:
            json_data: dict
                security data of a single repository

        Returns:
            A dictionary (dict) of security data with security information as keys.
        """
        row = {
            "full_name": json_data["repo"]["name"].split("github.com/")[1],
            "repository_url": f'https://{json_data["repo"]["name"]}',
            "security_score": json_data["score"],
            **{k: {} for k in self.sec_fields},
        }
        for check in json_data["checks"]:
            if check["name"] not in sec_fields:
                raise Exception(f'{check["name"]} not in {self.sec_fields}')
            row[check["name"]] = check
        return row

    def get_projects_security_data(self):
        """Retrieve LIBIO projects' security data found
        in JSON files from BigQuery security data

        Returns:
            A dataframe containing security data of LIBIO projects found
            in BigQuery security data
        """
        project_names = pd.read_csv(self.projects_csv).full_name.tolist()
        if self.df_security == None:
            self.json_security_data_to_df(project_names)
        common_projects = set(project_names) & set(self.df_security.full_name)
        df_common = self.df_security[
            self.df_security.full_name.isin(list(common_projects))
        ]
        df_common = pd.merge(df_common, projects, on="full_name", how="left")
        return df_common

    def get_missing_projects(self):
        """Retrieve LIBIO projects' security data found
        in JSON files from BigQuery security data

        Returns:
            A dataframe containing security data of LIBIO projects not found
            in BigQuery security data
        """
        projects = pd.read_csv(projects_csv).drop_duplicates().reset_index()
        if self.df_security == None:
            self.json_security_data_to_df(projects.full_name.tolist())
        missed_projects = set(self.projects.full_name) - set(self.df_security.full_name)
        df_missed = projects[projects.full_name.isin(list(missed_projects))]
        return df_missed

    def fetch_data(self, credential_json):
        """Run a query to retrieve all
        BigQuery projects' security data

        Returns:
            dataframe containing security data
        """
        self.credentials = service_account.Credentials.from_service_account_file(
            credential_json
        )
        self.bqclient = bigquery.Client(credentials=self.credentials)
        query_string = """
SELECT repo, checks, score
FROM `openssf.scorecardcron.scorecard-export-weekly-json`
"""
        dataframe = self.bqclient.query(query_string).result().to_dataframe()
        return dataframe


if __name__ == "__main__":
    path = os.path.dirname(__file__)
    score_dir = f"{path}/../../scorecard-weekly-7-15-22/json-files"
    projects_csv = (
        f"{path}/../data/filtered_projects/project_list_stargazers_2022-06-10.csv"
    )
    json_filenames = sorted(glob(f"{score_dir}/scorecard-weekly-*.json"))

    sbq = SecurityBigQuery(json_filenames, projects_csv)
    df_missed = sbq.get_missing_projects()
    print(f"No of missing projects: {len(df_missed)}")
    df_missed.to_csv(
        f"{path}/../data/filtered_projects/projects_missing_security_data.csv",
        index=None,
    )
