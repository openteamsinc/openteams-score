import enum
from collections import OrderedDict
from datetime import datetime

import numpy as np
import pandas as pd
from scipy.stats import zscore

from db_connect.libio_rds import LibioData
from utils import date2days, logscale, s_shape


class Release(enum.Enum):
    MAJOR = 4
    MINOR = 3
    PATCH = 2
    OTHER = 1
    FIRST = 0


def none_nat2nan(x):
    """Convert 0/None/NaT to np.nan
    This function is used to implicitly replace
    deltatimes equal to 0/None/NaT (due to missing dates)
    to the mean of non missing ones of the same project.
    It is done by the use of `np.nanmean` in the method:
    `_meantime_weighted_sum` of Versioning class.

    Args:
        x: float|None|NaT
            represents mainly the delta time in days between
            two dates

    Returns:
        - `np.nan` if x equals None or NaT or 0.0
        - x otherwise
    """
    if (pd.isna(x)) or (x is None) or (x == 0.0):
        return np.nan
    else:
        return x


class Versioning:
    """
    This class calculates the versioning score
    component of an OSS based on the sub-components:
        - weighted count of versions
        - weighted mean time between consecutive versions
        - weighted frequency of version updates
    """

    def __init__(self, version_data_csv, version_count_csv):
        self.version = pd.read_csv(version_data_csv)
        self.versioning = pd.read_csv(version_count_csv)

        v_cols = [
            "project_id",
            "number",
            "published_at",
            "runtime_dependencies_count",
            "updated_at",
        ]
        self.version = pd.merge(self.version[v_cols], self.versioning, on="project_id")

        self.version["published_at"] = self.version.published_at.apply(pd.to_datetime)
        self.version["updated_at"] = self.version.updated_at.apply(pd.to_datetime)
        cols = ["project_id", "platform", "versions_count", "updated_at"]
        self.versioning = self.version.groupby("repository_id").agg(
            {col: max for col in cols}
        )
        self.versioning.reset_index(inplace=True)
        self._set_version_series()

    def _clean_version_data(self, version):
        """Remove versions with number containing
        `rc`, `dev` or `nightly`

        Args:
            version: DataFrame
                version dataframe

        Returns:
            version DataFrame without release or version number
            containing one of: `rc`, `dev`, `nightly`.
        """
        indexes_to_remove = version[version.number.str.contains("rc|dev|nightly")].index
        cleaned_version = version.drop(labels=indexes_to_remove, axis=0)
        if len(cleaned_version) > 0:
            return cleaned_version
        else:
            # When all versions have one of `rc`, `dev` and `nightly`
            # then we don't clean them.
            return version

    def _set_version_series(self):
        """
        Creates the date series of version numbers associated
        with each project
        """
        self.date_series = {}
        self.parallel_date_series = {}
        for id in self.versioning.repository_id:
            repo_versions = self.version[self.version.repository_id == id].sort_values(
                by="published_at", ascending=True
            )
            repo_versions = self._clean_version_data(repo_versions)
            repo_versions.reset_index(inplace=True)
            self._build_series(repo_versions, id)

    def _build_series(self, repo_versions, id):
        self.date_series[id] = []
        self.parallel_date_series[id] = []
        prev_num_parts = self._split_version_num(repo_versions.loc[0, "number"])
        for curr_num in repo_versions.number:
            curr_num_parts = self._split_version_num(curr_num)
            version_type = self._find_version_type(prev_num_parts, curr_num_parts)
            self._update_series(repo_versions, curr_num, version_type)
            prev_num_parts = curr_num_parts
        has_parallel_versions, major_nums = self.check_parallel_versions(
            self.date_series[id]
        )
        if has_parallel_versions:
            self.parallel_date_series[id] = self.track_parallel_versions(
                self.date_series[id], major_nums
            )

    def check_parallel_versions(self, date_series):
        """Check if multiple versions are maintained in parallel.
        We assume that if there is a decrease in major version series at least
        3 times then multiple versions are maintained
        e.g: '2.0.1' -> '1.0.1' -> '3.0.2' -> '2.0.3' -> '1.0.5'
        major versions are: 2 -> 1 -> 3 -> 2 -> 3 -> 1.
        We noticed 3 decreases: 2 -> 1, 3-> 2 and 3 -> 1

        Args:
            date_series: list
                List of triplet (datetime, Release, version_number)
                ordered by datetime

        Returns: tuple (bool, list)
            (True, list(str)): if multiple versions are maintained in parallel and
            the list of major version numbers

            (False, []): otherwise and an empty list

        """
        majors = [date[2].split(".")[0] for date in date_series]
        n_occurrences = 0
        for i in range(len(majors) - 1):
            if majors[i] > majors[i + 1]:
                n_occurrences += 1
                if n_occurrences >= 3:
                    return True, list(set(majors))
        return False, []

    def track_parallel_versions(self, date_series, major_versions):
        """Build separate date series for each parallel version

        Args:
            date_series: list
                List of triplet (datetime, Release, version_number)
                ordered by datetime

            major_versions: list
                List of major version numbers (str)

        Returns:
            A dictionary where keys are major version numbers and values
            are the associate date series
        """
        parallel_version_date_series = {k: [] for k in major_versions}
        for elt in date_series:
            major_num = elt[2].split(".")[0]
            parallel_version_date_series[major_num] += [elt]
        return OrderedDict(sorted(parallel_version_date_series.items()))

    def _split_version_num(self, num):
        """
        splits version number into 3 parts
        """
        num_parts = num.split(".")
        num_parts += ["0"] * (3 - len(num_parts))
        return [num_parts[0], num_parts[1], ".".join(num_parts[2:])]

    def _find_version_type(self, prev_num_parts, curr_num_parts):
        """Finds the type of the release between two versions

        Args:
            prev_num_parts: list(str)
                an array of string containing dot separated
                parts of the previous version ('1.0.0.1' -> ['1', '0', '0.1'])

            curr_num_parts: list(str)
                same as `prev_num_parts` but fo rthe current version

        Returns:
            The kind of release from `prev_num_parts` to `curr_num_parts`
        """
        if prev_num_parts[0] != curr_num_parts[0]:
            return Release.MAJOR
        if prev_num_parts[1] != curr_num_parts[1]:
            return Release.MINOR
        if prev_num_parts[2] != curr_num_parts[2]:
            return Release.PATCH
        return Release.FIRST

    def _update_series(self, repo_versions, version_num, version_type=1):
        date = repo_versions.loc[
            repo_versions.number == version_num, "published_at"
        ].iloc[0]
        repo_id = repo_versions.loc[0, "repository_id"]
        date_item = (pd.to_datetime(date), version_type, version_num)
        self.date_series[repo_id] += [date_item]

    def score(self):
        """
        Calculates the versioning score
        """
        self.number_version_score()
        self.version_meantime_score()
        self.version_frequency()
        n = 3.0
        n -= (self.versioning["weighted_meantime"] == 0.0).astype(int)
        n -= (self.versioning["weighted_freq"] == 0.0).astype(int)
        score = (
            100
            * np.nansum(
                [
                    self.number_version_score(),
                    self.version_meantime_score(),
                    self.version_frequency(),
                ],
                axis=0,
            )
            / n
        )
        score = np.round(score, 2)
        self.versioning["versioning_openteams_score"] = score
        return score

    def number_version_score(self):
        """
        Retrieves the total number of versions
        released for each project
        """
        self.versioning["major_count"] = 0.0
        self.versioning["minor_count"] = 0.0
        self.versioning["patch_count"] = 0.0
        scores = [
            self._num_versions_weighted_sum(id) for id in self.versioning.repository_id
        ]
        scores = np.round(s_shape(logscale(np.array(scores))), 2)
        self.versioning["weighted_count"] = scores

        return self.versioning["weighted_count"]

    def _num_versions_weighted_sum(self, repo_id):
        majors, minors, patches = 0, 0, 0
        for date in self.date_series[repo_id]:
            if date[1] == Release.MAJOR:
                majors += 1
            elif date[1] == Release.MINOR:
                minors += 1
            elif date[1] == Release.PATCH:
                patches += 1
        condition = self.versioning.repository_id == repo_id
        self.versioning.loc[condition, "major_count"] = majors
        self.versioning.loc[condition, "minor_count"] = minors
        self.versioning.loc[condition, "patch_count"] = patches
        return (5 * majors + 2 * minors + patches) / 8.0

    def version_meantime_score(self):
        """
        Calculates the mean time between consecutive versions
        in days.
        """
        self.versioning["major_meantime"] = 0.0
        self.versioning["minor_meantime"] = 0.0
        self.versioning["patch_meantime"] = 0.0

        scores = [
            self._meantime_weighted_sum(id) for id in self.versioning.repository_id
        ]
        self.versioning["weighted_meantime"] = scores

        return self.versioning["weighted_meantime"]

    def _meantime_weighted_sum(self, repo_id):
        if len(self.date_series[repo_id]) < 2:
            return 0.0
        majors, minors, patches = self._build_deltatime_series(repo_id)
        means = [
            np.round(np.nanmean(v), 2) if len(v) > 0 else np.nan
            for v in [majors, minors, patches]
        ]
        condition = self.versioning.repository_id == repo_id
        self.versioning.loc[condition, "major_meantime"] = means[0]
        self.versioning.loc[condition, "minor_meantime"] = means[1]
        self.versioning.loc[condition, "patch_meantime"] = means[2]
        weights = [6, 4, 2]
        components = [
            w / means[i] if not (np.isnan(means[i]) or means[i] == 0) else 0.0
            for i, w in enumerate(weights)
        ]
        res = s_shape(logscale(sum(components)))
        return np.round(res, 2)

    def _build_deltatime_series(self, repo_id):

        if len(self.parallel_date_series[repo_id]) > 0:
            return self._build_parallel_deltatime_series(repo_id)

        majors, minors, patches = [], [], []
        for i, date in enumerate(self.date_series[repo_id]):
            if i > 0:
                deltatime = none_nat2nan(date2days(date[0] - prev_date))
                if date[1] == Release.MAJOR:
                    majors += [deltatime]
                elif date[1] == Release.MINOR:
                    minors += [deltatime]
                elif date[1] == Release.PATCH:
                    patches += [deltatime]
            prev_date = date[0]
        return majors, minors, patches

    def _build_parallel_deltatime_series(self, repo_id):
        """Build the delta time series for parallel versions

        Args:
            repo_id: int
                repository unique id

        Returns: tuple(list,list, list)
            list of delta time respectively for major, minor
            and patch releases
        """
        majors, minors, patches = [], [], []
        date_series = self.parallel_date_series[repo_id]
        for i, major_num in enumerate(date_series):
            if i > 0:
                deltatime = date2days(date_series[major_num][0][0] - prev_date)
                majors += [none_nat2nan(deltatime)]
            prev_date = date_series[major_num][0][0]
            repo_id_minors, repo_id_patches = self._get_minor_patch_deltatimes(
                date_series[major_num]
            )
            minors += repo_id_minors
            patches += repo_id_patches
        return majors, minors, patches

    def _get_minor_patch_deltatimes(self, major_date_series):
        """build the minor and patch date series from
        a major version associated date series (e.g: 3.X.Y)

        Args:
            version_dates:
        """
        minors, patches = [], []
        for i, elt in enumerate(major_date_series):
            date, _, version_num = elt
            num_parts = self._split_version_num(version_num)
            if i > 0:
                if num_parts[1] > prev_num_parts[1]:
                    minors += [none_nat2nan(date2days(date - prev_minor_date))]
                    prev_patch_date = date
                    prev_minor_date = date
                elif num_parts[2] > prev_num_parts[2]:
                    patches += [none_nat2nan(date2days(date - prev_patch_date))]
                    prev_patch_date = date
            else:
                prev_minor_date = date
                prev_patch_date = date
                prev_num_parts = num_parts

        return minors, patches

    def version_frequency(self, fromdate=datetime.now()):
        """
        Calculate the frequency of released versions from
        1st release to last update of the libio database
        """
        if "weighted_count" not in self.versioning.columns:
            self.number_version_score()
        get_age = lambda row: date2days(
            row["updated_at"] - self.date_series[row["repository_id"]][0][0]
        )
        ages = self.versioning.apply(get_age, axis=1)
        self.versioning["major_freq"] = np.round(
            self.versioning["major_count"] / (ages / 365), 2
        )
        self.versioning["minor_freq"] = np.round(
            self.versioning["minor_count"] / (ages / (365 / 3)), 2
        )
        self.versioning["patch_freq"] = np.round(
            self.versioning["patch_count"] / (ages / (365 / 18)), 2
        )
        res = s_shape(
            logscale(self.versioning["minor_freq"] + self.versioning["patch_freq"])
        )
        self.versioning["weighted_freq"] = np.round(res, 2)
        return self.versioning["weighted_freq"]

    def get_json_cols(self):
        cols = [
            "major_meantime",
            "minor_meantime",
            "patch_meantime",
            "major_count",
            "minor_count",
            "patch_count",
            "major_freq",
            "minor_freq",
            "patch_freq",
            "versioning_openteams_score",
        ]
        return cols

    def compute_component_zscores(self):
        for col in self.get_json_cols():
            zscore_ = np.round(zscore(self.versioning[col], nan_policy="omit"), 2)
            self.versioning[col + "_zscore"] = zscore_
