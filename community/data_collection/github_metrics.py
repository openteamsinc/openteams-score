import numpy as np
import requests
import random

class GithubMetrics:

    """
    A class to examine pull community metrics from the gh api.
    """

    def __init__(self, logger, tokens=None):
        self.tokens = tokens
        self.logger = logger

    def set_repo(self, repo_name):
        token = random.choice(self.tokens)
        self.headers = {"Authorization": f"token {token}"}
        self.repo_name = repo_name

    def get_contributor_stats(self):
        """
        This function retrieves the number of
        contributors gathering 50% of all commits.
        The higher it is the better it is (less fragile).
        The lower it is the worst it is (very fragile).
        """
        self.logger.info(f'Retrieving contributor stats for {self.repo_name}.')
        try:
            response = requests.get(headers=self.headers, url=f'https://api.github.com/repos/{self.repo_name}/stats/contributors')
            totals = np.array([c['total'] for c in response.json()])
            score = self._fragility_score(totals)
            self.logger.info(f'Successfully pulled contributor stats for {self.repo_name}.')
            return score
        except Exception as e:
            self.logger.exception(f'Contributor stats for the project {self.repo_name} failed with error: {e}')
            return None

    def get_weekly_commits(self):
        """
        This function retrieves the number of commits for each week of
        the last year. We simply return the weekly average.
        """
        try:
            response = requests.get(headers=self.headers, url=f'https://api.github.com/repos/{self.repo_name}/stats/participation')
            out = np.mean(response.json()['all'])
            self.logger.info(f'Successfully pulled weekly commits for {self.repo_name}.')
            return out
        except Exception as e:
            self.logger.exception(f'Weekly commits for the project {self.repo_name} failed with error: {e}')
            return None

    def _fragility_score(self, commit_stats):
        """
        helper method to calculates the highest number of contributors
        accounting for less than 55%  of all commits.
        """
        commit_rates = np.sort(commit_stats/np.sum(commit_stats))[::-1]
        cumul_freq = np.cumsum(commit_rates)
        return np.sum(cumul_freq < 0.55)


def get_metrics(df, tokens, logger):

    names = df['full_name'].tolist()

    contrib_stats = []
    weekly_commits = []
    metrics = GithubMetrics(logger, tokens)
    for name in names:
        stats = None
        commits = None
        try:
            metrics.set_repo(name)
            stats = metrics.get_contributor_stats()
            commits = metrics.get_weekly_commits()
            if commits != None: commits = round(commits, 2)
        except Exception as e:
            logger.exception(f'Processing repository "{name}" failed with error: {e}')
        finally:
            contrib_stats.append(stats)
            weekly_commits.append(commits)

    df['weekly_commits'] = weekly_commits
    df['contrib_stats'] = contrib_stats
    return df