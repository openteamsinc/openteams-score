import numpy as np
import pandas as pd
from scipy.stats import zscore
from utils import logscale, s_shape



def popularity_score(df: pd.Series):
    """
    Computes the popularity subscore by multiplying by the below coefficients.
    """

    if not np.isnan(df['recent_downloads']):
        coefs = np.array([15,  5.,  40., 10., 15., 5., 10.])
        columns = ['contributions_count', 'subscribers_count', 'dependent_repos_count',
                'stargazers_count', 'dependents_count', 'forks_count', 'recent_downloads']
    elif not np.isnan(df['total_downloads']):
        coefs = np.array([15,  10.,  40., 10., 15., 5., 5.])
        columns = ['contributions_count', 'subscribers_count', 'dependent_repos_count',
                'stargazers_count', 'dependents_count', 'forks_count', 'total_downloads']
    else:
        coefs = np.array([15,  5.,  40., 15., 20., 5.])
        columns = ['contributions_count', 'subscribers_count', 'dependent_repos_count',
                'stargazers_count', 'dependents_count', 'forks_count']

    A = s_shape(logscale(df[columns].astype(float)))
    df['popularity_openteams_score'] = round(A.dot(coefs), 2)
    return df

def get_popularity_json_cols():
    cols = ['contributions_count', 'subscribers_count', 'dependent_repos_count',
            'stargazers_count', 'dependents_count', 'forks_count', 'recent_downloads',
            'total_downloads', 'popularity_openteams_score']
    return cols

if __name__ == '__main__':
    df_pop = pd.read_csv('../data/db_tables/popularity_table.csv')
    print(df_pop.tail(10))
    df_clean = df_pop.fillna(0)
    score = popularity_score(df_clean)
    df_pop['popularity_openteams_score'] = score
    json_cols = ['popularity_openteams_score', 'total_downloads', 'recent_downloads']
    df_pop['popularity_score_json'] = df_pop.apply(lambda row: row[json_cols].to_json(), axis=1)
    print(df_pop)
    print(score.max(), score.min(), score.median(), score.mean())
