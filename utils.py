import pandas as pd
import numpy as np
from functools import partial
import json
import os
import re
import logging
from datetime import datetime, timedelta
from db_connect.libio_rds import LibioData



def load_gh_tokens(path):
    tokens = open(path, 'r').readlines()
    tokens = [t[:-1] if '\n' in t else t for t in tokens]
    return tokens

def license_score(lic, license_dict):
    if lic in license_dict:
        return license_dict[lic]
    return None

def create_dir(path_to_dir):
    if not os.path.exists(path_to_dir):
        os.mkdir(path_to_dir)


def setup_logger(name, log_file, level=logging.INFO):
    """To setup as many loggers as you want"""

    handler = logging.FileHandler(log_file)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    if (logger.hasHandlers()):
        logger.handlers.clear()

    logger.addHandler(handler)

    return logger

def binarize_field(value:str):
        """
        Binarize inputs

        Returns: 0 if `value` is NaN or empty string
                    1 otherwise
        """
        output = 0
        if (pd.isna(value)):
            output = 0
        elif (type(value) == str and not value.isspace()):
            output = 1
        elif type(value) == bool:
            output = int(value)

        return output

def clean(x):
    """
    Convert string integers (e.g "11,000") into integers (11000)
    """
    if pd.isna(x) : return 0
    if type(x) == str and not x.isspace():
        return int(x.replace(',', ''))
    else:
        return x

def logscale(x):
    """
    Apply a log transform to reduce
    values' scale.
    Add 1 to avoid infinity.
    """
    return np.log(1.+ x)

def s_shape(x):
    """
    shrink values into [0, 1] and saturate
    later than sigmoid and tanh
    """
    return x/(1. + x)

def ratio_of_2cols(df, col1, col2):
    """
    Divide a dataframe column out of another
    and give 0 when the denominator is 0
    """
    ratio = df.apply(lambda row : row[col1]/row[col2] if row[col2] != 0 else 0, axis=1)
    return s_shape(ratio)

def columns2json(df, score_col, sec_score_col):
    """
    Transform all columns into json and put it in
    dedicated column.
    """
    def col2json_part(df, score_col, cols):
        json_cols = {}
        json_cols['component_score'] = {'score': stringify_nan(df[score_col])}
        if score_col+'_zscore' in df.keys():
            json_cols['component_score']['z-score'] = stringify_nan(df[score_col+'_zscore'])
        cols = list(cols)
        cols.remove(score_col)
        if score_col == sec_score_col:
            res = [sec_subscore(c, df) for c in cols]
        else:
            res = []
            for c in cols:
                item = {'column': c, "actual_value": stringify_nan(df[c])}
                if c+'_zscore' in df.keys(): item["z-score"] = stringify_nan(df[c+'_zscore'])
                res += [item]

        json_cols['subcomponents_score'] = res
        return json.dumps(json_cols)

    cols = select_columns(df.columns)
    func = partial(col2json_part, score_col=score_col, cols=cols)
    return df.apply(func, axis=1)

def sec_subscore(col, df_sec):
    res = {"column": col}
    try:
        col_dict = eval(df_sec[col])
        res["actual_value"] = col_dict['score']
        res['reason'] = col_dict['reason']
        res['description'] = col_dict['documentation']['short']
    except:
        res["actual_value"] = stringify_nan(df_sec[col])
    finally:
        return res

def add_zscore(df, col):
    res = stringify_nan(df[col+'_zscore'])
    return res

def stringify_nan(x):
    if pd.isna(x):
        return "NaN"
    else: return x

def select_columns(columns):
    cols = [ c for c in columns if 'zscore' not in c]
    unused_cols = ["project_id","repository_id","project_name", "full_name", "platform",
                   "rank","score", "updated_at", "name", "weighted_count", "weighted_meantime",
                   "weighted_freq","major_freq", "repository_url", "security_score",
                   "homepage", "has_readme", "has_contributing", "score_last_calculated"]
    for c in unused_cols:
        try:
            cols.remove(c)
        except:
            continue
    return cols


def fillna_json_cols(df):
    """
    Transform null entry in Json column
    to a Json object filled with NaN for all
    values.
    """
    cols = list(df.columns)
    for col in cols:
        if '_json' in col and df[col].isna().sum() > 0 :
            json_obj = df[df[col].notnull()][col][0]
            nan_json = build_nan_json(json_obj)
            df[col].fillna(nan_json, inplace=True)
    return df

def build_nan_json(json_obj):
    """
    Builds the Json object with NaN
    for any value
    """
    nums = list(set(re.findall('(-*\d+(?:\.\d+)?)', json_obj)))
    nums = list(sorted(nums, key = len))
    nums.reverse()
    res = json_obj
    for n in nums:
        res = res.replace(n, '"NaN"')
    return res

def date2days(date:datetime):
    return date.total_seconds()/(3600*24)


def pull_project_list(db_env, path, filename):
    """
    Pull the list of projects from the database with
    credentials encapsulated in `db_env`
    dates is the list of date for querying the database
    `path` is the path to the folder where csv files of
    projects' list will be stored.
    """
    conn = LibioData(db_env)
    projects = conn.db.table('projects')
    repositories = conn.db.table('repositories')

    cols = ['id','repository_id',  'repository_url', 'pushed_at', 'full_name',
            'host_type', 'stargazers_count', 'contributions_count']
    project_repos = projects.join(repositories, predicates=(projects['repository_id']==repositories['id'] ))
    project_repos = project_repos[projects, repositories['pushed_at'], repositories['full_name'], repositories['host_type'],
                                  repositories['stargazers_count'], repositories['contributions_count'] ]
    table = project_repos[cols]

    date = str(datetime.now())
    four_years_ago_date = str(datetime.now() - timedelta(days=4*365))
    condition = (table['pushed_at'] > four_years_ago_date) & (table['pushed_at'] <= date)
    filename = f"{filename}_{str(date).split()[0]}.csv"
    projects = table[condition].execute(limit=None)
    print('projects cols:', projects.columns)
    duplicates = projects.duplicated(subset=['repository_id', 'full_name', 'host_type',
                                     'stargazers_count', 'contributions_count'])
    print('projects cols after removing duplicates:', projects[~duplicates].columns)

    projects['star_contrib'] = projects['stargazers_count'] + projects['contributions_count']
    projects[~duplicates].sort_values(by='star_contrib', ascending=False).to_csv(f'{path}/{filename}', index=None)
    print(f"file : {filename} saved at: {str(datetime.now()).split('.')[0]} ")

