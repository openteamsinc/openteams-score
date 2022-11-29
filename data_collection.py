# Run data collection for all of the score components

import pandas as pd
import os
import ray
import logging
from datetime import datetime
from db_connect.libio_rds import LibioData
from popularity.data_collection.project_downloads import DownloadStats
from community.data_collection.github_metrics import get_metrics
from community.data_collection.github_scrape import download_all
from community.data_collection.stack_query import StackQueryManager
from community.data_collection.twitter_query import TwitterQuery
from security.security_score import SecurityScore
from openteams_score import OpenTeamsScore
from governance.score_calculation.governance_calculation import get_project_governance_infos
from dotenv import dotenv_values
from utils import setup_logger, load_gh_tokens, license_score, create_dir, pull_project_list, create_dir
from healthcheck import HealthCheck


hc = HealthCheck()


def load_tables_from_db(project_name_list, db_env, logger):
    """
    Loads OpenTeams score components of projects listed in
    `projects_csv`
    """
    try:
        hc.send_ping("start")
        logger.info('Connecting to RDS and collecting data.')
        try:
            data = LibioData(db_env)
            popularity, community, licenses, security, version_data, \
            version_count_data = data.get_projects(project_name_list)
        except Exception as e:
            logger.exception(f'RDS connection and collection failed with error {e}.')
            raise e
        logger.info('Successfully gathered data from RDS.')
        # remove the index which is on repository_id
        popularity = popularity.reset_index()
        community = community.reset_index()
        licenses = licenses.reset_index()
        security = security.reset_index()
        version_data = version_data.reset_index()
        version_count_data = version_count_data.reset_index()
    except Exception as e:
        logger.exception(f'An exception occurs: {e}.')
        hc.send_ping("fail", data=e)
        raise e
    else:
        hc.send_ping()
    return popularity, community, licenses, security, version_data, version_count_data

def create_pop_download_stats(popularity, pop_env, log_dir):
    """
    Create the DownloadStats object for
    querying project download stats
    """
    popularity_config = dotenv_values(pop_env)
    julia_registry = popularity_config['JULIA_REGISTRY']
    bigquery_credentials = [{'json':popularity_config['BIGQUERY_CREDENTIALS'],
            'name': 'BIG_QUERY_PROJECT_NAME'}]
    # loggers for popularity are handled internally
    stats = DownloadStats(popularity, julia_registry,
                            bigquery_credentials, log_dir)
    return stats


def collect_popularity_data(popularity, popularity_file, pop_env,
                            logger, log_dir):
    """
    Collects popularity data from
    """
    try:
        hc.send_ping("start")
        logger.info('Collecting package manager downloads.')
        stats = create_pop_download_stats(popularity, pop_env, log_dir)
        stats.get_downloads()
        stats.write()
        stats.data.to_csv(popularity_file, index=None)
        logger.info('Successfully gathered data for package managers.')
    except Exception as e:
        logger.exception(f'An exception occurs: {e}.')
        hc.send_ping("fail", data=e)
    else:
        hc.send_ping()


def combine_community_files(files):
    try:
        hc.send_ping("start")
        twitter_data = pd.read_csv(files['twitter_tmp'])
        gh_data = pd.read_csv(files['gh_scrape_tmp'])
        metrics_data = pd.read_csv(files['gh_metrics_tmp'])
        stack_data = pd.read_csv(files['stack_tmp'])
        governance_data = pd.read_csv(files['governance_tmp'])
        metric_cols =  ['full_name', 'weekly_commits', 'contrib_stats', ]

        community_data = pd.merge(gh_data, twitter_data, on='full_name', how='outer')
        community_data = pd.merge(community_data, stack_data, on='full_name', how='outer')
        community_data = pd.merge(community_data, metrics_data[metric_cols], on='full_name', how='outer')
        community_data = pd.merge(community_data, governance_data, on='full_name', how='outer')
        community_data.to_csv(files['community_tmp'], index=None)
    except Exception as e:
        logger.exception(f'An exception occurs: {e}.')
        hc.send_ping("fail", data=e)
    else:
        hc.send_ping()


def collect_metrics_data(community, ghmetrics_file, metrics_env,
                         logger, log_dir):
    try:
        hc.send_ping("start")
        logger.info('Collecting community metrics using the gh api.')
        metrics_config = dotenv_values(metrics_env)
        tokens = load_gh_tokens(metrics_config['GH_TOKENS'])
        metrics_logger = setup_logger('metrics_logger', log_dir+'/metrics.log')
        metrics_data = get_metrics(community, tokens, metrics_logger)
        metrics_data.to_csv(ghmetrics_file, index=None)
    except Exception as e:
        logger.exception(f'An exception occurs: {e}.')
        hc.send_ping("fail", data=e)
    else:
        hc.send_ping()

    return metrics_data


def collect_gh_data(community, ghscrape_file,
                    logger, log_dir):
    try:
        hc.send_ping("start")
        logger.info('Collecting GitHub issues and PR data by scraping gitHub.')
        repo_logger = setup_logger('repo_logger', log_dir+'/repo.log')
        gh_data = download_all(community, repo_logger)
        logger.info('Successfully downloaded issues and pr data.')
        gh_data.to_csv(ghscrape_file, index=None)
    except Exception as e:
        logger.exception(f'An exception occurs: {e}.')
        hc.send_ping("fail", data=e)
    else:
        hc.send_ping()
    return gh_data


def collect_twitter_data(community, twitter_file, twitter_env,
                         logger, log_dir):
    try:
        hc.send_ping("start")
        logger.info('Collecting data from twitter api.')
        twitter_logger = setup_logger('twitter_logger', log_dir+'/twitter.log')
        twitter = TwitterQuery(twitter_env, twitter_logger)
        twitter_data = twitter.get_multiple_stats(community.full_name)
        logger.info('Successfully gathered data from Twitter API.')
        twitter_data.to_csv(twitter_file, index=None)
    except Exception as e:
        logger.exception(f'An exception occurs: {e}.')
        hc.send_ping("fail", data=e)
    else:
        hc.send_ping()
    return twitter_data


def collect_stack_data(community, stack_file, stack_env,
                       logger, log_dir):
    try:
        hc.send_ping("start")
        logger.info('Collecting StackOverflow data.')
        stack_logger = setup_logger('stack_logger', log_dir+'/stack.log')
        # load stackoverflow env
        stack = StackQueryManager(stack_env, stack_logger)
        stack_data = stack.make_many_queries(community.full_name)
        logger.info('Successfully gathered data from stackoverflow.')
        stack_data.to_csv(stack_file, index=None)
    except Exception as e:
        logger.exception(f'An exception occurred: {e}.')
        hc.send_ping("fail", data=e)
    else:
        hc.send_ping()

    return stack_data


def collect_governance_data(project_full_names, gov_file, gov_env, logger):
    try:
        hc.send_ping("start")
        logger.info('Collecting governance data.')
        #load env variables
        governance_config = dotenv_values(gov_env)
        gov_tokens = load_gh_tokens(governance_config['GOV_TOKENS'])
        governance_info = get_project_governance_infos(project_full_names, gov_tokens)
        logger.info('Successfully collected governance data.')
        governance_info.to_csv(gov_file, index=None)
    except Exception as e:
        logger.exception(f'An exception occurs: {e}.')
        hc.send_ping("fail", data=e)
    else:
        hc.send_ping()
    return governance_info


def collect_security_data(security, security_file, logger, log_dir):
    try:
        hc.send_ping("start")
        logger.info('Collecting security data.')
        security_logger = setup_logger('security_logger', log_dir+'/scorecard.log')
        ss = SecurityScore(security, security_logger)
        ss.run_scorecard()
        ss.df.to_csv(security_file, index=False)
        logger.info('Successfully gathered security data.')
    except Exception as e:
        logger.exception(f'An exception occurs: {e}.')
        hc.send_ping("fail", data=e)
    else:
        hc.send_ping()


def collect_license_data(licenses, license_file, logger):
    try:
        hc.send_ping("start")
        logger.info('Generating license scores.')
        license_table = pd.read_csv('data/db_tables/license_table.csv')
        license_dict = dict(zip(license_table['license'].tolist(), license_table['permissiveness_score']))
        licenses['permissiveness_score'] = licenses.license.apply(license_score, license_dict=license_dict)
        licenses.to_csv(license_file, index=False)
        logger.info('Wrote licenses to file.')
    except Exception as e:
        logger.exception(f'An exception occurs: {e}.')
        hc.send_ping("fail", data=e)
    else:
        hc.send_ping()


def collect_versioning_data(version_data, version_count_data, version_file,
                            version_count_file, logger, log_dir):
    try:
        hc.send_ping("start")
        logger.info('Collecting versioning data.')
        versioning_logger = setup_logger('versioning_logger', log_dir+'/versioning.log')
        version_data.to_csv(version_file)
        logger.info('Successfully gathered versioning data.')
    except Exception as e:
        logger.exception(f'An exception occurred: {e}.')
        hc.send_ping("fail", data=e)
    else:
        hc.send_ping()

def create_files(run_folder, data_dir='data'):
    """
    Create folders and files to store each component's data.
    """
    data_dir = f'{data_dir}/{run_folder}'
    create_dir(data_dir)
    files = {}
    files['popularity'] = os.path.join(data_dir, f'popularity.csv')
    files['gh_metrics'] = os.path.join(data_dir, f'com_ghmetrics.csv')
    files['gh_scrape'] = os.path.join(data_dir, f'com_ghscrape.csv')
    files['twitter'] = os.path.join(data_dir, f'com_twitter.csv')
    files['stack'] = os.path.join(data_dir, f'com_stack.csv')
    files['governance'] = os.path.join(data_dir, f'com_gov.csv')
    files['community'] = os.path.join(data_dir, f'community.csv')
    files['security'] = os.path.join(data_dir, f'security.csv')
    files['license'] = os.path.join(data_dir, f'license.csv')
    files['version'] = os.path.join(data_dir, f'version_data.csv')
    files['version_count'] = os.path.join(data_dir, f'version_count.csv')
    files['projects'] = os.path.join(data_dir, f'projects.csv')
    keys = list(files.keys())
    for f in keys: files[f+'_tmp'] = files[f][:-4]+'_tmp.csv'
    return files

def setup_logs(root_dir):
    script_logs = f'{root_dir}/logs'
    create_dir(script_logs)
    log_dirs = {}
     # logging paths
    log_dirs['popularity'] = script_logs+'/popularity_logs'
    log_dirs['community'] = script_logs+'/community_logs'
    log_dirs['governance'] = script_logs+'/governance_logs'
    log_dirs['security'] = script_logs+'/security_logs'
    log_dirs['version'] = script_logs+'/versioning_logs'
    for dir in log_dirs.values(): create_dir(dir)


    # set up logger
    main_logfile = os.path.join(script_logs, 'data_collection.log')
    logger = setup_logger('1000_sample_test', main_logfile)
    return log_dirs, logger

def query_libio_db(db_env, project_list, files, logger):
    popularity, community, license, security, version, version_count = \
    load_tables_from_db(project_list, db_env, logger)
    projects = community[['repository_id', 'full_name']]
    projects.rename(columns={"full_name": "project_name"}, inplace=True)

    projects.to_csv(files['projects_tmp'], index=False)
    community.to_csv(files['community_tmp'], index=False)
    popularity.to_csv(files['popularity_tmp'], index=False)
    license.to_csv(files['license_tmp'], index=False)
    security.to_csv(files['security_tmp'], index=False)
    version.to_csv(files['version_tmp'], index=False)
    version_count.to_csv(files['version_count_tmp'], index=False)
    return community, popularity, license, security, version, version_count

def process_one_batch(batch_df, files, envs, logfiles, logger):
    """Process one batch and stores results into temp files"""

    community, popularity, license, security, version, version_count = \
    query_libio_db(envs['db'], batch_df.full_name.tolist(), files, logger)
    collected_data = [
        collect_gh_data.remote(community, files['gh_scrape_tmp'], logger, logfiles['community']),
        collect_twitter_data.remote(community, files['twitter_tmp'], envs['twitter'], logger, logfiles['community']),
        collect_metrics_data.remote(community,files['gh_metrics_tmp'], envs['gh_metrics'], logger, logfiles['community']),
        collect_stack_data.remote(community,files['stack_tmp'], envs['stack'], logger, logfiles['community']),
        collect_governance_data.remote(community.full_name, files['governance_tmp'], envs['gov'], logger),
        collect_popularity_data.remote(popularity, files['popularity_tmp'], envs['popularity'], logger, logfiles['popularity']),
        collect_license_data.remote(license, files['license_tmp'], logger),
        collect_security_data.remote(security, files['security_tmp'], logger, logfiles['security']),
        collect_versioning_data.remote(version, version_count, files['version_tmp'], files['version_count_tmp'],
                                logger, logfiles['version']),
        ]
    ray.get(collected_data)


def merge_collected_data(files):
    """
    Append component temp files
    to component files.
    """
    fnames= ['popularity', 'gh_metrics', 'gh_scrape', 'twitter', 'stack', 'governance',
             'community', 'license', 'version', 'version_count', 'projects',
             'security',
             ]
    if sum([os.path.exists(files[name]) for name in fnames]) == 0:
        for name in fnames: os.rename(files[name+'_tmp'], files[name])
    else:
        for s in fnames:
            pd.concat([pd.read_csv(files[s]), pd.read_csv(files[s+'_tmp'])]).to_csv(files[s], index=False)



def set_envs(type='integ'):
    """
    Sets all env files in a dictionary.
    """
    envs = {}
    dir = 'env_variables'
    envs['twitter'] = f'{dir}/.twitter-env'
    envs['gh_metrics'] = f'{dir}/.metrics-env'
    envs['stack'] = f'{dir}/.stack-env'
    envs['gov'] = f'{dir}/.gov-env'
    envs['popularity'] = f'{dir}/.popularity-env'
    if type == 'integ': envs['db'] = f'{dir}/.rds-connect-env'
    elif type == 'prod': envs['db'] = f'{dir}/.rds-connect-env-prod'
    return envs

def initial_setups(data_dir, resume=False, subfolder=None):
    """Create CSV files to store collected data,
    log folders and logger object.

    Args:
        data_dir: str
            Relative path from the current folder to the folder
            where logs and data collected (csv files) will be saved.
        resume: bool
            True for resuming data collection from a stopped run
            False otherwise (default value)
        subfolder: str or None(default)
            Name of the subfolder of the stopped run. When `resume = True`,
            it should be `str` and not `None`

    Returns:
        files: A dictionary of relative paths to CSV files of collected data.
        logfiles: same as previous one for log file paths
        logger: Logger object for writing into logfiles
    """
    if resume:
        if subfolder is None:
            raise KeyError(f'Subfolder should not be None when resume is True')
        run_folder = subfolder
    else:
        run_folder = '_'.join(str(datetime.now()).split('.')[0].split())
        run_folder = run_folder.replace(':', '_')

    files = create_files(run_folder, data_dir=data_dir)
    logfiles, logger = setup_logs(f'{data_dir}/{run_folder}')
    return files, logfiles, logger

def process_batches(df_projects, envs, data_dir='data', resume=False, subfolder=None, bs=100):
    """Process all the projects in batch chunks"""
    files, logfiles, logger = initial_setups(data_dir, resume, subfolder)
    if resume:
        logger.info("*********************** Resume data collection ***********************")
    else:
        logger.info("*********************** Start data collection ***********************")

    for k in range(0, len(df_projects), bs):
        logger.info(f"--- Batch {k//bs+1}: projects {k} to {k+bs}  ---")
        batch_df = df_projects.iloc[k:k+bs]
        process_one_batch(batch_df, files, envs, logfiles, logger)
        combine_community_files(files)
        merge_collected_data(files)

    logger.info("*********************** End of data collection ***********************")
    return files

def calculate_scores_and_save_to_db(files, db_env, overwrite_db=False):
    """
    Calculates the aggregated scores and saves them to the database.
    """
    ots = OpenTeamsScore(files['security'], files['license'], files['popularity'],
            files['community'], files['version'], files['version_count'])
    projects = ots.overall_score()
    projects.to_csv(files['projects'], index=False)
    ots.save_agg_score_to_db(db_env, forward=True, table='openteams_score',
                             overwrite=overwrite_db)
    print('Successfully saved data into database')



