
"""
Functions for collecting downloads data from different package managers.
Some of the package managers offer total downloads while some show the
recent downloads.
"""

import datetime
import requests
import bs4
import pandas as pd
import numpy as np
import dask.dataframe as dd
import logging
import warnings
import edn_format
import toml
import glob
import os
import random

from condastats.cli import overall
from collections.abc import Iterable
from dateutil.relativedelta import relativedelta
from lxml import etree
from gzip import decompress
from io import StringIO
from google.cloud import bigquery


PM_FUNCTIONS = {'pypi_downloads': 'Pypi',
    'cran_downloads': 'Cran',
    'npm_downloads' : 'NPM',
    'rubygems_downloads' : 'Rubygems',
    'nuget_downloads' : 'NuGet',
    'haxelib_downloads' : 'Haxelib',
    'hackage_downloads' : 'Hackage',
    'query_emacs': 'Emacs',
    'cocoapods_downloads': 'CocoaPods',
    'packagist_downloads': 'Packagist',
    'sublime_downloads': 'Sublime',
    'query_clojars': 'Clojars',
    'query_julia': 'Julia',
    'query_conda': 'Conda'}



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


def browser_prep(headless=True):
    """
    Prepares some browser settings for selenium.
    """
    useOptions = Options()
    useOptions.add_experimental_option('excludeSwitches', ['enable-logging'])
    useOptions.add_argument('window-size=1920x1080')
    useOptions.add_argument("--no-sandbox")
    useOptions.add_argument("--disable-dev-shm-usage")
    useOptions.add_argument("start-maximised")
    if headless:
        useOptions.add_argument("--headless")
    browser = selenium.webdriver.Chrome(ChromeDriverManager().install(), options=useOptions)
    browser.maximize_window()
    return browser

def set_credentials(credentials):
    account = random.choice(credentials) # pick a gmail account at random
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = account['json']
    project = account['name']
    client = bigquery.Client(project=project)
    return client



class DownloadStats:

    """
    A class for organizing downloads data from different package managers.

    data: pd.DataFrame
        A df that must include `platform` and `project_name` columns.

    path_to_julia_registry: str
        a path to a cloned julia_registry repo

    bigquery_credentials: list[dict]
        A list of dictionaries that hold the credentials for different gmail accounts.
        Each dict should have a `name` and `json` entry.
    """

    def __init__(self, data: pd.DataFrame, path_to_julia_registry, bigquery_credentials,
                 log_dir, days_delta=90, **kwargs):
        self.data = data
        pm_dispatch, log_files = log_setups(log_dir)
        self.pm_dispatch = pm_dispatch
        self.log_files = log_files
        self.days_delta = days_delta
        self.downloads = None

        self.bigquery_credentials = bigquery_credentials
        self.julia_data = julia_downloads(path_to_julia_registry, days_delta, self.log_files)

        if 'emacs_data' in kwargs:
            self.emacs_data = kwargs['emacs_data']
        else: self.emacs_data = emacs_downloads(self.log_files)

        if 'clojars_data' in kwargs:
            self.clojars_data = kwargs['clojars_data']
        else: self.clojars_data = clojars_downloads(self.log_files)

    def get_downloads(self):
        """
        Collects the downloads for each row in a dataframe.

        The following package managers return total download stats:
            pypi
            cran
            rubygems
            nuget
            haxelib
            hackage
            emacs
            cocoapods
            packagist
            sublime

        The following package managers return recent download stats:
            cran
            conda
            npm
            pypi (possibly in the future)
        """
        self.downloads = self.data.apply(download_all, args=(self.days_delta, self.bigquery_credentials,
            self.emacs_data, self.clojars_data, self.julia_data, self.pm_dispatch, self.log_files), axis=1)
        return self.downloads

    def write(self):
        if self.downloads is not None:
            total = []
            recent = []
            for d in self.downloads:
                total.append(d[0])
                recent.append(d[1])
            self.data['total_downloads'] = total
            self.data['recent_downloads'] = recent


def unpack_series(downloads):
    total = []
    recent = []
    for d in downloads:
        total.append(d[0])
        recent.append(d[1])
    return total, recent


def get_returns(out):
    if 'total_downloads' in out:
        td = out['total_downloads']
    else:
        td = np.nan
    if 'recent_downloads' in out:
        rd = out['recent_downloads']
    else:
        rd = np.nan
    return (td, rd)

def download_all(row: pd.Series, days_delta, credentials, emacs_data, clojars_data, julia_data,
                 pm_dispatch, log_files):
    projects = pd.DataFrame()
    projects['project_name'] = row.project_name
    projects['platform'] = row.platform
    projects['full_name'] = [row.full_name] * len(row.project_name)
    downloads = projects.apply(download_projects, args=(days_delta, credentials, emacs_data, clojars_data, julia_data, pm_dispatch, log_files), axis=1)
    total, recent = unpack_series(downloads)

    if all(np.isnan(n) for n in total):
        total = np.nan
    else: total = np.nansum(total)

    if all(np.isnan(n) for n in recent):
        recent = np.nan
    else: recent = np.nansum(recent)

    return total, recent


def download_projects(row: pd.Series, days_delta, credentials, emacs_data,
                      clojars_data, julia_data, pm_dispatch, log_files):

    platform = row['platform']
    if platform == 'Packagist' or platform == 'Julia':
        project_name = row['full_name']
    else: project_name = row['project_name']

    if platform in log_files:
        logger = log_files[platform]
    else: logger = log_files['missing_pms']
    kwargs = {'project': project_name,
        'days_delta': days_delta,
        'credentials': credentials,
        'logger': logger,
        'emacs_data': emacs_data,
        'clojars_data': clojars_data,
        'julia_data': julia_data}

    if platform in pm_dispatch:
        pm_func = pm_dispatch[platform]
        return pm_func(**kwargs)
    logger.info(f'No data available for project {project_name} for PM {platform}.')
    return get_returns({})

def log_downloads(func):
    def wrapper(**kwargs):
        project = kwargs['project']
        logger = kwargs['logger']
        pm = PM_FUNCTIONS[func.__name__]
        logger.info(f'Gathering data for {project} from {pm}')
        try:
            out = func(**kwargs)
            logger.info(f'Successfully gathered data for {project} from {pm}')
            return get_returns(out)
        except Exception as e:
            log_error(project, pm, e, logger)
            return get_returns({})
    return wrapper

def log_error(project, pm, error, logger):
    logger.warning(f'Current run for the {pm} package manager failed with error: {error.__class__.__name__}')
    logger.exception(f'Current run for the projects {project} for the package manager {pm} \
        failed with error: {error.__class__.__name__}')

@log_downloads
def pypi_downloads(**kwargs):
    project = kwargs['project']
    credentials = kwargs['credentials']
    days_delta = kwargs['days_delta']
    client = set_credentials(credentials)
    query = f"""
SELECT COUNT(*) AS num_downloads
FROM `bigquery-public-data.pypi.file_downloads`
WHERE file.project = '{project}'
  -- Only query the last x days of history
  AND DATE(timestamp)
    BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL {days_delta} DAY)
    AND CURRENT_DATE()"""

    job = client.query(query)
    results = job.result().to_dataframe()['num_downloads'][0]
    return {'recent_downloads': results}

@log_downloads
def query_conda(**kwargs):
    project = kwargs['project']
    out = overall([project])
    return {'total_downloads': out[project]}

@log_downloads
def cran_downloads(**kwargs):

    project = kwargs['project']
    days_delta = kwargs['days_delta']
    out = {}
    start = datetime.date(2012,10,1)
    now = datetime.date.today()

    # total downloads
    res = requests.get(f'https://cranlogs.r-pkg.org/downloads/total/{start}:{now}/{project}')
    out['total_downloads'] = res.json()[0]['package']

    # recent downloads
    start = now - relativedelta(days=days_delta)
    res = requests.get(f'https://cranlogs.r-pkg.org/downloads/total/{start}:{now}/{project}')
    out['recent_downloads'] = res.json()[0]['package']


    return out

@log_downloads
def npm_downloads(**kwargs):

    project = kwargs['project']
    days_delta = kwargs['days_delta']
    out = {}
    now = datetime.date.today()
    start = now - relativedelta(days=days_delta)

    res = requests.get(f'https://api.npmjs.org/downloads/point/{start}:{now}/{project}')
    out['recent_downloads'] = res.json()['downloads']
    return out

@log_downloads
def rubygems_downloads(**kwargs):
    project = kwargs['project']
    version = requests.get(f'https://rubygems.org/api/v1/versions/{project}.json').json()[0]['number']
    res = requests.get(f'https://rubygems.org/api/v1/downloads/{project}-{version}.json')
    return {'total_downloads': res.json()['total_downloads']}

@log_downloads
def nuget_downloads(**kwargs):
    project = kwargs['project']
    res = requests.get(f'https://www.nuget.org/packages/{project}/')
    soup = bs4.BeautifulSoup(res.text)
    html = etree.HTML(str(soup))
    total = html.xpath("//span[@class='download-info-content']")[0].text

    if 'K' in total:
        total = int(float(total.split('K')[0]) * 1000)
    elif 'M' in total:
        total = int(float(total.split('M')[0]) * 1000000)
    elif 'B' in total:
        total = int(float(total.split('B')[0]) * 1000000000)
    else: total = int(total)
    return {'total_downloads': total}

@log_downloads
def haxelib_downloads(**kwargs):
    project = kwargs['project']
    res = requests.get(f'https://lib.haxe.org/p/{project}/versions/')
    soup = bs4.BeautifulSoup(res.text)
    html = etree.HTML(str(soup))
    totals = html.xpath("//tr/td")
    totals = [t for t in totals if t.text is not None]
    totals = [int(t.text) for t in totals if t.text.isnumeric()]
    return {'total_downloads': sum(totals)}

@log_downloads
def hackage_downloads(**kwargs):
    project = kwargs['project']
    res = requests.get(f'https://hackage.haskell.org/package/{project}')
    soup = bs4.BeautifulSoup(res.text)
    html = etree.HTML(str(soup))
    elements = html.xpath('//tr//following-sibling::td')

    for e in elements:
        if e.text is not None:
            if 'total' in e.text:
                out = e
    return {'total_downloads': int(out.text.split('total')[0])}

@log_downloads
def query_emacs(**kwargs):
    project = kwargs['project']
    emacs_data = kwargs['emacs_data']
    return {'total_downloads': emacs_data[project]}

def emacs_downloads(log_files):
    try:
        logger = log_files['Emacs']
        logger.info('attempting to access emacs data.')
        res = requests.get('https://melpa.org/download_counts.json')
        if res.status_code != 200:
            logger.error('unable to get Emacs data. Wrong status code.')
        else:
            logger.info('Successfully downloaded Emacs data.')
        return res.json()
    except Exception as e:
        logger.error(f'unable to get Emacs data. Failed with error {e}.')

@log_downloads
def cocoapods_downloads(**kwargs):
    # old of the cocoapods projects I've seen are old. It's unclear whether or not this data gets updated
    project = kwargs['project']
    res = requests.get(f'http://metrics.cocoapods.org/api/v1/pods/{project}.json')
    return {'total_downloads': res.json()['stats']['download_total']}

@log_downloads
def packagist_downloads(**kwargs):
    # requires repo name: organization/repo_name
    project = kwargs['project']
    res = requests.get(f'https://packagist.org/packages/{project}.json')
    return {'total_downloads': res.json()['package']['downloads']['total']}

@log_downloads
def sublime_downloads(**kwargs):
    project = kwargs['project']
    res = requests.get(f'https://packagecontrol.io/packages/{project}')
    soup = bs4.BeautifulSoup(res.text)
    tree = etree.HTML(str(soup))
    total_tag = tree.xpath('//span[@class="installs"]')
    return {'total_downloads': int(total_tag[0].items()[1][1].replace(',', ''))}

def clojars_downloads(log_files):
    try:
        logger = log_files['Clojars']
        logger.info('attempting to access clojars data.')
        res = requests.get('https://repo.clojars.org/stats/all.edn')
        data = edn_format.loads(res.text)
        out = {}
        for k,v in data.dict.items():
            key = '/'.join(k)
            val = sum(v.values())
            out[key] = val
        logger.info('Successfully downloaded Clojars data.')
        return out
    except Exception as e:
        logger.error(f'unable to get Clojars data. Failed with error {e}.')

@log_downloads
def query_clojars(**kwargs):
    project = kwargs['project']
    clojars_data = kwargs['clojars_data']
    return {'total_downloads': clojars_data[project]}


def julia_uuids(path_to_registry, log_files):

    logger = log_files['Julia']
    out = {}
    logger.info('attempting to get Julia uuids.')
    try:
        # git pull to get updates
        files = glob.glob(os.path.join(path_to_registry, '*', '*', 'Package.toml'))
        for f in files:
            toml_dict = toml.load(f)
            repo = toml_dict['repo'].split('.com/')[-1].split('.git')[0]
            uuid = toml_dict['uuid']
            out[uuid] = repo
        logger.info('succeeded in getting Julia uuids.')
        return out
    except Exception as e:
        logger.error(f'unable to get Julia uuids. Failed with error {e}.')

def julia_downloads(path_to_registry, days_delta, log_files):

    def convert_uuid(uuid):
        try:
            return uuids[uuid]
        except:
            return np.nan
    # get uuids
    uuids = julia_uuids(path_to_registry, log_files)
    logger = log_files['Julia']

    try:
        res = requests.get('https://julialang-logs.s3.amazonaws.com/public_outputs/current/package_requests.csv.gz')
        df = pd.read_csv(StringIO(decompress(res.content).decode("utf-8")))
        df['date_min_dt'] = df.date_min.apply(datetime.datetime.strptime, args=('%Y-%m-%d',))
        df['date_max_dt'] = df.date_max.apply(datetime.datetime.strptime, args=('%Y-%m-%d',))
        df['package_name'] = df.package_uuid.apply(convert_uuid)
        start = df.date_max_dt.max() - relativedelta(days=90)
        sample = df[(df.date_min_dt >= start)].groupby('package_name').sum()
        logger.info('succeeded in geting Julia data.')
        return dict(zip(sample.index, sample.request_count))

    except Exception as e:
        logger.error(f'unable to get Julia data. Failed with error {e}.')

@log_downloads
def query_julia(log_files, **kwargs):
    logger = log_files['Julia']
    project = kwargs['project']
    julia_data = kwargs['julia_data']
    if project in julia_data:
        return {'recent_downloads': julia_data[project]}
    else:
        logger.info(f'cannot find data for Julia project {project}.')
        return {}

def log_setups(path):
    pm_dispatch = {
        'Pypi': pypi_downloads,
        'Conda': query_conda,
        'NPM': npm_downloads,
        'Cran': cran_downloads,
        'Rubygems': rubygems_downloads,
        'NuGet': nuget_downloads,
        'Haxelib': haxelib_downloads,
        'Hackage': hackage_downloads,
        'Emacs': query_emacs,
        'CocoaPods': cocoapods_downloads,
        'Packagist': packagist_downloads,
        'Sublime': sublime_downloads,
        'Clojars': query_clojars,
        'Julia': query_julia,
    }
    log_files = {pm: setup_logger(pm, f"{path}/{pm}.log") for pm in pm_dispatch.keys()}
    log_files['missing_pms'] = setup_logger('missing_pms',  f'{path}/missing_pms.log')
    return pm_dispatch, log_files
