import bs4
import requests
from lxml import etree
import logging

def log_error(project, e, logger):
    logger.exception(f'Current run for the project {project} failed with error: {e}')

def log_downloads(func):
    def wrapper(project, logger):
        logger.info(f'Scraping data for {project}')
        try:
            out = func(project, logger)
            logger.info(f'Successfully scraped data for {project}.')
            return out
        except Exception as e:
            log_error(project, e, logger)
            return None, None
    return wrapper

def download_all(df, logger):
    names = df['full_name'].tolist()

    open_issues = []
    closed_issues = []
    open_prs = []
    closed_prs = []

    for name in names:
        o_p, c_p = get_pulls(name, logger)
        o_i, c_i = get_issues(name, logger)

        open_prs.append(o_p)
        closed_prs.append(c_p)
        open_issues.append(o_i)
        closed_issues.append(c_i)

    df['open_issues_count'] = open_issues
    df['closed_issues_count'] = closed_issues
    df['open_pr_count'] = open_prs
    df['closed_pr_count'] = closed_prs
    return df


def clean_text(texts):
    """
    A utility function for pulling out numbers from a list of text.

    texts: list[str]
        a list of strings pulled from xpath texts

    Example;
    >>> a = ['\n', '\n      8,227 Closed\n    ', '\n', '\n      8,227 Closed\n    ']
    >>> clean_text(a)
    '8,227'
    """
    for t in texts:
        if t != '\n':
            subs = t.split(' ')
            for s in subs:
                if ',' in s:
                    return int(s.replace(',',''))
                elif s.isnumeric():
                    return s

@log_downloads
def get_pulls(project, logger):
    """
    A function for scraping the number of open and closed PRs for a Github project.

    project: str
        The gh project (e.g. 'numpy/numpy')

    Returns:

    opened_prs: str
        the number of open PRs
    closed_prs: str
        the number of closed PRs
    """
    out = requests.get(f'https://github.com/{project}/pulls')
    soup = bs4.BeautifulSoup(out.text, 'html.parser')
    html = etree.HTML(str(soup))

    closed = html.xpath("//a[@class='btn-link'][@data-ga-click='Pull Requests, Table state, Closed']/text()")
    closed_prs = clean_text(closed)

    opened = html.xpath("//a[@class='btn-link selected'][@data-ga-click='Pull Requests, Table state, Open']/text()")
    opened_prs = clean_text(opened)

    return opened_prs, closed_prs

@log_downloads
def get_issues(project, logger):
    """
    A function for scraping the number of open and closed PRs for a Github project.

    project: str
        The gh project (e.g. 'numpy/numpy')

    Returns:

    opened_issues: str
        the number of open issues
    closed_issues: str
        the number of closed issues
    """
    out = requests.get(f'https://github.com/{project}/issues')
    soup = bs4.BeautifulSoup(out.text, 'html.parser')
    html = etree.HTML(str(soup))

    closed = html.xpath("//a[@class='btn-link'][@data-ga-click='Issues, Table state, Closed']/text()")
    closed_issues = clean_text(closed)

    opened = html.xpath("//a[@class='btn-link selected'][@data-ga-click='Issues, Table state, Open']/text()")
    opened_issues = clean_text(opened)

    return opened_issues, closed_issues

def get_docs(project):
    """
    A function for getting the documentation link from github. This method is not perfectly reliable.
    If a gh project does not link their documentation in the place for it, we won't be able to find
    their docs. It's possible that a project could post something else in this link that isn't their
    docs.

    project: str
        The gh project (e.g. 'numpy/numpy')
    """
    out = requests.get(f'https://github.com/{project}')
    soup = bs4.BeautifulSoup(out.text, 'html.parser')
    html = etree.HTML(str(soup))

    docs = html.xpath("//a[@class='text-bold'][@role='link'][@target='_blank']")[0]
    for d in docs.items():
        if 'href' in d[0]:
            return d[1]
