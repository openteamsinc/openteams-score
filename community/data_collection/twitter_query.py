import requests
import os, logging
import json
import time
from dotenv import dotenv_values
import pandas as pd

s_url = "https://api.twitter.com/2/tweets/search/recent"
agent = "v2RecentSearchPython"
q_params = {'tweet.fields': 'public_metrics'}

class MonthlyQuotasReachException(Exception):
    def __init__(self, response):
        self.response = response
        super().__init__(self.response.status_code,
                         self.response.text )


def validate_keyword(name):
    org_name, word = name.split('/')
    spe_chars =".-+*$:;!?," 
    if word[0] in spe_chars :
        word = word[1:]
    if word[-1] in spe_chars:
        word = word[:-1]
    if word == 'http':
        word = f'{org_name}/{word}'
    return word

class TwitterQuery():
    """
    This class aims tp build search queries to the
    Twitter API based on project
    """

    def __init__(self, env, logger, user_agent=agent, url=s_url, params=q_params,
                 max_results=None, sleep_on_limit=True):
        creds = dotenv_values(env)
        self.bearer_token = creds['BEARER_TOKEN1']
        self.logger = logger
        self.user_agent = user_agent
        self.url = url
        self.params = params
        self.params['max_results'] = 100 if max_results is None else max_results
        self.sleep_on_limit = sleep_on_limit
        
        
    def bearer_oauth(self, r):
        """
        Method required by bearer token authentication.
        """
        r.headers["Authorization"] = f"Bearer {self.bearer_token}"
        r.headers["User-Agent"] = self.user_agent
        return r

    def search(self, project=''):
        """
        Make a search query based on
        the provided project
        """
        self.logger.info(f'Querying project: {project}...')
        if project == '':
            raise KeyError(f'project must not be empty')
        self.params['query'] = project
        response = requests.get(self.url, auth=self.bearer_oauth,
                                params=self.params)
        response = self._act_upon_response(response, project)
        return response

    def _act_upon_response(self, resp, project):
        """
        This method execute the proper action
        based on query response whether to sleep
        a few minutes or raise an exception about
        daily quota limit reached
        """
        resp_dict = resp.json()
        if  resp.status_code != 200:
            self.logger.warning(f'Query error, {resp.status_code}: {resp.text}')
            if 'Usage cap exceeded' in resp.text:
                raise MonthlyQuotasReachException(resp)
            if resp.status_code > 422:
                if self.sleep_on_limit:
                    self._sleep(14)
                return self.search(project)
        return resp
        
    def _sleep(self, minutes=3):
        self.logger.info(f'sleeping for {minutes} minutes ...')
        time.sleep(minutes*60)

    def get_stats(self, response_json):
        """
        Compute some statistics about a response
        from a Twitter API query search 
        """
        stats = {'retweet_count': 0, 'reply_count': 0,
                 'like_count': 0, 'quote_count': 0}
        stats['tweet_count'] = response_json['meta']['result_count']
        if stats['tweet_count'] > 0:
            for d in response_json['data']:
                for k in d['public_metrics']:
                    stats[k] += d['public_metrics'][k]

        return stats

    def response_json(self, resp):
        if resp.status_code == 200:
            return resp.json()
        else:
            return {'meta': {'result_count': 0}}

    def get_project_stats(self, name):
        """
        Gets Twitter stats from a given
        project
        """
        self.logger.info(f'Collecting twitter data of project: {name} ...')
        project_name = validate_keyword(name)
        
        try:
            response = self.search(project_name)
        except MonthlyQuotasReachException as e:
            self.logger.exception('Monthly Quota reached')
            return None
        finally:
            self.logger.info(f'Collection twitter data of project: {name} has been completed successfully.')
            return self.get_stats(self.response_json(response))

    def get_multiple_stats(self, project_full_names=['']):
        """
        Compute statistics for multiple search queries
        """
        data_fields = ['tweet_count', 'retweet_count','reply_count', 'like_count', 'quote_count']
        res = { k: [] for k in ['full_name'] + data_fields}
        for full_name in project_full_names:
            stats = self.get_project_stats(full_name)
            if stats == None:
                self.logger.error(f'Stopping data collection at project "{full_name}" as Monthly Quotas have been reached.')
                break
            res['full_name'] += [full_name]
            for k in data_fields: res[k] += [stats[k]]
        twitter_data = pd.DataFrame.from_dict(res)
        return twitter_data

    