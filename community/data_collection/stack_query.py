import requests
import os
import json
import time
import logging
from datetime import datetime
from itertools import cycle
import pandas as pd
import numpy as np
from dotenv import dotenv_values


def date2timestamp(date_str, format='%Y-%m-%d'):
    """
    Convert a date in string format into seconds
    """
    date = datetime.strptime(date_str,format).timetuple()
    return  int(time.mktime(date))


class RequestsExhaustedException(Exception):
    
    def __init__(self, status_code, message):
        self.status_code = status_code
        self.message = message
        super().__init__(self.status_code,
                         self.message )


class DailyJobFinishedException(Exception):
    
    def __init__(self, message):
        self.message = message
        super().__init__(message)
        

class StackQuery():

    """
    This class aims to build search queries to the
    StackExchange API based on project
    """
    
    s_url = "https://api.stackexchange.com/2.3/search/advanced"

    def __init__(self, logger, fromdate=None, todate=None, key=None, 
                 token=None, pagesize=100, site='stackoverflow'):
        
        self._set_dates(fromdate, todate)
        self.key = key
        self.token = token
        self.logger = logger
        self.site = site
        self.pagesize = 100
        self.api_calls = 0
        self.backoff_times = 0
        self.sleep_time = 0
        self.slept_times = 0

    def _set_dates(self, fromdate, todate):
        """
        Properly populate dates covering the period
        of the search.
        By default a period of the last 90 days is set.
        """
        if todate is None:
            now = str(datetime.now())[:10]
            self.todate = date2timestamp(now)
        else:
            self.todate = date2timestamp(todate)

        if fromdate is None:
            self.fromdate = self.todate - 90*(24*3600)
        else:
            self.fromdate = date2timestamp(fromdate)

    def search(self, project=''):
        """
        Make a search query t the StackExchange API 
        based on the provided project.
        Returns the response object
        """
        if project == '':
            raise KeyError(f'project must not be empty')
        self._set_query_params(project)
        response = requests.get(StackQuery.s_url, params=self.params)
        self._act_upon_response(response, project)
        return response
    
    
    def _act_upon_response(self, resp, project):
        """
        This method execute the proper action
        based on query response whether to sleep
        a few minutes or raise an exception about
        daily quota limit reached
        """
        resp_dict = resp.json()
        if resp.status_code == 200:
            if 'backoff' in resp_dict:
                backoff = int(resp_dict["backoff"])+5
                self._sleep(backoff, 'backoff')
            self.quota = resp_dict['quota_remaining']
            self.api_calls += 1
            self.slept_times = 0
        else:
            if self.quota > 0:
                self._deal_with_error_code(resp)
                self.search(self, project)
            else:
                self.logger.warning('Daily quotas exhausted')
                raise RequestsExhaustedException(resp.status_code, resp.text)
                
    def _deal_with_error_code(self, resp):
        """
        Handle query error
        """
        self.logger.info(f'{resp.status_code}: {resp.text}')
        self.slept_times += 1
        self._sleep(300*self.slept_times)
       
    
    def _sleep(self, duration, reason=''):
        """
        Sleep for a few minutes to
        resume queries
        """
        self.logger.info(f'sleeping for {reason}: {round(duration/60.0, 3)} minutes...')
        time.sleep(duration)
        if reason == 'backoff': self.backoff_times += 1
        else: self.sleep_time += duration
        
        

    def _set_query_params(self, project):
        """
        Helper to set the query parameters
        """
        self.params ={}
        self.params['fromdate'] = self.fromdate
        self.params['todate'] = self.todate
        self.params['sort'] = 'activity'
        self.params['title'] = project
        self.params['site'] = self.site
        self.params['pagesize'] = self.pagesize
        
        if self.key is not None:
            self.params['key'] = self.key
        if self.token is not None:
            self.params['access_token'] = self.token


    def get_stats(self, response):
        """
        Make a search and compute some
        statistics on the response
        """
        stats = {'question_count': 0, 'view_count':0, 'answer_count':0, 
                 'score':0, 'is_answered':0}
        for item in response.json()['items']:
            stats['question_count'] += 1
            for k in list(stats)[1:]:
                stats[k] += int(item[k])
        return stats, response


class StackQueryManager():
    """
    This class takes care of managing
    mmultiple key/token pairs to maximize
    daily queries
    """
    def __init__(self, env, logger, todate=None):
        
        self.logger = logger
        self.todate = str(datetime.now())[:10] if todate==None else todate
        self._instantiate_query_objects(env)
        self.idxs = np.arange(len(self.query_objs))
        self.idx = np.random.choice(self.idxs)
        self.exhausted = 0

    def _instantiate_query_objects(self, env):

        creds = dotenv_values(env)
        n = (len(creds.keys()) // 2) + 1
        self.keys = [creds[f'KEY{i}'] for i in range(1,n)]
        self.tokens = [creds[f'TOKEN{i}'] for i in range(1,n)]
        self.query_objs = [StackQuery(self.logger, key=k, token=t,
                           todate=self.todate) for k,t in zip(self.keys,self.tokens)]


    def make_query(self, project_name):
        """
        make a single query about `project_name`
        """
        try:
            self.logger.info(f'querying project: {project_name}...')
            resp = self.query_objs[self.idx].search(project_name)

        except KeyError as k:
            self.logger.exception(f'Error: project name is empty')
            return {}
        except RequestsExhaustedException as e:
            self.logger.exception(f"Requests Exhausted for one credential (key/token): {e}")
            self.exhausted += 1
            if self.exhausted < len(self.keys):
                self.idx = (self.idx + 1) % len(self.idxs)
            else:
                raise DailyJobFinishedException('Daily Query Capacity reached')
            resp = self.query_objs[self.idx].search(project_name)
        finally:
            
            res, _ = self.query_objs[self.idx].get_stats(resp)
            self.logger.info(f'Successfully returned query results.')
            return res
            
    def make_many_queries(self, project_full_names):
        """
        Makes as many queries as `project_names`
        provided
        """
        data_fields = ['question_count', 'view_count', 'answer_count', 
                               'score', 'is_answered']
        res = {k: [] for k in ['full_name']+data_fields}
        for name in project_full_names:
            _, p = name.split('/')
            try :
                stats = self.make_query(p)
                res['full_name'] += [name]
                for k in data_fields: res[k]+= [stats[k]]
            except DailyJobFinishedException as e:
                self.logger.exception("Daily Job is over")
                stack_data = pd.DataFrame.from_dict(res)
                return stack_data

        stack_data = pd.DataFrame.from_dict(res)
        return stack_data
