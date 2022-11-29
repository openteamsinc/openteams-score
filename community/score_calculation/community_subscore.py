import numpy as np
import pandas as pd
import sys, os
from scipy.stats import zscore
from utils import s_shape, clean, ratio_of_2cols, binarize_field



class Community():
    """
    This class is responsible for
    calculating the community subscore based
    on data from Github, Twitter, Stackoverflow
    """
    
    def __init__(self, table):
        self.data_file = table
        self.data = pd.read_csv(self.data_file)
        

    def score(self):
        """
        Compute the community subscore based on the following components
        along with their weights:
            - Fundamental score (documentation, contribution guidelines
                and readme): 20 points;
                
            - Activity score (Github): 40
            
            - Interaction score (Twitter): 20
            
            - Learning score (Stackoverflow): 20
             
        """
        project = self.data.copy()
        f_score = self.fundamental_score()
        activity_score = self.github_score()
        
        interaction_score = self.twitter_score()
        learning_score = self.stack_score()
        self.data['twitter_activity'] = interaction_score
        self.data['stackoverflow_activity'] = learning_score
        score = round(f_score + activity_score + interaction_score + learning_score , 2)
        self.data['community_openteams_score'] = score
        return self.data['community_openteams_score']


    def fundamental_score(self):
        """
        Compute the partial community score (counting for 20 points out of 100)
        based on boolean fields with the following coefficients:
            - documentation          : 0.4
            - conribution guidelines : 0.3
            - readme                 : 0.2
            - governance             : 0.1
        """
        doc = self.data['homepage'].apply(lambda x: binarize_field(x))
        contrib = self.data['has_contributing'].apply(lambda x: binarize_field(x))
        readme = self.data['has_readme'].apply(lambda x: binarize_field(x))
        governance = self.data['governance'].apply(lambda x: binarize_field(x))
        score = 20*(0.4*doc + 0.3*contrib + 0.2*readme + 0.1*governance)
        return np.round(score, 2)


    def github_score(self):
        """
        Compute a weighted score based on github data
        (open/closed issues, PRs and commits)
        """
        project = self.data.copy()
        cols = ['open_issues_count', 'closed_issues_count', 'open_pr_count',
                'closed_pr_count', 'weekly_commits', 'contrib_stats']
        for c in cols:
            project[c] = project[c].apply(lambda x: clean(x))
            project['norm_'+c] = s_shape(project[c])

        open_closed_score = 5*(project['norm_open_issues_count']+ project['norm_open_pr_count'] +\
                               project['norm_closed_issues_count'] + project['norm_closed_pr_count'])
        activity_fragility_score = 10*(project['norm_weekly_commits'] + project['norm_contrib_stats'])
        score = np.round(open_closed_score + activity_fragility_score, 2)
        return score
    
    
    def stack_score(self):
        """
        Compute the stackoverflow  score based on
        questions about the open source project and
        associated data: answers, likes, views 
        """
        questions = s_shape(self.data.question_count)
        answered_ratio = ratio_of_2cols(self.data, 'is_answered', 'question_count')
        viewed_ratio = ratio_of_2cols(self.data, 'view_count', 'question_count')
        reaction_ratio = ratio_of_2cols(self.data, 'answer_count', 'question_count')
        score = 5 * (questions + answered_ratio + viewed_ratio + reaction_ratio)
        return np.round(score, 2)
    

    def twitter_score(self):
        """
        Compute the Twitter score based on
        tweets about the open source project and
        associated data: retweets, likes, replies,
        quotes
        """
        tweets = s_shape(self.data.tweet_count)
        like_ratio = ratio_of_2cols(self.data, 'like_count', 'tweet_count')
        retweet_ratio = ratio_of_2cols(self.data, 'retweet_count', 'tweet_count')
        quote_reply = s_shape(self.data.reply_count+self.data.quote_count)
        score = 5*(like_ratio + retweet_ratio + quote_reply + tweets)
        return np.round(score, 2)

    def get_json_cols(self):
        github = ['open_issues_count', 'closed_issues_count', 'open_pr_count',
                'closed_pr_count', 'weekly_commits', 'contrib_stats']
        components = ['twitter_activity', 'stackoverflow_activity', 'community_openteams_score']
        return github + components

    def calculate_json_zscore(self):
        for col in self.get_json_cols():
            zscore_ = np.round(zscore(self.data[col], nan_policy='omit'), 2)
            self.data[col+'_zscore'] = zscore_

