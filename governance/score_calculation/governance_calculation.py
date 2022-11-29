import pandas as pd
import os
import sys
import random
from scipy.stats import zscore
# sys.path.append('../')
from ..data_collection import github_md_parser
print(os.getcwd())



class GovernanceInfo():
    '''This class looks for the existence of any governance related information
    in a project's repository files and if it exists, assigns a binary value of 1
    to the `governance_info` column of the governance.csv file. '''
    
    def __init__(self, repo, token):
        self.repo = repo
        self.parser = github_md_parser.MarkdownParser(token=token)
        self.readme = self.parser.get_readme(self.repo)
        self.contents = self.parser.get_contents(self.repo)
        self.governance_info = self.governance_check(self.repo)
        
    def governance_check(self, repo):
        '''Checks for the existence of governance related information that might be
        represented with one or more keywords in the keywords file in the 
        README.md file. '''

        governance_info = 0
        path_file = os.path.dirname(os.path.abspath(__file__))+"/../data_collection/keywords.csv"
        df = pd.read_csv(path_file)
        keywords = df['keywords'].tolist()
        keyword_set = set(keywords)
        if set(self.readme.lower().split()) & keyword_set:
            governance_info = 1
            #write to governance_table here
        return governance_info
    
    def get_github(self):
        '''Returns the Github object.'''
        return self.parser
    
    def get_contents(self):
        '''Returns a list of all files in the repo.'''
        return self.contents
    
    def get_readme(self):
        '''Returns the README.md file from the repo.'''
        return self.readme
    
    def get_repo(self):
        '''Returns the repo name.'''
        return self.repo
    
    def get_governance_score(self):
        '''Returns the binary value of governance field.'''
        return self.governance_info

    def get_json_cols(self):
        return ['governance']

def get_project_governance_infos(project_full_names, tokens):
    """
    Gets the governance score of many projects
    using a list of github tokens
    """
    scores = []
    for name in project_full_names:
        token = random.choice(tokens)
        gov = GovernanceInfo(name, token)
        scores += [gov.get_governance_score()]

    results = pd.DataFrame()
    results['full_name'] = project_full_names
    results['governance'] = scores
    return results

if __name__ == '__main__':
    governancefile = pd.read_csv('../../data/db_tables/governance_table.csv')
    repo = 'numpy/numpy'
    token = os.environ['GITHUB_AUTH_TOKEN']
    governance = GovernanceInfo(repo,token)
    if governance.governance_check(repo) == 1:
        print('Governance info found in {}'.format(repo))
    else:
        print('No governance info found in {}'.format(repo))
    print(governance.get_governance_score())
    print(governance.get_repo())


