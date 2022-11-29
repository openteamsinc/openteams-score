from github import Github
from dotenv import load_dotenv
import os
from utils import setup_logger

path = os.environ['COMMUNITY_LOGS_PATH']
LOGGER = setup_logger('governance_log', f'{path}governance.log')
class MarkdownParser():
    ''' This class uses Github wrapper to query a given repo and identify
    files that might potentially contain project governance-related content. '''
    
    def __init__(self, token=None):
        self.token = token
        self.github = Github(token)
    
    def get_mds(self, repo):
        '''Assumes that goverance-related content will most likely be 
        in the main directory and contained in Markdown files. '''
        repo = self.github.get_repo(repo)
        
        md_files = []
        for f in repo.get_contents(''):
            if '.md' in f.path:
                md_files.append(f)
        md_files = [f.decoded_content.decode("utf-8")]
        return md_files
    
      
    def get_extension(self, repo, ext):
        '''Fetches a list of all files in a repo which has the 
        input extension if exists in any of the files'''

        ext_files = []
        try:
            repo = self.github.get_repo(repo)
            contents = repo.get_contents('')
            while contents:
                file_content = contents.pop(0)
                if ext in file_content.path:
                    ext_files.append(file_content)
                if file_content.type == "dir":
                    contents.extend(repo.get_contents(file_content.path))
        except Exception as e:
            LOGGER.exception(f'An exception occurs while getting a list of all files with input extension in the repo {repo} : {e}')
        
        
        return [f.decoded_content.decode("utf-8") for f in ext_files]
    
    def get_readme(self, repo):
        ''' Gets the README.md file from a repo'''
        LOGGER.info(f'Getting readme file for repo {repo}')
        try:
            repo = self.github.get_repo(repo)
            readme = repo.get_contents('README.md')
            readme = readme.decoded_content.decode("utf-8")
        except Exception as e:
            LOGGER.exception(f'An exception occurs while getting readme file for repository {repo} : {e}')
            readme = ''
        return readme
    
    def get_contents(self, repo):
        ''' Gets all the content file names from the repo. '''
        try:
            repo = self.github.get_repo(repo)
            contents = repo.get_contents('')
        except Exception as e:
            LOGGER.exception(f'An exception occurs while getting content file name from repository {repo} : {e}')
            contents = ''
        return contents
    
    def get_all_files(self, repo):
        '''Returns a list of all files in the repo.'''
        md_files = []
        try:
            repo = self.github.get_repo(repo)
            contents = repo.get_contents('')
            while contents:
                file_content = contents.pop(0)
                if '.md' in file_content.path:
                    md_files.append(file_content)
                if file_content.type == "dir":
                    contents.extend(repo.get_contents(file_content.path))
        except Exception as e:
            LOGGER.exception(f'An exception occurs while getting a list of all files in the repo {repo} : {e}')
        
        return [f.decoded_content.decode("utf-8") for f in md_files]

if __name__ == '__main__':
    parser = MarkdownParser(token=os.environ['GITHUB_AUTH_TOKEN'])
    readme = parser.get_readme('numpy/numpy')
    # print(readme)

    ext = parser.get_extension('numpy/numpy', '.rst') #Numpy's documentation is in rst format
    print(ext)

