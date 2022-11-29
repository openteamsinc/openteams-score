from base64 import urlsafe_b64decode
# from typing_extensions import Self
import pandas as pd
import subprocess
import datetime
import json
import logging

class SecurityScore():
    '''
    This class calculates the security score of projects
    using the Scorecards app.

    It takes as input the security_score.csv file from the dm and fetches the
    project url from the associated column.
    '''

    def __init__(self, df, logger):
        self.df = df
        self.logger = logger

    def run_scorecard(self):
        '''
        This method runs the scorecard app and returns the output for each
        security check in the app. It takes as its input the set of urls
        '''

        binary_artifacts = []
        branch_protection = []
        code_review = []
        ci_tests = []
        cii_best_practices = []
        contributors = []
        dependency_update_tool = []
        fuzzing = []
        maintained = []
        packaging = []
        pinned_dependencies = []
        sast = []
        security_policy = []
        signed_releases = []
        token_permissions = []
        vulnerabilities = []
        security_score = []
        score_last_calculated = []

        # urls = self.df.loc[self.df['repository_url'] != '', 'repository_url']
        urls = self.df[self.df.repository_url!=''].repository_url
        for url in urls:
            self.logger.info(f'Running scorecard on {url}.')
            result = subprocess.run(['scorecard', '--repo='+url,'--format=json'], stdout=subprocess.PIPE)
            try:
                decoded_result = result.stdout.decode('utf-8')
                scorecard = json.loads(decoded_result)

                # Extract the date the scorecard is run and the aggregate score
                #the given project received and append to the the associated lists.
                score_last_calculated.append(scorecard['date'])
                security_score.append(scorecard['score'])

                # Extract the individual checks from the json returned
                # from the subprocess and store the value in the associated
                # list named after the check's title. In its current version,
                # Scorecards app has the following 16 checks (1/22).

                binary_artifacts.append(scorecard['checks'][0])
                branch_protection.append(scorecard['checks'][1])
                ci_tests.append(scorecard['checks'][2])
                cii_best_practices.append(scorecard['checks'][3])
                code_review.append(scorecard['checks'][4])
                contributors.append(scorecard['checks'][5])
                dependency_update_tool.append(scorecard['checks'][6])
                fuzzing.append(scorecard['checks'][7])
                maintained.append(scorecard['checks'][8])
                packaging.append(scorecard['checks'][9])
                pinned_dependencies.append(scorecard['checks'][10])
                sast.append(scorecard['checks'][11])
                security_policy.append(scorecard['checks'][12])
                signed_releases.append(scorecard['checks'][13])
                token_permissions.append(scorecard['checks'][14])
                vulnerabilities.append(scorecard['checks'][15])
                self.logger.info('Successfully got security data.')
            except:
                self.logger.warning(f'Failed to get scorecard data for {url}.')
                continue

        binary_artifacts = pd.Series(binary_artifacts)
        branch_protection = pd.Series(branch_protection)
        ci_tests = pd.Series(ci_tests)
        cii_best_practices = pd.Series(cii_best_practices)
        code_review = pd.Series(code_review)
        contributors = pd.Series(contributors)
        dependency_update_tool = pd.Series(dependency_update_tool)
        fuzzing = pd.Series(fuzzing)
        maintained = pd.Series(maintained)
        packaging = pd.Series(packaging)
        pinned_dependencies = pd.Series(pinned_dependencies)
        sast = pd.Series(sast)
        security_policy = pd.Series(security_policy)
        signed_releases = pd.Series(signed_releases)
        token_permissions = pd.Series(token_permissions)
        vulnerabilities = pd.Series(vulnerabilities)
        security_score = pd.Series(security_score)
        score_last_calculated = pd.Series(score_last_calculated)

        self.df['Binary-Artifacts'] = binary_artifacts
        self.df['security_score'] = security_score
        self.df['Branch-Protection'] = branch_protection
        self.df['CI-Tests'] = ci_tests
        self.df['CII-Best-Practices'] = cii_best_practices
        self.df['Code-Review'] = code_review
        self.df['Contributors'] = contributors
        self.df['Dependency-Update-Tool'] = dependency_update_tool
        self.df['Fuzzing'] = fuzzing
        self.df['Maintained'] = maintained
        self.df['Packaging'] = packaging
        self.df['Pinned-Dependencies'] = pinned_dependencies
        self.df['SAST'] = sast
        self.df['Security-Policy'] = security_policy
        self.df['Signed-Releases'] = signed_releases
        self.df['Token-Permissions'] = token_permissions
        self.df['Vulnerabilities'] = vulnerabilities
        self.df['security_score'] = security_score
        self.df['score_last_calculated'] = score_last_calculated


