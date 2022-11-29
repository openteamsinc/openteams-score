import numpy as np
import pandas as pd
import panel as pn
import holoviews as hv
import json
hv.extension('bokeh')

from scipy.stats import zscore

def convert_sec_score(score):
    """
    The security scores are stored as strings of the
    form score/10 (4.5/10). If a project has a score
    we'll extract it. Otherwise, we'll return nan.
    """
    try:
        return float(score.split('/')[0])
    except: return np.nan

def find_range(val):
    """
    The function helps to determine where a project
    sits among others. It is used to dispatch the
    the markdown used to describe a project.
    """
    if val <= -0.5:
        return 'low'
    elif -0.5 < val < 0.5:
        return 'mid'
    return 'high'

def lic_score(df, p_id):
    """
    This returns the permissiveness and license
    for a project specified by `p_id`. If the
    project has no license it returns nan for
    both.
    """
    out = df[df.project_id==p_id]
    if out.size==0:
        return np.nan, np.nan
    return out.permissiveness_score.tolist()[0], out.license.tolist()[0]

def get_copyleft(df, p_id):
    out = df[df.project_id==p_id]
    if out.size==0:
        return None

    return out.copyleft.tolist()[0]

def shorten_names(name):
    return name.split('/')[-1].capitalize()

def str_to_int(val):
    try:
        return int("".join(val.split(',')))
    except AttributeError:
        return val

def key_from_value(d, val):
    return list(d.keys())[list(d.values()).index(val)]

def load_dfs(json_column):
    """
    Makes two dataframes from the data stored in the json columns of the
    full dataframe.
    """
    labels = [c['column'] for c in json.loads(json_column.tolist()[0])['subcomponents_score']]
    data = {c: [] for c in labels}
    data['score'] = []
    data_zscores = {c: [] for c in labels}
    data_zscores['score'] = []
    for d in json_column:
        if d == '0':
            data['score'].append(np.nan)
            data_zscores['score'].append(np.nan)
            for l in labels:
                data[l].append(np.nan)
                data_zscores[l].append(np.nan)
            continue
        scores = json.loads(d)
        data['score'].append(scores['component_score']['score'])
        if 'z-score' in scores['component_score'].keys():
            data_zscores['score'].append(scores['component_score']['z-score'])
        else:
            data_zscores['score'].append(np.nan)

        for s in scores['subcomponents_score']:
            name = s['column']
            data[name].append(s)
            data[name][-1]['score'] = data[name][-1].pop('actual_value')
            if 'description' in data[name][-1].keys():
                data[name][-1]['documentation'] = {'short': data[name][-1].pop('description')}
            try:
                if s['z-score'] == 'NaN':
                    data_zscores[name].append(np.nan)
                    continue
                data_zscores[name].append(s['z-score'])
            except:
                data_zscores[name].append(np.nan)

    return pd.DataFrame(data, index=json_column.index), pd.DataFrame(data_zscores, index=json_column.index)

class App:
    """
    The main object for organizing the panel app.
    """
    def __init__(self, openteams_score = 'data/qs_demo_projects.csv'):

        # load data
        self.ots_data = pd.read_csv(openteams_score).set_index('repository_id')
        self.ots_data['total_zscore'] = zscore(self.ots_data.openteams_score, nan_policy='omit')
        self.community_data, self.community_zscores = load_dfs(self.ots_data.community_json)
        self.popularity_data, self.popularity_zscores = load_dfs(self.ots_data.popularity_json)
        self.security_data, self.security_zscores = load_dfs(self.ots_data.security_json)
        self.license_data, self.license_zscores = load_dfs(self.ots_data.license_json)
        self.version_data, self.version_zscores = load_dfs(self.ots_data.version_json)

        frames = [self.community_data, self.community_zscores,
            self.popularity_data, self.popularity_zscores,
            self.security_data, self.security_zscores,
            self.license_data, self.license_zscores,
            self.version_data, self.version_zscores]

        for frame in frames: frame['project_name'] = self.ots_data.project_name

        # some useful resources
        self.id_mapping = {name: repository_id for name, repository_id in zip(
            self.popularity_data.project_name, self.popularity_data.index)}
        self.name_mapping = {partial_name: full_name for partial_name, full_name in zip(
            self.popularity_data.project_name.apply(shorten_names), self.popularity_data.project_name
        )}
        self.name_mapping_lower = {partial_name.lower(): full_name for partial_name, full_name in zip(
            self.popularity_data.project_name.apply(shorten_names), self.popularity_data.project_name
        )}

        # setup widgets

        # selector
        self.popularity_data['short_name'] = self.popularity_data.project_name.apply(shorten_names)
        self.popularity_data = self.popularity_data.sort_values('short_name')
        self.project_select = pn.widgets.Select(
            options=self.popularity_data.short_name.tolist(), width=200)
        self.repo_input = pn.widgets.TextInput(placeholder='Enter a project name (i.e. cmath)')
        self.button = pn.widgets.Button(name='Run',
                                        button_type='primary',
                                        width=200)
        self.warning_markdown = pn.pane.Markdown('', width=200)
        self.input_row = pn.Row(self.project_select, self.repo_input, self.button, self.warning_markdown)
        self.button.on_click(self.click_update) # updates on click or selector change

        # total score
        self.score_markdown = pn.pane.Markdown(width=500)
        self.main_plot = pn.pane.HoloViews(object=None, width=600, height=225)
        self.score_row = pn.Row(self.score_markdown, self.main_plot, height=225)

        # popularity
        self.popularity_markdown = pn.pane.Markdown(width=500)
        self.popularity_plot = pn.pane.HoloViews(object=None, width=600, height=325)
        self.popularity_row = pn.Row(self.popularity_markdown, self.popularity_plot, height=400)

        # community
        self.community_markdown = pn.pane.Markdown(width=500)
        self.community_plot1 = pn.pane.HoloViews(object=None, width=250, height=325)
        self.community_plot2 = pn.pane.HoloViews(object=None, width=350, height=325, rot=45)
        self.community_row = pn.Row(self.community_markdown, self.community_plot1,
            self.community_plot2, height=700)

        # license
        self.license_markdown = pn.pane.Markdown(width=500, height=125)
        self.license_plot = pn.pane.HoloViews(object=None, width=600, height=125)
        self.license_row = pn.Row(self.license_markdown, self.license_plot)

        # security
        self.security_markdown = pn.pane.Markdown(width=500, height=125)
        self.security_plot = pn.pane.HoloViews(object=None, width=600, height=325)
        self.security_row = pn.Row(self.security_markdown, self.security_plot)

        # versioning
        self.versioning_markdown = pn.pane.Markdown(width=500, height=175)
        self.versioning_plot = pn.pane.HoloViews(object=None, width=600)
        self.versioning_row = pn.Row(self.versioning_markdown, self.versioning_plot)

        # whole layout
        self.layout = pn.Column(self.input_row,
                                    self.score_row,
                                    self.popularity_row,
                                    self.community_row,
                                    self.versioning_row,
                                    self.license_row,
                                    self.security_row,
                                    )

        # define responses
        self.score_responses = {'low': "has a low score. Its score is below the mean by at least half \
                        of a standard deviation",
                          'mid': "has a medium score. Its score falls within half of a standard deviation \
                        of the mean.",
                          'high': "has a high score. Its score is greater than the mean by at least \
                        half of a standard deviation."}
        self.main_procedure = "After calculating the subscore for each individual component, we generated an \
            aggregate score by performing a weighted sum."

        self.project_select.param.watch(self.select_update, "value")
        self.select_update('') # app starts out empty. Call update initially to load contents
        self.layout.show()
        self.layout.servable()

    def click_update(self, event):
        """
        Checks if the text input is valid. If so it
        proceeds to set the `current_name` and then
        calls update.
        """
        if self.repo_input.value == '':
            self.warning_markdown.object = 'Please enter a project name.'
            return
        elif self.repo_input.value in self.name_mapping.keys():
            long_name = self.name_mapping[self.repo_input.value]
        elif self.repo_input.value in self.name_mapping_lower.keys():
            long_name = self.name_mapping_lower[self.repo_input.value]
        else:
            self.warning_markdown.object = 'No data found for that project.'
            return

        self.current_name = long_name
        self.project_select.value = key_from_value(self.name_mapping, long_name)
        self.update()

    def select_update(self, event):
        """
        Sets `current_name` and then calls update.
        """
        self.current_name = self.name_mapping[self.project_select.value]
        self.update()

    def get_data(self):
        """
        Collects all of the data for a specific project.
        Includes preparing data for the barplots which are
        specified in the form:
        [('label1', scaler_value1), ('label2',scaler_value2),...]
        """
        current_id = self.id_mapping[self.current_name]
        self.current_zscores = {
            'popularity': self.popularity_zscores.loc[current_id].score,
            'community': self.community_zscores.loc[current_id].score,
            'security': self.security_zscores.loc[current_id].score,
            #'license': lic_score(self.zscore_license, current_id)[0],
            'license': self.license_zscores.loc[current_id].score,
            'version': self.version_zscores.loc[current_id].score,
        }

        self.current_subscores = {
            'popularity': self.popularity_data.loc[current_id].score,
            'community': self.community_data.loc[current_id].score,
            'security': self.security_data.loc[current_id].score,
            #'license': lic_score(self.zscore_license, current_id)[0],
            'license': self.license_data.loc[current_id].score,
            'version': self.version_data.loc[current_id].score,
        }

        self.current_total_score = self.ots_data.loc[current_id].openteams_score
        self.current_total_zscore = self.ots_data.loc[current_id].total_zscore
        self.current_license = self.license_data.loc[current_id].license
        #self.copyleft = get_copyleft(self.license_data, current_id)
        self.copyleft = True ##### temporary

        current_popularity_zscores = self.popularity_zscores.loc[current_id]

        if not np.isnan(current_popularity_zscores.recent_downloads).any():
            columns = ['subscribers_count', 'dependents_count',
            'dependent_repos_count', 'forks_count', 'stargazers_count',
            'contributions_count', 'recent_downloads']
            names = ['Followers', 'Dep. Count', 'Dep. Repos', 'Forks', 'Stars',
                    'Contributors', 'Recent Downloads']
        elif not np.isnan(current_popularity_zscores.total_downloads).any():
            columns = ['subscribers_count', 'dependents_count',
            'dependent_repos_count', 'forks_count', 'stargazers_count',
            'contributions_count', 'total_downloads']
            names = ['Followers', 'Dep. Count', 'Dep. Repos', 'Forks', 'Stars',
                    'Contributors', 'Total Downloads']
        else:
            columns = ['subscribers_count', 'dependents_count',
            'dependent_repos_count', 'forks_count', 'stargazers_count',
            'contributions_count']
            names = ['Followers', 'Dep. Count', 'Dep. Repos', 'Forks', 'Stars',
                    'Contributors']

        self.popularity_plot_data = [(n, current_popularity_zscores[c]) for c,n in zip(columns, names)]

        components = ['popularity', 'community', 'security', 'license', 'version']
        self.main_plot_data = [(c, float(self.current_zscores[c])) for c in components]

        # first community plot
        current_community_data = self.community_data.notna().loc[current_id]
        columns = ['homepage', 'has_contributing',
           'has_readme', 'governance']
        names = ['homepage', 'contributing', 'readme', 'governance']

        # TODO: Remove this workaround when full dataset is restored
        if 'homepage' in current_community_data:
            self.community_plot1_data = [(n, current_community_data[c]) for c,n in zip(columns, names)]
        else:
            print('Warning: some community data is missing.  Using dummy values instead.')
            self.community_plot1_data = [(n, False) for c, n in zip(columns, names)]

        # second community plot
        current_community_zscores = self.community_zscores.loc[current_id]
        columns = ['open_issues_count', 'closed_issues_count',
           'open_pr_count', 'closed_pr_count', 'weekly_commits', 'contrib_stats','twitter_activity','stackoverflow_activity']
        names = ['Open Issues', 'Closed Issues', 'Open PRs', 'Closed PRs', 'Weekly Commits', 'Commit Dist.', 'Twitter', 'Stackoverflow']
        self.community_plot2_data = [(n, current_community_zscores[c]) for c,n in zip(columns, names)]

        # version plot
        current_version_zscores = self.version_zscores.loc[current_id]
        columns = ['versions_count', 'patch_count', 'patch_freq']
        names = ['versions count', 'time between versions',
            'version update frequency']
        self.versioning_plot_data = [(n, current_version_zscores[c] if not None else np.nan) for c,n in zip(columns, names)]

        self.current_security_data = self.security_data.loc[current_id]
        self.current_version_data = self.version_data.loc[current_id]

        self.components = ['Binary-Artifacts',
            'Branch-Protection',
            'CI-Tests',
            'CII-Best-Practices',
            'Code-Review',
            'Contributors',
            'Dependency-Update-Tool',
            'Fuzzing',
            'Maintained',
            'Packaging',
            'Pinned-Dependencies',
            'SAST',
            'Security-Policy',
            'Signed-Releases',
            'Token-Permissions',
            'Vulnerabilities']
        # self.security_plot_data = [(c, float(ast.literal_eval(self.current_security_data[c])['score'])) for c in self.components]
        self.security_plot_data = [(c, float(self.current_security_data[c]['score'])) for c in self.components]

    def update(self):
        """
        Fills all of the panel objects with data. This gets
        called regardless of whether the change was induced
        by a selector change of a text input.
        """

        # update data
        self.get_data()

        # update markdown text

        # remove warning if it's present
        self.warning_markdown.object = ''

        self.score_markdown.object = f"""
## OSSHS: {self.current_total_score} <br>
**{self.current_name}** {self.score_responses[find_range(self.current_total_zscore)]}<br><br>
The highest score for a project in this sample is **{self.ots_data.openteams_score.max()}**.

{self.main_procedure}"""
        self.main_plot.object = hv.Bars(self.main_plot_data, 'OpenTeams Score Components', 'z scores').opts(xrotation=45)

        # popularity
        self.popularity_markdown.object = f"""\n
## Popularity Score: {str(np.round(self.current_subscores['popularity'], decimals=2))}/100

For the popularity subscore we used a linear combination of the following seven variables:\n
- Followers Count\n
- Dependent Count\n
- Dependent Repository Count
- Forks Count\n
- Stars Count\n
- Contributors Count\n
- Project Downloads (whichever is available)\n
    - Total downloads\n
    - Recent downloads (last 90 days)\n
    - If no downloads data is available we don't count it against the project\n
"""
        self.popularity_plot.object = hv.Bars(self.popularity_plot_data, 'Popularity Variables', 'z scores').opts(xrotation=45)

        # community
        self.community_markdown.object = f"""\n
## Community Score: {self.current_subscores['community']}/100

The community subscore consists of a linear combination of the
following variables:\n
- has a documentation page\n
- has contributing guidelines\n
- has a readme\n
- open issues count\n
- closed issues count\n
- open pull request count\n
- closed pull request count\n
- average commits per week\n
- distribution of commits\n
- twitter activity\n
    - number of tweets\n
    - number of tweet likes\n
    - number of retweets\n
    - number of tweet quotes\n

- stackoverflow activity\n
    - number of questions\n
    - number of views\n
    - number of answers\n
    - number of questions answered\n
    - scores\n
"""
        self.community_plot1.object = hv.Bars(self.community_plot1_data, 'Community Variables', 'True/False').opts(xrotation=45)
        self.community_plot2.object = hv.Bars(self.community_plot2_data, 'Other Community Variables', 'z scores').opts(xrotation=45)

        # license
        self.license_markdown.object = f"""\n
## License Score: {self.current_subscores['license']}/100\n

The license subscore is based on how permissive the license is considered to be and whether it is a copyleft license or not. """
        if self.current_license is not np.nan:
            self.license_markdown.object += f"This project uses {self.current_license}."
            if self.copyleft:
                self.license_markdown.object += f"This license is copyleft, which means that this project grants permission  \
                to alter and distribute this software with the requirement that derivative works preserve the same license."
            else:
                self.license_markdown.object += f"This license is not copyleft. It does not require the licensee to distribute derivative works under the same license."
        else:
            self.license_markdown.object += "This project has no license listed in their repository."

        self.license_plot.object = hv.Bars([('License', float(self.current_subscores['license']))],
            ' ', 'Permissiveness Score', extents=(None, 0, None, 100)).opts(invert_axes=True, ylim=(0,100))

        # versioning

        self.versioning_markdown.object = f"""\n
## Versioning Score: {self.current_subscores['version']}/100\n

The versioning subscore is generated from a linear combination of the following variables:\n
- weighted count of versions\n
- weighted mean time between consecutive versions\n
- weighted frequency of version updates\n
        """
        self.versioning_plot.object = hv.Bars(self.versioning_plot_data, 'Versioning Score').opts(xrotation=45)

        # security

        self.security_plot.object = hv.Bars(self.security_plot_data, 'Security Scores', ' ').opts(xrotation=45)

        if not pd.isna(float(self.current_subscores['security'])):
            md_table = '| &nbsp; | Score | Reason | Description |\n'
            md_table += '| ---- | ---- | ---- | ---- |\n'
            for c in self.components:
                d = self.current_security_data[c]
                md_table += f"**{c}** | **{float(d['score'])}** | {d['reason']} | {d['documentation']['short']} |\n"

            self.security_markdown.object = f"""\n
## Security Score: {self.current_subscores['security']}/100

For security score, we use the application named Scorecards
originally developed by Google. Scorecards is released under the OpenSSF.
Scorecards auto-generates a “security score” for open source
projects based on the components below:\n

{md_table}"""
        else:
            self.security_markdown.object = f"""\n
## No Security Score available for this project\n
"""
            

App().layout
