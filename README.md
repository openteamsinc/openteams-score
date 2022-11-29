# Description

OpenTeams Score is a scoring algorithm that assigns a score to all open source projects we are indexing.  The score is a weighted average of the 5 components, namely community, popularity, security, license, and versioning.  The score is intended to provide a consistent and transparent measure that can potential guide a business user in choosing OSS packages that best fit their project goals.

V.1 of the project takes into account only 5 of these components as a proof of concept and development will continue as we improve the project in V.2, which we are currently working on. 


## 1. Data Collection 

The data for the subscores comes from two main sources:
- queries from the libraries.io DB that OpenTeams indexes 
- data scraped from GitHub
- queries from GitHub API
- queries from Twitter API
- queries from stack exchange API 

Most of the data for the popularity data all come from the `repositories` and `projects` tables from the libraries.io database:
- contributions_count
- subscribers_count     
- dependent_repos_count 
- stargazers_count      
- dependents_count      
- forks_count

Some of the package managers offer download statistics for projects. If possible, we collect either the downloads from the last 90 days or we get the total downloads counts of a project. We do not punish projects for not having downloads data since many package managers do not offer any data. We either use public APIs or scrape the data as necessary.

Similarly, for the license data, we get all of the data from libraries.io.

For the community data, the table below shows all data points used along with their sources.


| Data points                                                           | Source          |
|-----------------------------------------------------------------------| --------------- |
| documentation, has_contribution_guidelines, has_readme                | libraries.io    |
| open_issues_count, closed_issues_count, open_pr_count, closed_pr_count| GitHub scraping |
| weekly_commits, contrib_stats                                         | GitHub API      |
| num_tweet_likes, num_tweets, num_retweets, num_tweet_quotes, num_tweet_replies | Twitter API|
| num_answered_stack, num_questions_stack, num_views_stack, num_answers_stack | stack exchange API|


## 2. Scoring

All component scores are within [0, 100].

- **2.1 Popularity Score**

The table below shows columns or fields along with their types and contributions to the popularity score.

If no downloads data are available:

| Field name            | Type    | subscore |
| ----------------------|---------|--------- |
| contributions_count   | numeric | 15       |
| subscribers_count     | numeric |  5       |
| dependent_repos_count | numeric | 40       |
| stargazers_count      | numeric | 15       |
| dependents_count      | numeric | 20       |
| forks_count           | numeric |  5       |

If downloads in the last 90 days are available:

| Field name            | Type    | subscore |
| ----------------------|---------|--------- |
| contributions_count   | numeric | 15       |
| subscribers_count     | numeric |  5       |
| dependent_repos_count | numeric | 40       |
| stargazers_count      | numeric | 10       |
| dependents_count      | numeric | 15       |
| forks_count           | numeric |  5       |
| recent_downloads      | numeric | 10       |

If total downloads are available:

| Field name            | Type    | subscore |
| ----------------------|---------|--------- |
| contributions_count   | numeric | 15       |
| subscribers_count     | numeric | 10       |
| dependent_repos_count | numeric | 40       |
| stargazers_count      | numeric | 10       |
| dependents_count      | numeric | 15       |
| forks_count           | numeric |  5       |
| total_downloads       | numeric |  5       |

The first step is to normalize these fields by applying the composition of functions:

`s_shape(logscale(x))`

where  s_shape and logscale are respectively: `np.log(1+x)` and `x/(x+1)`.

Then we scale each normalized field value by its `subscore` value and sum overall (dot product):  

```
A = s_shape(logscale(df[columns]))
score = round(A.dot(coefs), 2)
```
where `columns`and `coef` are respectively the fields in the table above and the `subscores` all aligned.

- **2.2 Community Score**

This score has 4 components:

- fundamental score
- GitHub activity score 
- Twitter activity score
- StackOverflow activity score

The table below shows each component along with columns or fields used to calculate them, their types, and contributions to the overall community score.

| components | Field names                                            | Type    | subscore |
|------------|--------------------------------------------------------| ------- | -------- |
|fundamental | documentation, has_contribution_guidelines, has_readme, governance | Boolean |    20    |
| GitHub     | open_issues_count, closed_issues_count, open_pr_count, closed_pr_count, weekly_commits, contrib_stats | Numeric |   40 |
| Twitter | num_tweet_likes, num_tweets, num_retweets, num_tweet_quotes, num_tweet_replies | Numeric | 20 |
| StackOverflow | num_answered_stack, num_questions_stack, num_views_stack, num_answers_stack | Numeric | 20 |

* fundamental score:

This component assesses fundamental or bare minimum documents that a project must have to make it comprehensive.
The formula is: `20*(0.4*doc + 0.3*contrib + 0.2*readme + 0.1*governance)`

where `doc`, `contrib`, `readme` and `governance`, named after the boolean fields, are converted to 0 (False) and 1 (True).
Each field has a coefficient based on its importance.

* GitHub activity score:

This component evaluates the activity of the community for building and maintaining the project. 

The formula of this component is:

`5 * (s_shape(open_issues_count) + s_shape(open_pr_count) + s_shape(closed_issues_count) + s_shape(closed_pr_count)) + 10 * (s_shape(weekly_commits) + s_shape(contrib_stats))`

Note that each variable is normalized which makes the score contribute to 40 in the community score.
`contrib_stats` is the highest number of contributors whose commits account for < 55% of all commits. This particular sub-component account for the fragility of the project activity. The lower, the worst it is, as the biggest part of the work depends on very few people while if it is larger, it is a sign that the work is spread in the community. 

* Twitter activity score:

This component measures the community social interaction related to the project.
The formula is:

`5*(s_shape(num_tweets) +  s_shape(num_tweet_quotes + num_tweet_replies) s_shape(like_ratio) + s_shape(retweet_ratio))` where

`num_tweets` is the total number of tweets related to the project during the last month,

`like_ratio = num_tweet_likes/num_tweets ` 

`retweet_ratio = num_retweets/num_tweets`

* stack activity score:

This component assesses the learning engagement/activity of the community by means of QAs from StackOverflow.
The formula is:

`5 * (s_shape(num_questions_stack) + s_shape(answered_ratio) + s_shape(viewed_ratio) + s_shape(reaction_ratio))`

`num_questions_stack` is the number of questions related to the project within the last 90 days and is a column from the community table. The remaining variables are calculated as follows:

`answered_ratio = num_answered_stack/num_questions_stack`

`viewed_ratio = num_views_stack/num_questions_stack`

`reaction_ratio = num_answers_stack/num_questions_stack`

- **2.3 License Score** 

We have assigned a `permissiveness_score` to each of the most popular OSS licenses using our judgment based on public data. This list of licenses will be expanded into all OSS licenses in `OpenTeams Score` in the future. Please note, these scores could change after we receive feedback and this list will be expanded to all licenses. Below is a sample from the current list of licenses that the projects in the pilot list had and their associated permissiveness scores, as well as their perceived legal risk, and whether the license is [copyleft](https://fossa.com/blog/all-about-copyleft-licenses/) or not.

| License | Permissiveness Score | Legal Risk | Copyleft |
| ------ | ------ | ------ | ------ |
| GPL-2.0 | 40| high | yes |
| MIT-0 | 95 | low | no |
| MS-PL | 60 | high | yes |
| CC-BY-SA-4.0 | 80 | medium | yes |
| LGPL | 60 | high | yes |
| EPL-1.0 | 60 | high | yes |
| BSD-3-Clause-Clear | 90 | low | no|
| BSL-1.0 | 90 | low | no |
| ... | ... | ... | ... |


- **2.4 Security Score**

We are using the [Scorecards](https://github.com/ossf/scorecard), an open source security check application developed by Google to compute the security score component of the OpenTeams Score. Currently, we are using 16 of Scorecards checks that the application looks at in a project repository and assigns a score out of 10 for each of these checks. The current list of checks we are using is: 


```
- CII-Best-Practices
- Fuzzing
- Pinned-Dependencies
- CI-Tests
- Maintained
- Packaging
- SAST
- Dependency-Update-Tool
- Token-Permissions
- Security-Policy
- Signed-Releases
- Binary-Artifacts
- Branch-Protection
- Code-Review
- Contributors
- Vulnerabilities
```



For details of each check, see [here](https://github.com/ossf/scorecard#checks-1).


Scorecards can store the output in a `json` or `csv` file. For this, you will need to install the most recent version of Scorecards (v 4.0.1 as of 1/22). Please note, if on a Mac OS, installing through Homebrew currently does not have the formula for the latest version, which supports exporting output to a file. For this, download the binary file from the [release page](https://github.com/ossf/scorecard/releases/tag/v4.0.1).

We used a Python subprocess to run the scorecards in our script for this version of the MVP. This might change as we expand the project depending on efficiency requirements as we scale to other projects. 

Scorecards can be run in Docker and in the command line with the URL of the project that is being scored. For this, and for any other method of using Scorecards, you will need to have your Github API Token stored in an environment variable in your machine. See details [here](https://github.com/ossf/scorecard#authentication)


**Command line example:** 

`scorecard —repo=https://github.com/numpy/numpy` 

If storing in json: 

`scorecard —repo=https://github.com/numpy/numpy  --format=json` 

If storing in CSV:

`scorecard —repo=https://github.com/numpy/numpy  --format=csv`

We ran scorecards on the pilot list of projects and exported the details of each check in a CSV file, which will constitute the `security_table` file that will be part of the data fed into the `Project_Score` table in our DB in this iteration of the project.


- **2.5 Governance**

We prepared [a brief governance survey](https://docs.google.com/forms/d/e/1FAIpQLSfCLaD7Hc9FSEIokYivzIBERKGaPZEwAKAsZB6QEWRQIJeQ9g/viewform) and shared it with a small number of maintainers to gain some insights into common and preferred governance practices in popular projects and evaluate other projects partially in the light of these insights. 

The form of governance as part of the OpenTeams score is limited to whether there is any indication, information, reference, or file in a project repo regarding its governance. The weight of such absence or governance will be minimal in the overall score. Since our goal is to make such absences (or presence) visible as a factor that is being taken into account when evaluating a project's OpenTeams score. 

Going forward, the governance aspect might be a collaborative effort with the open source community with the goal of enabling projects to make their governance practices more visible and well defined if they are not already as such. 

- **2.6 Versioning**

We are investigating the `Versioning` table in the libraries.io DB and will be using the data in this table to design an algorithm to compute a score for projects. 

**Community perception/interpretation of software versioning:**

An OSS version is a label indicating a set of reached development milestones.

The OSS community considers frequent releases as a good indicator of the software activity, backward compatibility problem mitigations, and a way to get user tests’ feedback early to rapidly improve the software resulting in better/higher quality software.

Thus to account for this information,  we propose to calculate the versioning score by combining the following data points:
- Total number of existing versions per type (major, minor, patch)
- Dates of all existing versions per type

Based on these data points, we derive the following metrics:
- Weighted sum of number existing versions
- Weighted mean time between consecutive versions 
- Weighted frequency of version updates 

The versioning score is the mean of these 3 metrics but we don't penalize projects when
some of these metrics are null.

**Calculation details:**

- Weighted sum of number existing versions formula is:

`(5*number_of_majors + 2*number_of_minors + number_of_patches)/8.0`

- Weighted mean time between consecutive versions formula is:

`s_shape(logscale( (6/majors_meantime) + (4/minors_meantime) + (2/patches_meantime) ))`

The rationale behind these coefficients is that there are more patches than minor and major releases
successively.

- Weighted frequency of version updates

`s_shape(logscale(minor_frequency + patch_frequency]))`

where

`minor_frequency = number_of_minors/(ages/(365/3))`

`patch_frequency = number_of_patches/(ages/(365/18))`

Here we expect minor release to be quarterly while patch release to be more frequent.













