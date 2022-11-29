### Running the OpenTeams Score Panel App

We have implemented a panel app to demonstrate the first version of the project score. To run the app locally, please follow the steps below:
In your shell, cd into the `openteams_score_panel_demo` folder and create a conda environment from the provided `environment.yml` file inside this folder with the following command:

`conda env create --file environment.yml`

Then, activate the environment you have just created:

`conda activate openteams_score_demo`

Finally, start the panel app by running the command:
`panel serve app.py`

This will start a Bokeh localhost server in your browser to query the app for a project name.
This first iteration of the app currently has data for a sample list of ~85 open source projects with varying scores.
