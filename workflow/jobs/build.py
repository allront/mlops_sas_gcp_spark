# -*- coding: utf-8 -*-

"""
build.py is the module to download useful model content.

Assumptions:
1 - hostname and password of a admin user
2 - Try to use plain sasctl (no futher wrappers)

Steps:
1 - Get Project ID
2 - Get Champion ID
3 - Get Model Content based on params from SAS Workflow (Train, Score and Config file)

Author: Ivan Nardini (ivan.nardini@sas.com)
"""

# Libraries ------------------------------------------------------------------------------------------------------------

# Basic
import argparse
import os
import yaml

# Viya library
import sasctl
from sasctl import Session, get
from sasctl.services import model_repository as mr

# Configfile path variable
CONFIGPATH = "../../config/demo-workflow.yml"

# Main -----------------------------------------------------------------------------------------------------------------

if __name__ == '__main__':

    # User variables for Viya
    parser = argparse.ArgumentParser(description="Build server artefact to deploy")
    parser.add_argument('--project-name', required=True, help='The name of the model to deploy')
    parser.add_argument('--train-script', required=True, help='Script to train the model on GCP')
    parser.add_argument('--configfile', required=True, help='configuration yaml file')
    parser.add_argument('--score-script', required=True, help='Script to score the model on GCP')
    args = parser.parse_args()

    # Configuration variables
    with open(CONFIGPATH, "r") as cf:
        config = yaml.load(cf, Loader=yaml.FullLoader)

    projname = args.project_name
    modelcontent = [args.train_script, args.configfile, args.score_script]
    hostname = config['workflow']['build']['hostname']
    username = config['workflow']['build']['username']
    password = config['workflow']['build']['password']
    modelpath = config['workflow']['build']['modelpath']

    # Start a Session to download model content
    with Session(hostname=hostname, username=username, password=password, verify_ssl=False):

        # Get the project object
        project = mr.get_project(projname)

        # Check for champion
        try:
            'championModelName' in project.keys()
        except KeyError as errorkey:
            print(errorkey)
        else:
            # Get the champion id
            championModelName = project['championModelName']
            champion_id = mr.get_model(championModelName)['id']

            # Get the content list
            content_list = get(f"/modelRepository/models/{champion_id}/contents")

            # Check for file
            for file in modelcontent:
                try:
                    file in content_list
                except IndexError as errorindex:
                    print(errorindex)
                else:
                    # Write down target file
                    for item in content_list:
                        if item['name'] == file:
                            filestr = get(f"/modelRepository/models/{champion_id}/contents/{item['id']}/content")
                            outfile = open(os.path.join(modelpath, file), 'w')
                            outfile.write(filestr)
                            outfile.close()