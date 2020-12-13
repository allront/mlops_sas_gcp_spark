# -*- coding: utf-8 -*-

"""
build.py is the module to download useful model content.

Assumptions:
1 - hostname and password of a admin user
2 - Try to use plain sasctl (no futher wrappers)

Steps:
1 - Get Project
2 - Get Champion ID
3 - Get Model Content based on params from SAS Workflow (Train, Score and Config file)

Author: Ivan Nardini (ivan.nardini@sas.com)
"""

# Libraries ------------------------------------------------------------------------------------------------------------

# Basic
import argparse
import os
import logging
import logging.config
import yaml

# Viya library
import sasctl
from sasctl import Session, get
from sasctl.services import model_repository as mr

# Configfile path variable
# CONFIGPATH = "../../../config/demo-workflow-config.yml"
# Run path
CONFIGPATH = "./config/demo-workflow-config.yml"


# Main -----------------------------------------------------------------------------------------------------------------

def run_build (args):
    # Read configuration
    logging.info('Read config file.')
    with open(CONFIGPATH, "r") as cf:
        config = yaml.load(cf, Loader=yaml.FullLoader)

    logging.info('Set running variables')
    projname = args.project_name
    modelcontent = [args.requirements, args.train_script, args.configfile, args.score_script]
    hostname = config['workflow']['build']['hostname']
    username = config['workflow']['build']['username']
    password = config['workflow']['build']['password']
    modelpath = config['workflow']['build']['modelpath']

    logging.info('Start Viya Session')
    with Session(hostname=hostname, username=username, password=password, verify_ssl=False):

        logging.info('Get the Project object')
        project = mr.get_project(projname)

        logging.info('Check for the Champion Model')
        try:
            'championModelName' in project.keys()
        except KeyError as errorkey:
            print(errorkey)
        else:
            logging.info('Get the champion id')
            championModelName = project['championModelName']
            champion_id = mr.get_model(championModelName)['id']

            logging.info('Get the content list')
            content_list = get(f"/modelRepository/models/{champion_id}/contents")

            logging.info('Check for file')
            for file in modelcontent:
                try:
                    file in content_list
                except IndexError as errorindex:
                    print(errorindex)
                else:
                    # Write down target file
                    for item in content_list:
                        if item['name'] == file:
                            logging.info(f'Get {file}')
                            filestr = get(f"/modelRepository/models/{champion_id}/contents/{item['id']}/content")
                            outfile = open(os.path.join(modelpath, file), 'w')
                            outfile.write(filestr)
                            logging.info(f'{file} stored.')
                            outfile.close()


if __name__ == '__main__':
    # User variables for Viya
    parser = argparse.ArgumentParser(description="Build server artefact to deploy")
    parser.add_argument('--project-name', default='Network anomaly detection', help='The name of the model to deploy')
    parser.add_argument('--requirements', default='requirement.txt', help='Requirements file')
    parser.add_argument('--train-script', default='train.py', help='Script to train the model on GCP')
    parser.add_argument('--configfile', default='demo-config.yml', help='configuration yaml file')
    parser.add_argument('--score-script', default='score.py', help='Script to score the model on GCP')
    args = parser.parse_args()

    # logging.config.fileConfig("../../../config/logging/local.conf")
    # logger = logging.getLogger(__name__)
    run_build(args)
