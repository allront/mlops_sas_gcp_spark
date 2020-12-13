# -*- coding: utf-8 -*-

"""
migrate.py is the module to move model content
on GCP Cloud Storage.

Assumptions:

Steps:
1 - Create a client
2 - Check for the bucket
3 - Upload files

Author: Ivan Nardini (ivan.nardini@sas.com)
"""
import argparse
import os
import logging
import logging.config
import yaml
from google.cloud import storage

# Configfile path variable
# CONFIGPATH = "../../../config/demo-workflow-config.yml"
# Run path
CONFIGPATH = "./config/demo-workflow-config.yml"

# Helpers --------------------------------------------------------------------------------------------------------------

def upload_file_bucket (bucket: str, file: str, remotefile: str) -> None:
    '''

    :param bucket: Target Bucket Name
    :param file: The file name to upload
    :param remotefile: The remote name file
    :return: None
    '''

    client = storage.Client()
    bucket = client.get_bucket(bucket)
    blob = bucket.blob(remotefile)
    blob.upload_from_filename(filename=file)

# Main -----------------------------------------------------------------------------------------------------------------

def run_migrate(args):

    # Read configuration
    logging.info('Read config file.')
    with open(CONFIGPATH, "r") as cf:
        config = yaml.load(cf, Loader=yaml.FullLoader)

    # Set variables
    logging.info('Set running variables')
    bucketname = args.bucket_name
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config['workflow']['migrate']['authfile']
    modelpath = config['workflow']['build']['modelpath']
    modelpath_gcp = config['workflow']['migrate']['modelpath_gcp']

    for file in os.listdir(modelpath):
        logging.info(f'Get {file}')
        upload_file_bucket(bucketname, f"{modelpath}/{file}", f"{modelpath_gcp}/{file}")
        logging.info(f'{file} uploaded in {modelpath_gcp}/{file}')


if __name__ == '__main__':

    # User variables for Viya
    parser = argparse.ArgumentParser(description="Migrate artefact on GCP CLoud storage")
    parser.add_argument('--bucket-name', default='network-migrate', help='The name of the model to deploy')
    args = parser.parse_args()

    #logging.config.fileConfig("../../../config/logging/local.conf")
    #logger = logging.getLogger(__name__)
    run_migrate(args)