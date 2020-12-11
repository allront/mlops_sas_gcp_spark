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
import os
import yaml
from google.cloud import storage

# Configfile path variable
CONFIGPATH = "../../config/demo-workflow.yml"


# Helpers --------------------------------------------------------------------------------------------------------------

def upload_file_bucket (bucket: str, remotefile: str, file: str) -> None:
    '''

    :param bucket: Target Bucket Name
    :param remotefile: The remote name file
    :param file: The file name to upload
    :return: None
    '''

    client = storage.Client()
    bucket = client.get_bucket(bucket)
    blob = bucket.blob(remotefile)
    blob.upload_from_filename(filename=file)

# Main -----------------------------------------------------------------------------------------------------------------


if __name__ == '__main__':
    # Configuration variables
    with open(CONFIGPATH, "r") as cf:
        config = yaml.load(cf, Loader=yaml.FullLoader)

    # Set variables
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config['workflow']['migrate']['authfile']
    modelpath = config['workflow']['build']['modelpath']
    bucket = config['workflow']['migrate']['bucket_name']
    modelpath_gcp = config['workflow']['migrate']['modelpath_gcp']

    for file in os.listdir(modelpath):
        upload_file_bucket(bucket, f"{modelpath_gcp}/{file}", f"{modelpath}/{file}")
