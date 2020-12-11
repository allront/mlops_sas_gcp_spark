# -*- coding: utf-8 -*-

"""
score.py is the scoring module for our project.
Remarks: The model package is designed in a way
it's executable both in SAS Viya and GCP Dataproc

Steps:
1 - Read data (both server and gcp cloud storage)
2 - Read serialized pipeline (both server and gcp cloud storage)
3 - Score (or trasform) data
4 - Store scored data (both server and gcp cloud storage)

Author: Ivan Nardini (ivan.nardini@sas.com)
"""

# Libraries ------------------------------------------------------------------------------------------------------------
import logging
import logging.config
import argparse
import yaml

# from helpers import read_parquet, write_parquet, load_model

import pyspark
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
from pyspark.ml import PipelineModel

try:
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession
except ImportError as e:
    print('WARN: Something wrong with pyspark library. Please check configuration settings!')

# Helpers --------------------------------------------------------------------------------------------------------------

def read_parquet (session: SparkSession, filepath: str) -> pyspark.sql.DataFrame:
    '''
    Read a parquet file
    :param session: SparkSession
    :param filepath: the path of parquet datafile
    :return: pyspark.sql.DataFrame
    '''
    return session.read.parquet(filepath)


def write_parquet (df: pyspark.sql.DataFrame, filepath: str) -> None:
    '''
    Write a parquet file
    :param df: DataFrame to store
    :param filepath: the path of parquet datafile
    :return: None
    '''
    df.write.mode('overwrite').save(filepath)


def load_model(filepath:str) -> PipelineModel:
    '''
    Load the fitted pipeline
    :param filepath:
    :return: PipelineModel
    '''
    return PipelineModel.load(filepath)

extract0_udf = udf(lambda value: value[0].item(), DoubleType())
extract1_udf = udf(lambda value: value[1].item(), DoubleType())

# Builders -------------------------------------------------------------------------------------------------------------

def score_model(data:pyspark.sql.DataFrame, model:PipelineModel) -> pyspark.sql.DataFrame:
    predictions_test = model.transform(data)
    return predictions_test

# Main -----------------------------------------------------------------------------------------------------------------

def run_scoring(args):
    # Read Configuration
    logging.info('Read config file.')
    with open(args.configfile, "r") as cf:
        config = yaml.load(cf, Loader=yaml.FullLoader)
    sparksession = config['sparksession']
    data = config['data']
    output = config['output']

    # Initiate the Spark session
    logging.info('Instantiate the {0} Spark session'.format(sparksession['appName']))
    spark = SparkSession.builder \
        .master(sparksession['master']) \
        .appName(sparksession['appName']) \
        .getOrCreate()

    # Load Data
    logging.info('Load data to score')
    datatoscore = read_parquet(spark, data['datatoscore_path'])

    # Load Model
    logging.info('Load trained pipeline')
    pipemodel = load_model(output['pipeline_path'])

    # Score data
    datascored = score_model(datatoscore, pipemodel)
    datascored.select(output['showschema_score']).show(5)

    # Store scored data
    logging.info('Save all the process outputs')
    datascored_fmt = datascored.withColumn('P_Unusual0', extract0_udf('probability')).withColumn(
         'P_Unusual1', extract1_udf('probability')).select(output['columnschema_score'])
    write_parquet(datascored_fmt, output['datascored_path'])


if __name__ == "__main__":
    logging.config.fileConfig("../../config/logging/local.conf")
    logger = logging.getLogger(__name__)
    parser = argparse.ArgumentParser(description="Score with Pyspark GBTClassifier")
    parser.add_argument('--configfile', required=True, help='path to configuration yaml file')
    args = parser.parse_args()
    run_scoring(args)
