# -*- coding: utf-8 -*-

"""
helpers.py is the help module for our project.

Author: Ivan Nardini (ivan.nardini@sas.com)
"""

# Libraries ------------------------------------------------------------------------------------------------------------
import pyspark
from pyspark.sql.functions import col, lit, udf
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


def metrics (session: SparkSession, dataframe: pyspark.sql.DataFrame, actual: str,
             predicted: str) -> pyspark.sql.DataFrame:
    '''
    Calculates evaluation metrics from predicted results

    :param dataframe: spark.sql.dataframe with the real and predicted values
    :param actual: Name of column with observed target values
    :param predicted: Name of column with predicted values
    :return:
    '''

    # Along each row are the actual values and down each column are the predicted
    dataframe = dataframe.withColumn(actual, col(actual).cast('integer'))
    dataframe = dataframe.withColumn(predicted, col(predicted).cast('integer'))
    cm = dataframe.crosstab(actual, predicted)
    cm = cm.sort(cm.columns[0], ascending=True)

    # Adds missing column in case just one class was predicted
    if not '0' in cm.columns:
        cm = cm.withColumn('0', lit(0))
    if not '1' in cm.columns:
        cm = cm.withColumn('1', lit(0))

    # Subsets values from confusion matrix
    zero = cm.filter(cm[cm.columns[0]] == 0.0)
    first_0 = zero.take(1)

    one = cm.filter(cm[cm.columns[0]] == 1.0)
    first_1 = one.take(1)

    tn = first_0[0][1]
    fp = first_0[0][2]
    fn = first_1[0][1]
    tp = first_1[0][2]

    # Calculate metrics from values in the confussion matrix
    if (tp == 0):
        acc = float((tp + tn) / (tp + tn + fp + fn))
        sen = 0
        spe = float((tn) / (tn + fp))
        prec = 0
        rec = 0
        f1 = 0
    elif (tn == 0):
        acc = float((tp + tn) / (tp + tn + fp + fn))
        sen = float((tp) / (tp + fn))
        spe = 0
        prec = float((tp) / (tp + fp))
        rec = float((tp) / (tp + fn))
        f1 = 2 * float((prec * rec) / (prec + rec))
    else:
        acc = float((tp + tn) / (tp + tn + fp + fn))
        sen = float((tp) / (tp + fn))
        spe = float((tn) / (tn + fp))
        prec = float((tp) / (tp + fp))
        rec = float((tp) / (tp + fn))
        f1 = 2 * float((prec * rec) / (prec + rec))

    # Print results
    print('Confusion Matrix and Statistics: \n')
    cm.show()

    print('True Positives:', tp)
    print('True Negatives:', tn)
    print('False Positives:', fp)
    print('False Negatives:', fn)
    print('Total:', dataframe.count(), '\n')

    print('Accuracy: {0:.2f}'.format(acc))
    print('Sensitivity: {0:.2f}'.format(sen))
    print('Specificity: {0:.2f}'.format(spe))
    print('Precision: {0:.2f}'.format(prec))
    print('Recall: {0:.2f}'.format(rec))
    print('F1-score: {0:.2f}'.format(f1))

    # Create spark dataframe with results
    l = [(acc, sen, spe, prec, rec, f1)]
    df = session.createDataFrame(l, ['Accuracy', 'Sensitivity', 'Specificity', 'Precision', 'Recall', 'F1'])
    return df

def save_pipeline(pipeline: PipelineModel, filepath:str) -> None:
    '''
    Serialize the fitted pipeline
    :param pipeline:
    :param filepath:
    :return: None
    '''
    pipeline.write().overwrite().save(path=filepath)

def load_model(filepath:str) -> PipelineModel:
    '''
    Load the fitted pipeline
    :param filepath:
    :return: PipelineModel
    '''
    return PipelineModel.load(filepath)
