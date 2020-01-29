from os import environ, listdir, path
import json
from pyspark import SparkFiles
from pyspark.sql import SparkSession

from dependencies import logging
def start_spark(app_name='my_spark_app', master='local[*]',spark_config={}):
    spark_builder = (
        SparkSession
            .builder
            .master(master)
            .appName(app_name))

    for key, val in spark_config.items():
        spark_builder.config(key, val)

    spark_sess = spark_builder.getOrCreate()
    spark_logger = logging.Log4j(spark_sess)

    spark_sess.sparkContext.addFile('configs/etl_config.json')

    with open(SparkFiles.get('etl_config.json'),'r') as cf:
        config_dict = json.load(cf)

    return spark_sess, spark_logger, config_dict









