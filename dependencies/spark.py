"""
spark.py
~~~~~~~~

Module containing helper function for use with Apache Spark
"""

from os import listdir, path
import json
from pyspark import SparkFiles
from pyspark.sql import SparkSession


def start_spark(
    app_name="pyspark-demo",
):
    """Start Spark session, get Spark logger and load config files.

    Start a Spark session on the worker node and register the Spark
    application with the cluster. Note, that only the app_name argument
    will apply when this is called from a script sent to spark-submit.
    All other arguments exist solely for testing the script from within
    an interactive Python console.

    :param app_name: Name of Spark app.
    :return: A tuple of references to the Spark session, logger and
        config dict (only if available).
    """

    spark_builder = SparkSession.builder.appName(app_name)

    # Create session
    spark = spark_builder.getOrCreate()
    sc = spark.sparkContext

    # Set up logging
    log4jLogger = sc._jvm.org.apache.log4j
    spark_logger = log4jLogger.LogManager.getLogger("pyspark-logger")

    # Set up Hadoop config to omit the _SUCCESS file from being written
    sc.hadoopConfiguration.set(
        "mapreduce.fileoutputcommitter.marksuccessfuljobs", "false"
    )

    # Get config file if sent to cluster with --files
    spark_files_dir = SparkFiles.getRootDirectory()
    config_files = [
        filename
        for filename in listdir(spark_files_dir)
        if filename.endswith("config.json")
    ]

    if config_files:
        path_to_config_file = path.join(spark_files_dir, config_files[0])
        with open(path_to_config_file, "r") as config_file:
            config_dict = json.load(config_file)
        spark_logger.warn("loaded config from " + config_files[0])
    else:
        spark_logger.warn("no config file found")
        config_dict = None

    return spark, spark_logger, config_dict
