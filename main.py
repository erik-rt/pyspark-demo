"""
etl_job.py
~~~~~~~~~~

Example Apache Spark ETL job definition.
"""

import argparse
from pyspark.sql.functions import sum

from dependencies.spark import start_spark


def main(data_source: str, output_uri: str):
    """Main ETL script definition.

    :return: None
    """
    # start Spark application and get Spark session, logger and config
    spark, log, _config = start_spark(app_name="pyspark-demo")

    # log that main ETL job is starting
    log.warn("etl_job is up-and-running")

    # Extract from S3
    data = extract_data(spark, data_source)

    # Transform
    data = transform_data(data)

    # Load into S3
    load_data(data, output_uri)

    # log the success and terminate Spark application
    log.warn("etl_job is finished")
    spark.stop()
    return None


def extract_data(spark, data_source: str):
    """Extract data from CSV.

    :param spark: Spark session object.
    :return: Spark DataFrame.
    """
    df = spark.read.option("header", "true").csv(data_source)

    return df


def transform_data(df):
    """Transform original dataset.

    :param df: Input DataFrame.
    :return: Transformed DataFrame.
    """
    df = df.groupby("NEIGHBORHOOD").agg(sum("TOTAL UNITS").alias("TOTAL UNITS SUM"))

    return df


def load_data(df, output_uri: str):
    """Write transformed dataframe to S3 bucket output URI.

    :param df: DataFrame to save.
    :param output_uri: Output URI for the dataframe
    :return: None
    """
    df.write.mode("overwrite").parquet(output_uri)
    return None


# entry point for PySpark ETL application
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_source")
    parser.add_argument("--output_uri")
    args = parser.parse_args()

    main(args.data_source, args.output_uri)
