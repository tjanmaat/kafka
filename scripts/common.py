from pyspark.sql.types import StringType
from pyspark.sql import SparkSession


def get_spark_session(app_name, config_key, config_value):
    # TODO: abstract config options to allow multiple keys & values
    return (SparkSession
            .builder
            .master('local')
            .appName(app_name)
            # Add kafka package
            .config(config_key, config_value)
            .getOrCreate())


def get_stream_dataframe_from_kafka_topic(kafka_server, kafka_topic, spark):
    # Create stream dataframe setting kafka server, topic and offset option
    df = (spark
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", kafka_server)  # kafka server
          .option("subscribe", kafka_topic)  # topic
          .option("startingOffsets", "latest")
          .load())

    # Convert binary to string key and value
    return (df
            .withColumn("key", df["key"].cast(StringType()))
            .withColumn("value", df["value"].cast(StringType())))
