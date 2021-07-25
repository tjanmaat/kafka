from pyspark.sql.types import StringType, StructType, StructField, BooleanType, LongType, IntegerType
from pyspark.sql.functions import from_json, col, from_unixtime, to_date, to_timestamp, window, to_json, struct

from common import get_stream_dataframe_from_kafka_topic, get_spark_session


def create_event_schema():
    return StructType(
        [StructField("$schema", StringType(), True),
         StructField("bot", BooleanType(), True),
         StructField("comment", StringType(), True),
         StructField("id", StringType(), True),
         StructField("length",
                     StructType(
                         [StructField("new", IntegerType(), True),
                          StructField("old", IntegerType(), True)]), True),
         StructField("meta",
                     StructType(
                         [StructField("domain", StringType(), True),
                          StructField("dt", StringType(), True),
                          StructField("id", StringType(), True),
                          StructField("offset", LongType(), True),
                          StructField("partition", LongType(), True),
                          StructField("request_id", StringType(), True),
                          StructField("stream", StringType(), True),
                          StructField("topic", StringType(), True),
                          StructField("uri", StringType(), True)]), True),
         StructField("minor", BooleanType(), True),
         StructField("namespace", IntegerType(), True),
         StructField("parsedcomment", StringType(), True),
         StructField("patrolled", BooleanType(), True),
         StructField("revision",
                     StructType(
                         [StructField("new", IntegerType(), True),
                          StructField("old", IntegerType(), True)]), True),
         StructField("server_name", StringType(), True),
         StructField("server_script_path", StringType(), True),
         StructField("server_url", StringType(), True),
         StructField("timestamp", StringType(), True),
         StructField("title", StringType(), True),
         StructField("type", StringType(), True),
         StructField("user", StringType(), True),
         StructField("wiki", StringType(), True)])


def format_stream_dataframe(dataframe):
    # Event data schema
    schema_wiki = create_event_schema()

    # Create dataframe setting schema for event data
    df_wiki = (dataframe
               # Sets schema for event data
               .withColumn("value", from_json("value", schema_wiki))
               )

    # Transform into tabular
    # Select relevant columns
    return (df_wiki.select(
        col("key").alias("event_key")
        , col("topic").alias("event_topic")
        , to_timestamp(from_unixtime(col("value.timestamp"))).alias("change_timestamp")
        , "value.bot"
        , "value.user"
    ))


def process_stream_dataframe(stream):
    # Create dataframe grouping by window
    return (stream
            .withWatermark("change_timestamp",
                           "10 minutes")  # Don't aggregate events arriving more than 10 minutes late
            .groupBy(window(col("change_timestamp"),
                            "10 minutes",
                            "10 minutes"),
                     # 10 minute window, updating every 10 minutes
                     col("user"))
            .count()
            )


def start_query_stream(dataframe, kafka_server, checkpoint_path):
    # Start query stream over stream dataframe
    return (dataframe.select(to_json(struct("*")).alias("value"))
            .selectExpr("CAST(value AS STRING)")
            .writeStream
            .outputMode("complete")
            .format("kafka")
            .option("kafka.bootstrap.servers", kafka_server)  # kafka server
            .option("topic", "wiki-formatted")
            .option("checkpointLocation", checkpoint_path)
            .start())


if __name__ == '__main__':
    kafka_server = "kafka:9092"

    # Spark session
    spark = get_spark_session('wiki-changes-event-consumer',
                              "spark.jars.packages",
                              "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2")

    # Get stream dataframe
    stream_dataframe = get_stream_dataframe_from_kafka_topic(kafka_server,
                                                             "wiki-changes",
                                                             spark)

    # Format stream dataframe
    df_wiki_formatted = format_stream_dataframe(stream_dataframe)
    df_processed = process_stream_dataframe(df_wiki_formatted)

    # Start query stream over stream dataframe
    checkpoint_path = "/home/appuser/data/checkpoint"

    queryStream = start_query_stream(df_processed, kafka_server, checkpoint_path)

    query = (df_processed.select(to_json(struct("*")).alias("value"))
             .selectExpr("CAST(value AS STRING)")
             .writeStream
             .outputMode("complete")
             .format("console")
             .start())

    query.awaitTermination()

    # Stop ingestion
    queryStream.stop()
