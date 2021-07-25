from pyspark.sql.types import StringType, StructType, StructField, IntegerType
from pyspark.sql.functions import window, col, current_timestamp, to_json, struct, from_json, to_timestamp, \
    from_unixtime
from common import get_stream_dataframe_from_kafka_topic, get_spark_session

from ksql import KSQLAPI


def create_event_schema():
    return StructType(
        [StructField("count", IntegerType(), True),
         StructField("user", StringType(), True)])


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
    return (df_wiki.select("value.count"
                           , "value.user"))


def create_querystream(dataframe):
    # Create query stream with memory sink
    return (dataframe
            .writeStream
            .format("memory")
            .queryName("wiki_changes")
            .outputMode("update")
            .start())


if __name__ == '__main__':
    kafka_server = "kafka:9092"

    # Spark session
    spark = get_spark_session('wiki-changes-dataviz',
                              "spark.jars.packages",
                              "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2")

    # Get stream dataframe
    stream_dataframe = get_stream_dataframe_from_kafka_topic(kafka_server,
                                                             "wiki-formatted",
                                                             spark)
    # Format stream dataframe
    df_formatted = format_stream_dataframe(stream_dataframe)

    client = KSQLAPI('http://0.0.0.0:8088')

    client.ksql('show topics')

    query = (df_formatted
             .writeStream
             .format("console")
             .start())

    query.awaitTermination()
