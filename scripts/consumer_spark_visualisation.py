
from pyspark.sql.functions import window, col, current_timestamp, to_json, struct
from common import get_stream_dataframe_from_kafka_topic, get_spark_session

from time import sleep
import seaborn as sns
import matplotlib
import matplotlib.pyplot as plt


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


def create_querystream(dataframe):
    # Create query stream with memory sink
    return (dataframe
            .writeStream
            .format("memory")
            .queryName("wiki_changes")
            .outputMode("update")
            .start())


def visualize_stream(queryStream, spark):

    matplotlib.rc('font', family='DejaVu Sans')
    sns.set(style="whitegrid")

    print("**********************")
    print("General Info")
    print("**********************")
    print("Run:{}".format(i))
    if len(queryStream.recentProgress) > 0:
        print("Stream timestamp:{}".format(queryStream.lastProgress["timestamp"]))
        print("Watermark:{}".format(queryStream.lastProgress["eventTime"]["watermark"]))
        print("Total Rows:{}".format(queryStream.lastProgress["stateOperators"][0]["numRowsTotal"]))
        print("Updated Rows:{}".format(queryStream.lastProgress["stateOperators"][0]["numRowsUpdated"]))
        print("Memory used MB:{}".format(
            (queryStream.lastProgress["stateOperators"][0]["memoryUsedBytes"]) * 0.000001))

    df = spark.sql(
        """
            select
                window.start
                ,window.end
                ,user
                ,sum(count) count
            from
                wiki_changes
            where
                window.start = (select max(window.start) from wiki_changes)
            group by
                window.start
                ,window.end
                ,user
            order by
                4 desc
            limit 10
        """
    ).toPandas()

    # Plot the total crashes
    sns.set_color_codes("muted")

    # Initialize the matplotlib figure
    plt.figure(figsize=(8, 6))

    print("**********************")
    print("Graph - Top 10 users")
    print("**********************")
    try:
        # Barplot
        sns.barplot(x="count", y="user", data=df)

        # Show barplot
        plt.show()
    except ValueError:
        # If Dataframe is empty, pass
        pass

    print("**********************")
    print("Table - Top 10 users")
    print("**********************")
    print(df)

    print("**********************")
    print("Table - Count by aggregation window")
    print("**********************")
    df1 = spark.sql(
        """
            select
                window.start
                ,window.end
                ,sum(count) qty_lines
                ,count(distinct user) qty_users
            from
                wiki_changes
            group by
                window.start
                ,window.end
            order by
                window.start desc
        """
    ).toPandas()

    print(df1)


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

    query = (stream_dataframe.select(to_json(struct("*")).alias("value"))
             .selectExpr("CAST(value AS STRING)")
             .writeStream
             .format("console")
             .start())

    query.awaitTermination()

    # # Create query stream with memory sink
    # queryStream = create_querystream(stream_dataframe)
    #
    # try:
    #     i = 1
    #     while True:
    #         visualize_stream(queryStream, spark)
    #
    #         sleep(10)
    #         i = i + 1
    # except KeyboardInterrupt:
    #     print("process interrupted.")
