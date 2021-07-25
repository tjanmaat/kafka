CREATE STREAM users_counts (
    user_name VARCHAR ,
    user_count INT
) WITH (
    kafka_topic = 'wiki-formatted',
    partitions = 3,
    value_format = 'json'
);