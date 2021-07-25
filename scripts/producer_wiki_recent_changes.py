import json

from sseclient import SSEClient as EventSource
from kafka import KafkaProducer


def get_wiki_recent_changes(producer):
    url = 'https://stream.wikimedia.org/v2/stream/recentchange'
    try:
        for event in EventSource(url):
            if event.event == 'message':
                try:
                    change = json.loads(event.data)
                except ValueError:
                    pass
                else:
                    # Send msg to topic wiki-changes
                    producer.send('wiki-changes', change)
    except KeyboardInterrupt:
        print("process interrupted")


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['kafka:9092'],
                                  api_version=(0, 10),
                                  value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer


if __name__ == '__main__':
    kafka_producer = connect_kafka_producer()
    get_wiki_recent_changes(kafka_producer)
