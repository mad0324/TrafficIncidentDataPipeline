import time
from kafka import KafkaConsumer
from json import loads
import json
from s3fs import S3FileSystem


def kafka_consumer():
    s3 = S3FileSystem()
    DIR = "s3://ece5984-bucket-mdavies1/Project/data_lake"  # S3 bucket location

    interval = 60       # Amount of time between calls
    call_count = 60     # Number of api calls
    t_end = time.time() + interval * call_count  # Consumptions end time

    consumer = KafkaConsumer(
        'TrafficIncidents',  # Topic name
        bootstrap_servers=['3.235.223.243:9092'],  # IP and port number
        consumer_timeout_ms=100000,
        value_deserializer=lambda x: loads(x.decode('utf-8')))

    while time.time() < t_end:
        for count, i in enumerate(consumer):
            with s3.open("{}/traffic_data_{}.json".format(DIR, count),
                         'w') as file:
                json.dump(i.value, file)

    consumer.close()

    print("done consuming")
