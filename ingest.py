import time
from kafka import KafkaConsumer
from json import loads
import json
from s3fs import S3FileSystem


def kafka_consumer():
    s3 = S3FileSystem()
    DIR = "s3://ece5984-bucket-mdavies1/Project/data_lake"  # Add S3 bucket location
    t_end = time.time() + 60 * 10  # Amount of time data is sent for UPDATE WITH produce.py
    while time.time() < t_end:
        consumer = KafkaConsumer(
            'TrafficIncidents',  # Topic name
            bootstrap_servers=['3.235.223.243:9120'],  # IP and port number
            value_deserializer=lambda x: loads(x.decode('utf-8')))

        for count, i in enumerate(consumer):
            with s3.open("{}/traffic_data_{}.json".format(DIR, count),
                         'w') as file:
                json.dump(i.value, file)
    print("done consuming")
