import time
from kafka import KafkaConsumer
from json import loads
import json
from s3fs import S3FileSystem

def kafka_consumer():
    s3 = S3FileSystem()
    DIR = "s3://ece5984-bucket-mdavies1/Homework1/stream"            # Add S3 bucket location
    t_end = time.time() + 60 * 1  # Amount of time data is sent for
    while time.time() < t_end:
        consumer = KafkaConsumer(
            'CryptoData',  # add Topic name here
            bootstrap_servers=['54.196.246.52:9104'],  # add your IP and port number here
            value_deserializer=lambda x: loads(x.decode('utf-8')))

        for count, i in enumerate(consumer):
            with s3.open("{}/crypto_data_{}.json".format(DIR, count),
                         'w') as file:
                json.dump(i.value, file)
    print("done consuming")