import time
import pandas as pd
import requests
import json
from kafka import KafkaProducer
from json import dumps


def kafka_producer():
    producer = KafkaProducer(bootstrap_servers=['54.196.246.52:9104'],  # change ip and port number here
                             value_serializer=lambda x:
                             dumps(x).encode('utf-8'))

    endpoints = ["https://api.coinbase.com/v2/prices/btc-usd/spot", "https://api.coinbase.com/v2/prices/eth-usd/spot",
                 "https://api.coinbase.com/v2/prices/xrp-usd/spot"]

    t_end = time.time() + 60 * 1  # Amount of time data is sent for in seconds
    while time.time() < t_end:
        df_stream = pd.DataFrame(columns=["Coin", "Currency", "Amount", "Time"])
        for endpoint in endpoints:
            quote = json.loads(requests.get(endpoint).text)
            new_row = {
                "Coin": quote.get("data").get("base"),
                "Currency": quote.get("data").get("currency"),
                "Amount": quote.get("data").get("amount"),
                "Time": time.time()
            }
            # print(new_row)
            df_stream = df_stream._append(new_row, ignore_index=True)
            producer.send('CryptoData', value=df_stream.to_json())  # Add topic name here
print("done producing")

kafka_producer()
