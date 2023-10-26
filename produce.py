# import time
import pandas as pd
import requests
import json


# from kafka import KafkaProducer
# from json import dumps


def kafka_producer():
    # producer = KafkaProducer(bootstrap_servers=['<IP>:<Port>'],  # change ip and port number here
    #                          value_serializer=lambda x:
    #                          dumps(x).encode('utf-8'))

    # Set request parameters
    base_url = "https://api.tomtom.com/traffic/services/5/incidentDetails?"
    min_lon = "-77.539145"
    min_lat = "39.0585509"
    max_lon = "-77.031028"
    max_lat = "38.614582"
    bounding_box = min_lon + "," + min_lat + "," + max_lon + "," + max_lat
    key = "iwWqJ7QRNYIgDUGtpkiyuPzrdnYcX3E6"
    fields = ("{incidents{geometry{type,coordinates},properties{"
              "id,iconCategory,magnitudeOfDelay,startTime,endTime,delay}}}")
    language = "en-US"
    category_filter = "0,1,2,3,4,5,6,7,8,9,10,11,14"
    time_validity_filter = "present"

    parameters = {'bbox': bounding_box, 'fields': fields, 'key': key, 'language': language,
                  'categoryFilter': category_filter, 'timeValidityFilter': time_validity_filter}

    df_stream = pd.DataFrame(columns=["ID", "Category", "Magnitude", "Delay", "Start Time",
                                      "End Time", "GeoType", "Coordinates", "Retrieve Time"])
    response = requests.get(base_url, params=parameters)
    snapshot = response.json()
    df_stream = pd.json_normalize(snapshot, 'incidents')
    df_stream.rename({"properties.id": "ID", "properties.iconCategory": "Category",
                      "properties.magnitudeOfDelay": "Magnitude", "properties.delay": "Delay",
                      "properties.startTime": "Start Time", "properties.endTime": "End Time",
                      "geometry.type": "GeoType", "geometry.coordinates": "Coordinates"},
                     axis=1, inplace=True)
    df_stream['Retrieve Time'] = pd.Timestamp.today().strftime('%Y-%m-%dT%H:%M:%SZ')
    # pd.set_option('display.max_columns', None)
    print(df_stream)

    # endpoints = ["https://api.coinbase.com/v2/prices/btc-usd/spot", "https://api.coinbase.com/v2/prices/eth-usd/spot",
    #              "https://api.coinbase.com/v2/prices/xrp-usd/spot"]
    # t_end = time.time() + 60 * 1  # Amount of time data is sent for in seconds
    # while time.time() < t_end:
    #     df_stream = pd.DataFrame(columns=["Coin", "Currency", "Amount", "Time"])
    #     for endpoint in endpoints:
    #         quote = json.loads(requests.get(endpoint).text)
    #         new_row = {
    #             "Coin": quote.get("data").get("base"),
    #             "Currency": quote.get("data").get("currency"),
    #             "Amount": quote.get("data").get("amount"),
    #             "Time": time.time()
    #         }
    #         # print(new_row)
    #         df_stream = df_stream._append(new_row, ignore_index=True)
    #         producer.send('CryptoData', value=df_stream.to_json())  # Add topic name here


print("done producing")

kafka_producer()
