# import s3fs
from s3fs.core import S3FileSystem
import numpy as np
import pandas as pd
import pickle
from io import StringIO
import json


def transform_data():
    # Get data from S3 bucket in a single dataframe (pipeline)
    s3 = S3FileSystem()
    # S3 bucket directory (data lake)
    DIR_lk = 's3://ece5984-bucket-mdavies1/Project/data_lake'  # S3 bucket location
    df_list = []
    for i in range(10):     # Number of files matching pattern 'traffic_data_{#}.json' to transform
        with s3.open('{}/{}'.format(DIR_lk, f'traffic_data_{i}.json')) as file:
            data = pd.read_json(StringIO(json.load(file)))
        df_list.append(data)
    df = pd.concat(df_list, ignore_index=True)

    # # Get data from files in a single dataframe (local)
    # df_list = []
    # for i in range(10):     # Number of files matching pattern 'traffic_data_{#}.json' to transform
    #     with open(f'traffic_data_{i}.json') as file:
    #         data = pd.read_json(StringIO(json.load(file)))
    #     df_list.append(data)
    # df = pd.concat(df_list, ignore_index=True)

    # Rename columns
    df.rename({"properties.id": "ID", "properties.iconCategory": "Category",
               "properties.magnitudeOfDelay": "Magnitude", "properties.delay": "Delay",
               "properties.startTime": "Start_Time", "properties.endTime": "End_Time",
               "geometry.coordinates": "Coordinates"},
              axis=1, inplace=True)

    # Add informational columns
    df['Category_Name'] = df['Category'].apply(get_category_name)
    df['Magnitude_Name'] = df['Magnitude'].apply(get_magnitude_name)

    # Convert coordinates to JSON to enable sqlalchemy to load into RDB
    df['Coordinates'] = df['Coordinates'].apply(json.dumps)

    print(df.head(5).to_string())
    print(df.info())

    # Push transformed data to S3 bucket warehouse
    DIR_wh = 's3://ece5984-bucket-mdavies1/Project/data_warehouse'  # Insert here
    with s3.open('{}/{}'.format(DIR_wh, 'clean_traffic_data.pkl'), 'wb') as f:
        f.write(pickle.dumps(df))


def get_category_name(category):
    if category == 1:
        return 'Accident'
    elif category == 2:
        return 'Fog'
    elif category == 3:
        return 'Dangerous Conditions'
    elif category == 4:
        return 'Rain'
    elif category == 5:
        return 'Ice'
    elif category == 6:
        return 'Jam'
    elif category == 7:
        return 'Lane Closed'
    elif category == 8:
        return 'Road Closed'
    elif category == 9:
        return 'Road Works'
    elif category == 10:
        return 'Wind'
    elif category == 11:
        return 'Flooding'
    elif category == 14:
        return 'Broken Down Vehicle'
    else:
        return 'Unknown'


def get_magnitude_name(magnitude):
    if magnitude == 1:
        return 'Minor'
    elif magnitude == 2:
        return 'Moderate'
    elif magnitude == 3:
        return 'Major'
    elif magnitude == 4:
        return 'Undefined'
    else:
        return 'Unknown'


# transform_data()
