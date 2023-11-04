# import s3fs
# from s3fs.core import S3FileSystem
import numpy as np
import pandas as pd
from io import StringIO
import json


def transform_data():
    # s3 = S3FileSystem()
    # S3 bucket directory (data lake)
    # DIR = 's3://ece5984-bucket-mdavies1/Project/data_lake'  # S3 bucket location
    # Get data from S3 bucket
    # raw_data = np.load(s3.open('{}/{}'.format(DIR, 'traffic_data_0.json')))

    # Get data from local file for testing (remove for pipeline)
    # f = open('traffic_data_0.json', mode='r')
    # df = pd.read_json(StringIO(json.load(f)))
    # f.close()
    # for i in range(1, 10):  # Add the next nine files
    #     f = open(f'traffic_data_{i}.json', mode='r')
    #     df = pd.concat([df, pd.read_json(StringIO(json.load(f)))], axis=0)
    #     f.close()

    df_list = []
    for i in range(10):
        with open(f'traffic_data_{i}.json') as file:
            data = pd.read_json(StringIO(json.load(file)))
        df_list.append(data)
    df = pd.concat(df_list, ignore_index=True)

    # Rename columns
    df.rename({"properties.id": "ID", "properties.iconCategory": "Category",
               "properties.magnitudeOfDelay": "Magnitude", "properties.delay": "Delay",
               "properties.startTime": "Start_Time", "properties.endTime": "End_Time",
               "geometry.coordinates": "Coordinates"},
              axis=1, inplace=True)

    # Add informational columns
    df['Category_Name'] = df['Category'].apply(get_category_name)
    df['Magnitude_Name'] = df['Magnitude'].apply(get_magnitude_name)

    print(df.head(5).to_string())
    print(df.info())

    # raw_data = np.load(s3.open('{}/{}'.format(DIR, 'data.pkl')), allow_pickle=True)
    #
    # # Dividing the raw dataset for each company individual company
    # raw_data.columns = raw_data.columns.swaplevel(0, 1)
    # raw_data.sort_index(axis=1, level=0, inplace=True)
    # df_aapl_rw = raw_data['AAPL']
    # df_amzn_rw = raw_data['AMZN']
    # df_googl_rw = raw_data['GOOGL']
    #
    # # Dropping rows with NaN in them
    # df_aapl = df_aapl_rw.dropna()
    # df_amzn = df_amzn_rw.dropna()
    # df_googl = df_googl_rw.dropna()
    #
    # # Removing rows with outliers
    # for col in list(df_aapl.columns)[0:4]:  # We ignore 'Volume' column
    #     df_aapl = df_aapl.drop(df_aapl[df_aapl[col].values > 900].index)  # Values above 900 are dropped
    #     df_aapl = df_aapl.drop(df_aapl[df_aapl[col].values < 0.001].index)  # Values below 0.001 are dropped
    #
    #     df_amzn = df_amzn.drop(df_amzn[df_amzn[col].values > 900].index)
    #     df_amzn = df_amzn.drop(df_amzn[df_amzn[col].values < 0.001].index)
    #
    #     df_googl = df_googl.drop(df_googl[df_googl[col].values > 900].index)
    #     df_googl = df_googl.drop(df_googl[df_googl[col].values < 0.001].index)
    #
    # # Dropping duplicate rows
    # df_aapl = df_aapl.drop_duplicates()
    # df_amzn = df_amzn.drop_duplicates()
    # df_googl = df_googl.drop_duplicates()
    #
    # # Push cleaned data to S3 bucket warehouse
    # DIR_wh = 's3://ece5984-bucket-mdavies1/Lab2'  # Insert here
    # with s3.open('{}/{}'.format(DIR_wh, 'clean_aapl.pkl'), 'wb') as f:
    #     f.write(pickle.dumps(df_aapl))
    # with s3.open('{}/{}'.format(DIR_wh, 'clean_amzn.pkl'), 'wb') as f:
    #     f.write(pickle.dumps(df_amzn))
    # with s3.open('{}/{}'.format(DIR_wh, 'clean_googl.pkl'), 'wb') as f:
    #     f.write(pickle.dumps(df_googl))


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


transform_data()
