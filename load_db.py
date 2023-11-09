from sqlalchemy import create_engine
import numpy as np
from s3fs.core import S3FileSystem


def load_data():
    s3 = S3FileSystem()
    # S3 bucket directory (data warehouse)
    DIR_wh = 's3://ece5984-bucket-mdavies1/Project/data_warehouse'  # Insert here
    # Get data from S3 bucket as a pickle file
    df = np.load(s3.open('{}/{}'.format(DIR_wh, 'clean_traffic_data.pkl')), allow_pickle=True)

    # Create sqlalchemy engine if database does not already exist
    # engine = create_engine("mysql+pymysql://{user}:{pw}@{endpnt}"
    #                        .format(user="admin",
    #                                pw="admin12345",
    #                                endpnt="database-dataeng.cwgvgleixj0c.us-east-1.rds.amazonaws.com"))

    # Create database if it does not already exist
    # engine.execute("CREATE DATABASE {db}"
    #                .format(db="mdavies1"))  # Insert pid here

    # Create sqlalchemy engine
    engine = create_engine("mysql+pymysql://{user}:{pw}@{endpnt}/{db}"
                           .format(user="admin",
                                   pw="admin12345",
                                   endpnt="database-dataeng.cwgvgleixj0c.us-east-1.rds.amazonaws.com",
                                   db="mdavies1"))

    # Insert whole DataFrame into MySQL DB
    df.to_sql('traffic_data',  # table name
                   con=engine, if_exists='replace', chunksize=1000)