import json
import pandas as pd
from io import StringIO
from ydata_profiling import ProfileReport

# Create dataframe from JSON data
f = open('traffic_data_0.json', mode='r')
df = pd.read_json(StringIO(json.load(f)))
f.close()
for i in range(1, 10):  # Add the next nine files
    f = open(f'traffic_data_{i}.json', mode='r')
    df = pd.concat([df, pd.read_json(StringIO(json.load(f)))], axis=0)
    f.close()
# Display the complete data
print("The Dataset looks like:")
print(df)
print(df.shape)
print("====================================")

# Set options to show all columns of the dataset
pd.set_option('display.max_columns', None)
# Display all the columns together in the console
print("Display first 5 rows")
print(df.head(5).to_string())
print("====================================")

# Basic EDA functions
print("Basic Dataframe info")
print(df.info())
print("====================================")
print("More detailed Dataframe info")
print(df.describe().to_string())
print("====================================")
print("Number of Empty values in each column:")
print(df.isnull().sum().sort_values(ascending=False))
print("====================================")
print("Number of Unique values in each column (Exclude Coordinates):")
print(df.drop(columns=["Coordinates"]).apply(pd.Series.nunique))
print("====================================")
print("Are there duplicate rows? Exclude Coordinates")
print(df.drop(columns=["Coordinates"]).duplicated())
print("====================================")

# Detailed investigation
print("Which categories are encountered?")
print(df["Category"].unique())
print("What are the start times?")
print(df["Start Time"].value_counts())

# Automatic profiling
profile = ProfileReport(df, title="Profiling Report")
profile.to_file("profile_report.html")
