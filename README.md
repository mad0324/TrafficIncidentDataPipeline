# Real Time Traffic Incident Visualization

## Project Function
The purpose of this project is to visualize real-time traffic incidents in the Northern Virginia Area.
A frequent problem faced by both individual travelers and navigation applications is the unexpected nature and high-impact effect of traffic incidents. When a traffic incident occurs—be it roadwork, an accident, or a naturally occurring traffic jam—routes and arrival times can be significantly affected. The goal of this project is to solve the problem of identifying and analyzing these incidents by visualizing their location and impact in order to improve navigation planning.

## Dataset
This project uses TomTom traffic data as its dataset. The primary endpoint used is the Incident Details service, an endpoint within the Traffic Incidents service segment of the TomTom Traffic API. Incident Details is limited in geographical extent to 10,000 square kilometers. To remain within this limitation, the visualization will focus on the Fairfax/Arlington/Alexandria area of Northern Virginia and its immediate surroundings.

## Pipeline / Architecture
This project uses the Stream -> Visualization pipeline. The pipeline diagram in the Pipeline Infographic section below displays the tools and data flow used.

_Stream Ingestion:_ Data is ingested from the **TomTom Traffic API** Incident Details endpoint into an **Amazon S3** data lake. Data from the API calls is extracted and loaded using **Apache Kafka**.

_Transformation:_ Data is transformed from its raw state in the data lake to a cleaned version in preparation for analysis using the **pandas** Python library. Clean data is stored in an **Amazon S3** data warehouse in a location separate from the data lake.

_Data Analytics:_ CSV files from the data warehouse are downloaded locally to be visualized using **Tableau**.

_Orchestration:_ The pipeline, from ingestion through transformation, is orchestrated using **Apache Airflow**.

### Pipeline Infographic
![alt text](graphics/FinalPipeline.png)

## Data Quality Assessment
The quality of this dataset is high. The data conforms to expected formats and values for all fields, with no unrealistic outliers. Exploratory data analysis was performed using the included [EDA file](EDA.py). This script will also allow the generation of a data profile report of any JSON files generated during the ingest portion of the pipeline.

## Data Transformation
Because of the high quality of the dataset, transformation of ingested data fields proved unnecessary. The following additions were made during the transformation process in order to facilitate visualization:
- Renamed API fields for readability
- Added columns
  - Category names based on API documentation
  - Magnitude names based on API documentation
  - Delay duration in human readable time format
- Secondary exploded data set with one row for each geometry coordinate to enable map display

## Final Results
The final result of this project is a visualization of real time data collected and processed by the streaming pipeline. Below is the animation of the final dashboard for data generated between 11:46 and 12:47 on Monday, November 27th, 2023, in addition to a still image of the point in time 11/27/23 12:46 PM.

### Dashboard Animation
![alt text](graphics/DashboardAnimation.gif)

### Dashboard Snapshot
![alt Text](graphics/DashboardStill.png)

## Thorough Investigation
This critically assesses the viability of your idea: Based on the results of this project (your pilot project, your prototype, etc), from a technical leadership point of view, what are your conclusions or recommendations for continuing this project in terms of scaling it up? How would you assess the innovativeness of your project? Any technical or platform concerns, difficulties, or limitations of the pipeline for the project? Based on your experience and results, what next step would you recommend to take this project to the next level/phase?

## Code
To replicate this project, download the [repository](https://github.com/mad0324/TrafficIncidentDataPipeline) as a zip file by clicking **Code** above the list of files, then **Download ZIP**. Follow the instructions in the **Special Instruction** and **Replicating this Project** sections below.

## Special Instructions
In order to access the dataset, a TomTom developer API key is required. This key can be generated for free, with instruction on the [TomTom developer site](https://developer.tomtom.com/traffic-api/documentation/product-information/introduction#getting-started). From the TomTom site:
1. To get your API Key, you first need to be registered for the TomTom Developer Portal. If you don't have an account, no worries! Register / Sign in now before continuing.
1. Once you are registered, go to your Dashboard and locate the API Key that you would like to use (we've created the first one for you, it's called My First API Key).

The ["My First API Key"](https://developer.tomtom.com/user/me/apps) generated by these steps is sufficient to run this project.

## Replicating this Project


