# Airflow imports
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

# Other imports
from datetime import datetime
from datetime import timedelta

# Import steps from other files
from ingest import kafka_consumer
from transform import transform_data
from load_db import load_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 9),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'stream_ingest_dag',
    default_args=default_args,
    description='ingest traffic data',
    schedule_interval=timedelta(minutes=1),
)

kafka_cons = PythonOperator(
    task_id='consume_data',
    python_callable=kafka_consumer,
    dag=dag,
)

transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

load_db = PythonOperator(
    task_id='push_to_db',
    python_callable=load_data,
    dag=dag,
)

kafka_cons >> transform >> load_db