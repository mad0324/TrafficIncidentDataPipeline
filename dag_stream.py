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
    'traffic_stream_dag',
    default_args=default_args,
    description='process traffic data',
    schedule_interval=timedelta(days=1),
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


kafka_cons >> transform