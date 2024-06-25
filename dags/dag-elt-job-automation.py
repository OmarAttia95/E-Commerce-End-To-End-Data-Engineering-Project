# All rights reserved to this software and its documentation are reserved under "OMAR H. ATTIA"
# Read the requring documentation before executing this script.
# If you have any questions please feel free to contact me via my LinkedIn account.
# This script mocks the streaming of a big data sources and its dependencies and helps with debugging and testing them properly.

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago, datetime, timedelta

# Define default arguments
default_args = {
    'owner': 'Omar H Attia',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'Ecom_Data_Engineering_Project',
    default_args=default_args,
    description='A DAG for e-commerce data engineering project',
    schedule_interval='@daily',
    start_date=datetime(2023, 6, 24),
    catchup=False,
)

# Task to produce data to Kafka
produce_data_to_kafka = BashOperator(
    task_id='produce_data_to_kafka',
    bash_command='python3 /path/to/your/e-commerce.py',  # Replace with your actual path
    dag=dag,
)

# Task to process data with PySpark using BashOperator
process_data_with_spark = BashOperator(
    task_id='process_data_with_spark',
    bash_command='spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /path/to/your/py-spark-processing.py',  # Replace with your actual path
    dag=dag,
)

# Task to shard data with PySpark using BashOperator
shard_and_aggregate_data_with_spark = BashOperator(
    task_id='shard_and_aggregate_data_with_spark',
    bash_command='spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /path/to/your/pyspark_sharding.py',  # Replace with your actual path
    dag=dag,
)

# Define task dependencies
produce_data_to_kafka >> process_data_with_spark >> shard_and_aggregate_data_with_spark
