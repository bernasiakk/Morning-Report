# Imports
from datetime import date, datetime, timedelta
from airflow.utils.dates import days_ago
import requests
import json
import os
from google.cloud import storage
from google.cloud import bigquery
import logging
from airflow import models
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from io import StringIO
import uuid
import pandas as pd
from airflow.sensors.external_task_sensor import ExternalTaskSensor


# Functions
def consolidate_data(task_instance): # query the data & load to json
    current_date = str(datetime.now().strftime("%Y-%m-%d"))
    
    table_id = f"{project_id}.{dataset}.{table}"

    bigquery_client = bigquery.Client()
        
    query = f"""
    SELECT weather.date as current_date,
        weather.temp, 
        weather.feels_like,
        weather.sunrise,
        weather.sunset,
        exchange_rates.mid as eur_to_pln,
        history_facts.date_fact,
        CONCAT(daily_word.word, ': ', daily_word.description) as daily_word,
        bored_activity.activity
    FROM morning_report.weather
    LEFT JOIN morning_report.exchange_rates
        ON weather.date = exchange_rates.date
    LEFT JOIN morning_report.history_facts
        ON weather.date = history_facts.date       
    LEFT JOIN morning_report.daily_word
        ON weather.date = daily_word.date    
    LEFT JOIN morning_report.bored_activity
        ON weather.date = bored_activity.date    
    WHERE 
    CAST(weather.date as DATE) = CURRENT_DATE();
    """
    
    data = []
    
    query_job = bigquery_client.query(query)
    
    rows = query_job.result()

    for row in rows:
        data.append({"current_date": row[0].strftime('%Y-%m-%d'), 
                     "temp": float(row[1]),
                     "feels_like": float(row[2]),
                     "sunrise": row[3].strftime('%H-%M-%S'),
                     "sunset": row[4].strftime('%H-%M-%S'),
                     "eur_to_pln": float(row[5]),
                     "date_fact": row[6],
                     "daily_word": row[7],
                     "activity": row[8]
                })
        
    # write data to JSON
    # Load to GCS Bucket
    storage_client = storage.Client()
    
    # Convert the data to bytes
    data_bytes = json.dumps(data).encode("utf-8")

    # Create a bucket object
    bucket_obj = storage_client.bucket(report_bucket_name)
    
    logging.info(f"Writing json file to ~/DAILY_REPORT folder...")
    
    dest_file_name = morning_report_prefix + "_" + current_date + '.json'
    
    # Create a blob object
    blob = bucket_obj.blob(dest_file_name)

    # Upload the data to the blob
    blob.upload_from_string(data_bytes)

    logging.info(f"Data uploaded to gs://{report_bucket_name}/{dest_file_name}")
    
    
    
# DAG setup
default_args = {
    'owner': 'Szymon',
    'start_date': days_ago(1),
    'retries': 0,
    # 'retry_delay': timedelta(seconds=60)
}

with models.DAG('consolidate_data',
                default_args=default_args,
                schedule_interval=Variable.get('cron_interval'),
                catchup=False,
                max_active_runs=1) as dag:

    # Set variables
    project_id = Variable.get('project_id')
    region = Variable.get('region')
    bucket_name = Variable.get('bucket_name')
    report_bucket_name = Variable.get('report_bucket_name')
    dataset = Variable.get('dataset')
    archive_prefix = Variable.get('archive_prefix')
    processed_prefix = Variable.get('processed_prefix')
    not_processed_prefix = Variable.get('not_processed_prefix')
    morning_report_prefix = Variable.get('morning_report_prefix')
    

    os.environ.setdefault("GCLOUD_PROJECT", project_id)

    dag_name = 'exchange_rates'
    logging.info(f"dag name: {dag_name}")
    folder = dag_name
    folder_path = os.path.join(bucket_name, folder)
    raw_file_prefix = f"raw_{dag_name}"
    processed_file_prefix = f"processed_{dag_name}"
    table = dag_name

    # Set DAGs
    # Wait for successful completion of tasks in dependent DAGs
    wait_for_weather = ExternalTaskSensor(
        task_id="wait_for_weather",
        external_dag_id="weather",
        external_task_id="transform_weather",  # Replace with actual task ID
        allowed_states=["success"]
    )
    
    wait_for_exchange_rates = ExternalTaskSensor(
        task_id="wait_for_exchange_rates",
        external_dag_id="exchange_rates",
        external_task_id="transform_exchange_rates",  # Replace with actual task ID
        allowed_states=["success"]
    )
    
    wait_for_history_facts = ExternalTaskSensor(
        task_id="wait_for_history_facts",
        external_dag_id="history_facts",
        external_task_id="transform_history_facts",  # Replace with actual task ID
        allowed_states=["success"]
    )
    
    wait_for_daily_word = ExternalTaskSensor(
        task_id="wait_for_daily_word",
        external_dag_id="daily_word",
        external_task_id="transform_daily_word",  # Replace with actual task ID
        allowed_states=["success"]
    )
    
    wait_for_bored_activity = ExternalTaskSensor(
        task_id="wait_for_bored_activity",
        external_dag_id="bored_activity",
        external_task_id="transform_bored_activity",  # Replace with actual task ID
        allowed_states=["success"]
    )
    
    # Define your tasks for loading data to JSON (dependent on previous tasks)
    consolidate_data = PythonOperator(
        task_id='consolidate_data',
        python_callable=consolidate_data,
    )

# Set up dependencies
wait_for_weather >> consolidate_data
wait_for_exchange_rates >> consolidate_data
wait_for_history_facts >> consolidate_data
wait_for_daily_word >> consolidate_data
wait_for_bored_activity >> consolidate_data