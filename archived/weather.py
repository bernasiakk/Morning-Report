# Imports
from datetime import datetime, timedelta
import requests
import json
import os
import json
from google.cloud import storage
from google.cloud import bigquery
import logging
from airflow import models
from datetime import datetime, timedelta
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.operators.python_operator import PythonOperator

# General variables
project_id = "morning-report-428716"
region = "europe-west3"
bucket = "morning-report"
dataset = "morning-report"
archive_prefix = "archived"

# DAG-specific variables: General
weather_folder = "weather"
weather_path = os.path.join(bucket, weather_folder)
raw_weather_file_prefix = "raw_weather_"
current_datetime = str(datetime.datetime.now().strftime("%m-%d-%Y-%H-%M-%S"))

# DAG-specific variables: BigQuery
table = "weather"

# Functions
def extract_weather():
    parameters = {'units': 'metric',
                'q': 'warszawa',
                'appid': '82e5d998828557d3a1b9fb1cd7bcd559'} # TODO store it as openweather_api_key

    result = requests.get(
        "http://api.openweathermap.org/data/2.5/weather?", parameters)

    if result.status_code == 200:

        json_data = result.json()
        
        logging.info(f"Response from API: {json_data}")
        
        raw_file_name = raw_weather_file_prefix + "_" + current_datetime + '.json'
    

    # UPLOAD TO GCS
    # Create a storage client
    storage_client = storage.Client.from_service_account_json('morning-report.json')

    # Convert the data to bytes
    data_bytes = json.dumps(json_data).encode("utf-8")

    # Create a bucket object
    bucket_obj = storage_client.bucket(bucket)
    
    # Construct the full object path with weather_folder
    source_file_path = f"{weather_folder}/{raw_file_name}"

    # Create a blob object
    blob = bucket_obj.blob(source_file_path)

    # Upload the data to the blob
    blob.upload_from_string(data_bytes)

    logging.info(f"Data uploaded to gs://{bucket}/{source_file_path}")    

def transform_weather(processed_prefix='processed'): # get only the relevant data into the ./processed/ folder
    gcs_hook = GoogleCloudStorageHook()
    files = gcs_hook.list(bucket, prefix=os.path.join(weather_folder, raw_weather_file_prefix))
    source_object = files[0]
    raw_file_name = source_object.split('/')[-1]  #Get the file name (potential TO DO)
    
    with open(source_object, 'r') as inputfile:
        doc = json.load(inputfile)
        logging.info("Read from {}".format(source_object))
        logging.info("Content: {}".format(doc))
    
    city = str(doc['name'])
    main = str(doc['weather'][0]['main'])
    description = str(doc['weather'][0]['description'])
    temp = float(doc['main']['temp'])
    feels_like = float(doc['main']['feels_like'])
    sunrise = datetime.datetime.fromtimestamp(int(doc['sys']['sunrise']) + int(doc['timezone']))
    sunset = datetime.datetime.fromtimestamp(int(doc['sys']['sunset']) + int(doc['timezone']))
    
    
    # to JSON
    json_data = {
        "city": city,
        "main": main,
        "description": description,
        "temp": temp,
        "feels_like": feels_like,
        "sunrise": sunrise,
        "sunset": sunset
    }
    
    # Construct a BigQuery client object. https://cloud.google.com/bigquery/docs/samples/bigquery-query-natality-tutorial
    bigquery_client = bigquery.Client()

    # Set table_id to the ID of table to append to.
    table_id = f"{project_id}.{dataset}.{table}"

    errors = bigquery_client.insert_rows_json(table_id, json_data)  # Make an API request.
    if errors == []:
        logging.info(f"New rows have been added to table: {table_id}")
        
        logging.info(f"Writing json file to ./processed folder...")
        dest_file_name = __name__ + "_" + current_datetime + '.json'
        
        # Load to GCS Bucket
        storage_client = storage.Client()

        # Convert the data to bytes
        data_bytes = json.dumps(json_data).encode("utf-8")

        # Create a bucket object
        bucket_obj = storage_client.bucket(bucket)
        
        # Construct the full object path with weather_folder
        source_file_path = os.path.join(weather_folder, processed_prefix, dest_file_name)

        # Create a blob object
        blob = bucket_obj.blob(source_file_path)

        # Upload the data to the blob
        blob.upload_from_string(data_bytes)

        logging.info(f"Data uploaded to gs://{bucket}/{source_file_path}")
    else:
        logging.info("Encountered errors while inserting rows: {}".format(errors))
    
    #Move the file to the 'archived' subdirectory
    if files:
        archived_object = os.path.join(weather_folder, archive_prefix, raw_file_name)
        gcs_hook.copy(bucket, source_object, bucket, archived_object)
        gcs_hook.delete(bucket, source_object)
        logging.info("archived file")
    else:
        return None

    

# DAG setup
default_args = {
    'owner': 'Szymon',
    'start_date': datetime(2024, 7, 5),
    'retries': 2,
    'retry_delay': timedelta(seconds=60),
    'dataflow_default_options': {
        'project': project_id,  # TO DO
        'region': region, # TO DO
        'runner': 'DataflowRunner' # TO DO
    }
}

with models.DAG('weather',
                default_args=default_args,
                schedule_interval=timedelta(hours=24),
                catchup=False,
                max_active_runs=1) as dag:

    extract_weather = PythonOperator(
        task_id='extract_weather',
        python_callable=extract_weather(),
    )

    transform_weather = PythonOperator(
        task_id='transform_weather',
        python_callable=transform_weather,
    )

    extract_weather >> transform_weather
