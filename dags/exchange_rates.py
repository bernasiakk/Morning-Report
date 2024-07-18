# Imports
from datetime import date, datetime, timedelta
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

# Functions
def extract_exchange_rates(task_instance):
    # Specify API parameters & query the API
    today = date.today()
    prev_day = today - timedelta(days=1)

    while True:
        prev_day_date = prev_day.strftime("%Y-%m-%d")  # Format date as YYYY-MM-DD

        parameters = {'table': 'a',
                    'currency': 'eur',
                    'date': prev_day_date}

        # Build the complete URL with parameters
        url = f"https://api.nbp.pl/api/exchangerates/rates/{parameters['table']}/{parameters['currency']}/{parameters['date']}/?format=json"
        result = requests.get(url)
        logging.info(f"url: {url}, result:{result}")

        # Check for successful response (200)
        if result.status_code == 200:
            # Get the data
            json_data = result.json()
            logging.info("Response from API: {}".format(json_data))

            # Pass variables to XCom.
            # You'll use them in the next task.
            current_datetime = str(datetime.now().strftime("%m-%d-%Y-%H-%M-%S"))
            file_name = raw_file_prefix + "_" + current_datetime + '.json'
            
            task_instance.xcom_push(key='current_datetime', value=current_datetime)
            task_instance.xcom_push(key='file_name', value=file_name)
            
            # Save output file in your local ./dags/output/ directory
            dir_path = os.path.join(os.path.dirname(__file__),
                                    'output',
                                    folder)
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)
            tot_name = os.path.join(dir_path, file_name)
            logging.info(f"Will write output to: {tot_name}")

            with open(tot_name, 'w') as outputfile:
                json.dump(json_data, outputfile)
                logging.info("Successfully wrote local output file")

            # upload to GCS
            gcs_dest_object = os.path.join(folder, file_name)
            logging.info(f"Will write output to GCS: {gcs_dest_object}")
            
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(gcs_dest_object)
            blob.upload_from_filename(tot_name)
            
            logging.info(f"Successfully wrote output file to GCS: gs://{bucket}/{gcs_dest_object}")
            break  # Exit the loop on success

        # Decrement date for retry on unsuccessful response
        prev_day -= timedelta(days=1)
        # task_instance.xcom_push(key='prev_day', value=prev_day)
        
        # Check if reached a date before today (avoid infinite loop)
        if prev_day < (today - timedelta(days=7)):  # Adjust year/month as needed
            logging.info("No data available for the past week. Exiting.")
            break

def transform_exchange_rates(task_instance): # get only the relevant data into the ./processed/ folder
    # Pull XCom variables from previous task
    current_datetime = task_instance.xcom_pull(task_ids='extract_exchange_rates', key='current_datetime')
    file_name = task_instance.xcom_pull(task_ids='extract_exchange_rates', key='file_name')
    # prev_day = task_instance.xcom_pull(task_ids='extract_exchange_rates', key='prev_day')

    # Download from GCP
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(os.path.join(folder, file_name))
    content = blob.download_as_string().decode('utf-8').strip()
    file_object = StringIO(content)
    doc = json.load(file_object)    
    
    # Extract only the relevant values from the GCP file & store them in a JSON object
    currency = doc['currency']
    code = doc['code']
    mid = doc['rates'][0]['mid']
    effective_date = doc['rates'][0]['effectiveDate']
    
    json_data = [{"id": str(uuid.uuid4()),
        "currency": currency,
        "code": code,
        "mid": mid,
        "effective_date": effective_date
        }]
    
    # Load to BigQuery table
    bigquery_client = bigquery.Client()

    table_id = f"{project_id}.{dataset}.{table}"
    
    logging.info(f"Validating that no record exists for today's date already...")
    
    current_date = str(datetime.now().strftime("%Y-%m-%d"))
    
    query = f"""
    SELECT * 
    FROM {table_id}
    WHERE date = '{current_date}';
    """
    rows = bigquery_client.query_and_wait(query)
    
    # Load to GCS Bucket
    storage_client = storage.Client()
    
    # Convert the data to bytes
    data_bytes = json.dumps(json_data).encode("utf-8")

    # Create a bucket object
    bucket_obj = storage_client.bucket(bucket_name)

    if rows.total_rows == 0:

        errors = bigquery_client.insert_rows_json(table_id, json_data)  # Make an API request.
        
        # If no errors, dump the file to GCS "./processed/" folder
        if errors == []:
            logging.info(f"New rows have been added to table: {table_id}")
            
            logging.info(f"Writing json file to ./processed folder...")
            dest_file_name = processed_file_prefix + "_" + current_datetime + '.json'
            
            # Construct the full object path with folder
            processed_file_path = os.path.join(folder, processed_prefix, dest_file_name)

            # Create a blob object
            blob = bucket_obj.blob(processed_file_path)

            # Upload the data to the blob
            blob.upload_from_string(data_bytes)

            logging.info(f"Data uploaded to gs://{bucket_name}/{processed_file_path}")
            
            # Move raw file to ./archived/ directory & delete from ./ directory
            logging.info(f"Moving file '{file_name}' to 'gs://./archived/' directory")
            
            source_bucket = storage_client.bucket(bucket_name)
            source_blob = bucket.blob(os.path.join(folder, file_name))
            destination_bucket = source_bucket
            destination_blob_name = os.path.join(folder, archive_prefix, file_name)

            blob_copy = source_bucket.copy_blob(
                source_blob, destination_bucket, destination_blob_name
            )
            
            source_blob.delete()

            logging.info(f"File moved to: {blob_copy.name}")
            
        else:
            logging.info("Encountered errors while inserting rows: {}".format(errors))
    
    else:
        logging.info(f"ERROR: Table already has a record with date {current_date}. No record inserted into BigQuery.")
        
        logging.info(f"Moving file to ./{not_processed_prefix}")
        
        not_processed_file_path = os.path.join(folder, not_processed_prefix, file_name)
        
        # Create a blob object
        blob = bucket_obj.blob(not_processed_file_path)

        # Upload the data to the blob
        blob.upload_from_string(data_bytes)

        source_blob = bucket.blob(os.path.join(folder, file_name))

        source_blob.delete()

        logging.info(f"File moved to gs://{bucket_name}/{not_processed_file_path}")

# DAG setup
default_args = {
    'owner': 'Szymon',
    'start_date': datetime(2024, 7, 5),
    'retries': 0,
    # 'retry_delay': timedelta(seconds=60)
}

with models.DAG('exchange_rates',
                default_args=default_args,
                schedule_interval=Variable.get('cron_interval'),
                catchup=False,
                max_active_runs=1) as dag:

    # Set variables
    project_id = Variable.get('project_id')
    region = Variable.get('region')
    bucket_name = Variable.get('bucket_name')
    dataset = Variable.get('dataset')
    archive_prefix = Variable.get('archive_prefix')
    processed_prefix = Variable.get('processed_prefix')
    not_processed_prefix = Variable.get('not_processed_prefix')

    os.environ.setdefault("GCLOUD_PROJECT", project_id)

    dag_name = dag.dag_id
    logging.info(f"dag name: {dag_name}")
    folder = dag_name
    folder_path = os.path.join(bucket_name, folder)
    raw_file_prefix = f"raw_{dag_name}"
    processed_file_prefix = f"processed_{dag_name}"
    table = dag_name

    # Set DAGs
    extract_exchange_rates = PythonOperator(
        task_id='extract_exchange_rates',
        python_callable=extract_exchange_rates,
    )

    transform_exchange_rates = PythonOperator(
        task_id='transform_exchange_rates',
        python_callable=transform_exchange_rates,
    )

    # Pipe DAGs
    extract_exchange_rates >> transform_exchange_rates
