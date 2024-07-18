# Imports
from datetime import datetime, timedelta
import requests
import json
import os
from google.cloud import storage
from google.cloud import bigquery
import logging
from airflow import models
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
import uuid
from bs4 import BeautifulSoup


# Functions
def extract_history_facts(task_instance):
    # Specify API parameters & query the API
    parameters = {'month': str(datetime.now().month),
                'day': str(datetime.now().day),
                }
    
    # Build the complete URL with parameters
    url = f"http://numbersapi.com/{parameters['month']}/{parameters['day']}/date"
    result = requests.get(url)
    logging.info(f"result: {result}")

    # If the API call was sucessful, get the data and dump it to a file (first local, then GCP)
    if result.status_code == 200:

        # Get the data
        data = str(BeautifulSoup(result.content, 'html.parser'))
        logging.info(f"data: {data}")

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
            outputfile.write(data) # slight change vs. the JSON parsers
            logging.info("Successfully wrote local output file")
        
        # upload to GCS
        gcs_dest_object = os.path.join(folder, file_name)
        logging.info(f"Will write output to GCS: {gcs_dest_object}")
        
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(gcs_dest_object)
        blob.upload_from_filename(tot_name)
        
        logging.info(f"Successfully wrote output file to GCS: gs://{bucket}/{gcs_dest_object}")
    
    else:
        raise ValueError('"Error In API call."')

def transform_history_facts(task_instance): # get only the relevant data into the ./processed/ folder
    # Pull XCom variables from previous task
    current_datetime = task_instance.xcom_pull(task_ids='extract_history_facts', key='current_datetime')
    file_name = task_instance.xcom_pull(task_ids='extract_history_facts', key='file_name')

    # Download from GCP
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(os.path.join(folder, file_name))
    content = blob.download_as_string().decode('utf-8').strip()
    
    # Extract only the relevant values from the GCP file & store them in a JSON object
    json_data = [{"id": str(uuid.uuid4()),
        "date_fact": content
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

with models.DAG('history_facts',
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
    extract_history_facts = PythonOperator(
        task_id='extract_history_facts',
        python_callable=extract_history_facts,
    )

    transform_history_facts = PythonOperator(
        task_id='transform_history_facts',
        python_callable=transform_history_facts,
    )
    
    # Pipe DAGs
    extract_history_facts >> transform_history_facts
