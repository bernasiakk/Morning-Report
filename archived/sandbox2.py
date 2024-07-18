import json

from google.cloud import storage

import logging
import datetime
import requests



def extract_weather(bucket_name, subfolder):
    parameters = {'units': 'metric',
                'q': 'warszawa',
                'appid': '***'} # TO DO (encrypt it somehow)

    result = requests.get(
        "http://api.openweathermap.org/data/2.5/weather?", parameters)

    if result.status_code == 200:

        json_data = result.json()
        
        logging.info(f"Response from API: {json_data}")
        
        file_name = __name__ + "_" + str(datetime.datetime.now().strftime("%m-%d-%Y-%H-%M-%S")) + '.json'
    

    # UPLOAD TO GCS
    # Create a storage client
    storage_client = storage.Client()

    # Convert the data to bytes
    data_bytes = json.dumps(json_data).encode("utf-8")

    # Create a bucket object
    bucket = storage_client.bucket(bucket_name)
    
    # Construct the full object path with subfolder
    full_path = f"{subfolder}/{file_name}"

    # Create a blob object
    blob = bucket.blob(full_path)

    # Upload the data to the blob
    blob.upload_from_string(data_bytes)

    logging.info(f"Data uploaded to gs://{bucket_name}/{full_path}")
