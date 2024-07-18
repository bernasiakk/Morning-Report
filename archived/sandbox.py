from google.cloud import storage


def extract_weather(destination_blob_name, path_to_file, bucket_name):
    storage_client = storage.Client.from_service_account_json(
        'service_account.json') # TO DO


    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_path+destination_blob_name)
    blob.upload_from_filename(path_to_file)
    
    return blob.public_url


python_callable=extract_weather