# Imports the Google Cloud client library
from google.cloud import storage
from tqdm import tqdm

PROJECT_NAME = 'trial-cloud-storage-19852'

def get_bucket(bucket_name, client=None):
    if not client:
        client = storage.Client(PROJECT_NAME)
    return client.get_bucket(bucket_name)

def upload_from_data(data, filename, bucket, **kwargs):
    blob = bucket.blob(filename)
    blob.upload_from_string(data, **kwargs)

def get_downloaded_ids(bucket, out_dir):
    ids = set(
        [b.name.split('/')[-1].split('.')[0] 
            for b in tqdm(bucket.list_blobs()) 
            if b.name.startswith(out_dir)]
    )
    return ids