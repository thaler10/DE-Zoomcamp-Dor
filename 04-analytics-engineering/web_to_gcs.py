import io
import os
import requests
import pandas as pd
from google.cloud import storage

BUCKET = "dezoomcamp_hw4_dor" 
SERVICES = ['yellow', 'green']
YEARS = ['2019', '2020']

def upload_to_gcs(bucket, object_name, local_file):
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

def web_to_gcs(year, service):
    for month in range(1, 13):
        month = f"{month:02d}"
        file_name = f"{service}_tripdata_{year}-{month}.csv.gz"
        request_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{service}/{file_name}"
        
        print(f"Downloading {request_url}...")
        os.system(f"wget {request_url} -O {file_name}")
        
        print(f"Uploading {file_name} to GCS...")
        upload_to_gcs(BUCKET, f"{service}/{file_name}", file_name)
        
        os.remove(file_name)
        print(f"Finished {file_name}")

if __name__ == "__main__":
    for year in YEARS:
        for service in SERVICES:
            web_to_gcs(year, service)