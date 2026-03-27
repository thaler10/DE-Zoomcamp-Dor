import os
import requests
from google.cloud import storage

BUCKET_NAME = "dezoomcamp_hw4_dor" 
CREDENTIALS_FILE = "../keys/terraform.json" 

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_FILE

def upload_to_gcs(bucket_name, source_url, destination_blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    print(f"Downloading from {source_url}...")
    
    response = requests.get(source_url, stream=True)
    if response.status_code == 200:
        blob.upload_from_string(response.content, content_type='application/gzip')
        print(f"Successfully uploaded to gs://{bucket_name}/{destination_blob_name}")
    else:
        print(f"Failed to download {destination_blob_name}. Status code: {response.status_code}")

service = 'fhv'
year = '2019'

for month in range(1, 13):
    month_str = f"{month:02d}"
    file_name = f"{service}_tripdata_{year}-{month_str}.csv.gz"
    source_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{service}/{file_name}"
    destination_path = f"{service}/{file_name}"
    
    upload_to_gcs(BUCKET_NAME, source_url, destination_path)

print("\nFinished! All FHV 2019 files are now in your bucket.")