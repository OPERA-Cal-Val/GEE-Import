import os
from datetime import datetime
import requests
import os
from google.cloud import storage
import shutil
import time
import config

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name,timeout=None)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name,timeout=None)


def makeRequest_DSWxHLS(filename,gcsurl):
    s = filename.split('_')
    burst = s[3]
    start_time = datetime.strptime(s[4],'%Y%m%dT%H%M%SZ').strftime("%Y-%m-%dT%H:%M:%SZ")
    prod_time = datetime.strptime(s[5].split('.')[0],'%Y%m%dT%H%M%SZ').strftime("%Y-%m-%dT%H:%M:%SZ")
    sensor = s[6]
    band = s[10]
    version = s[8]
    request = {
        'type': 'IMAGE',
        'gcs_location': {
            'uris': [gcsurl]
        },
        'properties': {
            'band': band,
            'sensor': sensor,
            'version': version,
            'prod_time':prod_time,
            'tile': burst
        },
        'startTime': start_time,
        }
    return request

def processDSWx(s3key,gcskey):
    filename = s3key.split('/')[-1]
    if os.path.exists(f'./downloads/{filename}/'):
        shutil.rmtree(f'./downloads/{filename}/')
    os.makedirs(f'./downloads/{filename}/')

    def download(url: str, fname: str):
        resp = requests.get(url, stream=True,)
        #print(f'Data download ended in response {resp.status_code}')
        if resp.status_code != 200:
            print(f'Data download ended in response {resp.status_code}')
        else:
            with open(fname, 'wb') as file:    
                for data in resp.iter_content(chunk_size=1024):
                    file.write(data)

    filename = s3key.split('/')[-1]
    filepath = f'./downloads/{filename}/'+filename
    download(s3key,filepath)
    outfile = filepath
    gcskey = gcskey
    gcsbucket = config.GCS_BUCKET
    upload_blob(gcsbucket,outfile,gcskey)

    shutil.rmtree(f'./downloads/{filename}/')
