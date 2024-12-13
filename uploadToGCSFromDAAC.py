import subprocess
import os
from google.cloud import storage
import shutil
import time
import multiprocessing as mp
import requests
import productFunctions
import config



def upload_blob(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name,timeout=None)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name,timeout=None)


def run_upload(keydict):
    product = config.PRODUCT
    
    try:
        start_time = time.time()
        s3key = keydict['s3key']
        gcskey = keydict['gcsKey']
        if product == 'DSWx-HLS':
            productFunctions.processDSWx(s3key,gcskey)
        #print(f'[{time.time() - start_time}] Uploaded to: {gcskey}')
        print(f'Uploaded to: {gcskey}')
    except Exception as e: 
        print(f'Failed on {s3key}')
        #print(e)

if __name__ == '__main__':
    keyList= []
    with open(config.URL_LIST) as f:
        lines = f.readlines()
        for line in lines:
            line = line.rstrip('\n')
            keyList.append(line)
    os.environ["GCLOUD_PROJECT"] = config.GCS_PROJECT
    gcs_prefix = config.GCS_PREFIX
    print(f'{len(keyList)} s3 keys found')

    storage_client = storage.Client()
    blobs = storage_client.list_blobs(config.GCS_BUCKET, prefix=gcs_prefix)
    gcsKeys = []
    for blob in blobs:
        gcsKeys.append(blob.name)
    print(f'{len(gcsKeys)} existing gcs keys found')

    keyPairs = []
    for key in keyList:
        fname = key.split('/')[-1]
        gcsKey = gcs_prefix+fname.rstrip('.tif').replace('.','_')+'.tif'
        if gcsKey not in gcsKeys:
            keydict = {'s3key':key,'gcsKey':gcsKey}
            keyPairs.append(keydict)
    print(f'{len(keyPairs)} key pairs identified')
    #print(keyPairs[0])
    pool = mp.Pool(mp.cpu_count())
    #run_rtc_transfer(keyPairs[1])
    pool.map(run_upload,keyPairs)
    pool.close()