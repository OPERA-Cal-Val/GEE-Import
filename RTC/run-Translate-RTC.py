import subprocess
import boto3
import os
from google.cloud import storage
import shutil
import time
from botocore import UNSIGNED
from botocore.client import Config
os.environ["GCLOUD_PROJECT"] = "opera-one"

s3_prefix = 'products/int_fwd_r2/2023-05-04_globalrun_2021-04-11_to_2021-04-22/RTC_S1/'
gcs_prefix = 'products/2023-05-04_globalrun_2021-04-11_to_2021-04-22/'

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name,timeout=None)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name,timeout=None)

def processRTC(s3key,gcskey):
    if os.path.exists('./temp/'):
        shutil.rmtree('./temp/')
    os.mkdir('./temp/')
    #os.mkdir('./temp/deflate/')

    #Download RTC file
    #print('Downloading RTC File')
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    bucket = 'opera-pst-rs-pop1'
    filename = s3key.split('/')[-1]
    filepath = './temp/'+filename
    s3.download_file(bucket, s3key, filepath)

    #Change compression to DEFLATE
    #print('Translating compression')
    tempfile = './temp/gtiff32.tif'
    outfile = './temp/cog32deflate.tif'
    gdal_cmd1 = f'gdal_translate -of GTiff -co NBITS=32 {filepath} {tempfile}'
    gdal_cmd2 = f'gdal_translate -of COG -co COMPRESS=DEFLATE -co RESAMPLING=AVERAGE {tempfile} {outfile}'
    subprocess.run(gdal_cmd1,shell=True,stdout=subprocess.DEVNULL)
    subprocess.run(gdal_cmd2,shell=True,stdout=subprocess.DEVNULL)
    metadata = os.popen(f'gdalinfo {filepath}').read()
    if metadata.split('ORBIT_PASS_DIRECTION=')[1].split('\n')[0] == 'ASCENDING':
        pass_d = 'A'
    elif metadata.split('ORBIT_PASS_DIRECTION=')[1].split('\n')[0] == 'DESCENDING':
        pass_d = 'D'
    else:
        pass_d = 'N'

    #Upload to GCS bucket
    #print('Uploading to GCS Bucket')
    gcskey = gcskey.split('.tif')[0] + f'_{pass_d}.tif'
    gcsbucket = "opera-bucket-rtc"
    upload_blob(gcsbucket,outfile,gcskey)


s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
result = s3.list_objects(Bucket='opera-pst-rs-pop1', Prefix=s3_prefix , Delimiter='/')
keyList = []
for o in result.get('CommonPrefixes'):
    path = o.get('Prefix')
    if path[-2] != 's':  #checks if end is not "static layers"
        fname = path.split('/')[-2]+'_VH.tif'
        keyList.append(path+fname)

print(f'{len(keyList)} s3 keys found')

storage_client = storage.Client()
blobs = storage_client.list_blobs("opera-bucket-rtc", prefix=gcs_prefix)
gcsKeys = []
for blob in blobs:
    gcsKeys.append(blob.name.split('.tif')[0][:-2]+'.tif')
print(f'{len(gcsKeys)} existing gcs keys found')

i = 0
start_time = time.time()
for key in keyList[0:5]:
    fname = key.split('/')[-1]
    gcsKey = gcs_prefix+fname
    if gcsKey not in gcsKeys:
        processRTC(key,gcsKey)
        print(f'[{i} - {time.time() - start_time}] Uploaded to: {gcsKey}')
    else:
        print(f'[{i} - {time.time() - start_time}] Skipped: {gcsKey}')
    i = i+1