import ee
import subprocess
from google.cloud import storage
from datetime import datetime
import json
from pprint import pprint
ee.Initialize()
from google.auth.transport.requests import AuthorizedSession
import multiprocessing as mp
from productFunctions import makeRequest_DSWxHLS
import config



def uploadAsset(key):
    try:
        bucket_name = config.GCS_BUCKET
        folderPath = config.GCS_PREFIX
        targetCollectionPath = config.GEE_COLLECTION_PATH
        product = config.PRODUCT

        collectionSubPath = targetCollectionPath.split('/assets/')[-1]
        filename = key.split('/')[-1]
        #print(filename)
        gcsurl = f'gs://{bucket_name}/{key}'
        asset_name = filename.split('.tif')[0]+filename.split('.tif')[1]
        asset_id = collectionSubPath + '/' + asset_name.replace('.','_')
        #print(asset_name)
        session = AuthorizedSession(ee.data.get_persistent_credentials())
        if product == 'DSWx-HLS':
            request = makeRequest_DSWxHLS(filename,gcsurl)
        project_folder = targetCollectionPath.split('/')[1]
        url = 'https://earthengine.googleapis.com/v1alpha/projects/{}/assets?assetId={}'
        #print(project_folder)
        #pprint(request)
        response = session.post(
            url = url.format(project_folder, asset_id),
            data = json.dumps(request)
            )
        print(f'{response} for upload: {asset_id}')
        #for item in response:
            #print(item)
    except Exception as e:
        print(f'Failed to upload {key}')
        print(e)

if __name__ == '__main__':
    session = AuthorizedSession(ee.data.get_persistent_credentials())

    bucket_name = config.GCS_BUCKET
    folderPath = config.GCS_PREFIX
    targetCollectionPath = config.GEE_COLLECTION_PATH
    collectionSubPath = targetCollectionPath.split('/assets/')[-1]

    targetCollection = ee.ImageCollection(targetCollectionPath)
    targetIDs = targetCollection.aggregate_array('system:index').getInfo()
    print(len(targetIDs))

    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=folderPath)
    gcsKeys = []
    for blob in blobs:
        gcsKeys.append(blob.name)
    print(len(gcsKeys))

    keyList = []
    for key in gcsKeys:
        filename = key.split('/')[-1]
        asset_name = filename.split('.tif')[0].replace('.','_')
        if asset_name in targetIDs:
            continue
        else:
            keyList.append(key)

    print(len(keyList))

    pool = mp.Pool(mp.cpu_count())
    pool.map(uploadAsset,keyList)
    pool.close()
