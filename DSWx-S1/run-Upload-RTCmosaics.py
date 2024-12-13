import ee
import subprocess
from google.cloud import storage
from datetime import datetime
import json
from pprint import pprint
ee.Initialize()
from google.auth.transport.requests import AuthorizedSession
import multiprocessing as mp



def uploadAsset(key):
    try:
        bucket_name = 'opera-bucket-dswx'
        folderPath = 'collections/validation-rtc-mosaics'
        targetCollectionPath = 'projects/opera-one/assets/DSWX/S1/Validation/rtc-mosaic'
        collectionSubPath = targetCollectionPath.split('/assets/')[-1]
        filename = key.split('/')[-1]
        #print(filename)
        gcsurl = f'gs://{bucket_name}/{key}'
        asset_name = filename.split('.tif')[0]+filename.split('.tif')[1]
        asset_id = collectionSubPath + '/' + asset_name.replace('.','_')
        #print(asset_name)
        session = AuthorizedSession(ee.data.get_persistent_credentials())
        s = filename.rstrip('.tif').split('_')
        #print(s)
        pol = s[1]
        site = f'{s[2]}_{s[3]}'
        #burst = s[3]
        #start_time = datetime.strptime(s[4],'%Y%m%dT%H%M%SZ').strftime("%Y-%m-%dT%H:%M:%SZ")
        #prod_time = datetime.strptime(s[5].split('.')[0],'%Y%m%dT%H%M%SZ').strftime("%Y-%m-%dT%H:%M:%SZ")
        #sensor = s[6]
        #band = s[9]
        #version = s[8]
        request = {
            'type': 'IMAGE',
            'gcs_location': {
                'uris': [gcsurl]
            },
            'properties': {
                'polarization': pol,
                'site': site 
            },
            'startTime': '2021-09-01T00:00:00Z',
            'bands': {
                "id": pol
            }
        }
        
        project_folder = 'opera-one'
        url = 'https://earthengine.googleapis.com/v1alpha/projects/{}/assets?assetId={}'
        #pprint(request)
        response = session.post(
            url = url.format(project_folder, asset_id),
            data = json.dumps(request)
            )
        print(f'{response} for upload: {asset_id}')
        #for item in response:
        #    print(item)
    except Exception as e:
        print(f'Failed to upload {key}')
        print(e)

if __name__ == '__main__':
    session = AuthorizedSession(ee.data.get_persistent_credentials())

    bucket_name = 'opera-bucket-dswx'
    folderPath = 'collections/validation-rtc-mosaics'
    targetCollectionPath = 'projects/opera-one/assets/DSWX/S1/Validation/rtc-mosaic'
    collectionSubPath = targetCollectionPath.split('/assets/')[-1]

    targetCollection = ee.ImageCollection(targetCollectionPath)
    targetIDs = targetCollection.aggregate_array('system:index').getInfo()
    print(len(targetIDs))

    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=folderPath+'/')
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
    #uploadAsset(keyList[1])
    pool.map(uploadAsset,keyList)
    pool.close()
