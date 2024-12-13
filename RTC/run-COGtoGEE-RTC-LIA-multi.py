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
    bucket_name = 'opera-bucket-rtc'
    folderPath = 'products/2023-05-04_globalrun_2021-04-11_to_2021-04-22_local_incidance_angle/'
    targetCollectionPath = 'projects/opera-one/assets/RTC/LIA-2023-05-09'
    collectionSubPath = targetCollectionPath.split('/assets/')[-1]
    filename = key.split('/')[-1]
    gcsurl = f'gs://{bucket_name}/{key}'
    asset_name = filename.split('.tif')[0].replace('.','_').replace('_local_incidence_angle','')
    asset_id = collectionSubPath + '/' + asset_name
    try:
        session = AuthorizedSession(ee.data.get_persistent_credentials())
        s = filename.split('_')
        #print(s)
        tile = s[3]
        start_time = datetime.strptime(s[4],'%Y%m%dT%H%M%SZ').strftime("%Y-%m-%dT%H:%M:%SZ")
        prod_time = datetime.strptime(s[5].split('.')[0],'%Y%m%dT%H%M%SZ').strftime("%Y-%m-%dT%H:%M:%SZ")
        pass_d = s[-1].split('.')[0]
        sensor = s[6]
        band = s[9]
        request = {
            'type': 'IMAGE',
            'gcs_location': {
                'uris': [gcsurl]
            },
            'properties': {
                'sensor': sensor,
                'band': 'local_incidencee_angle',
                'prod_time': prod_time,
                'tile': tile,
                'pass_direction':pass_d
            },
            'startTime': start_time,
            }
        #pprint(request)
        project_folder = 'opera-one'
        url = 'https://earthengine.googleapis.com/v1alpha/projects/{}/assets?assetId={}'
        response = session.post(
            url = url.format(project_folder, asset_id),
            data = json.dumps(request)
            )
        print(f'{response} for upload: {asset_id}')
        #for item in response.iter_lines():
        #    print(item)
    except:
        print(f'Failed to upload {asset_id}')

if __name__ == '__main__':
    session = AuthorizedSession(ee.data.get_persistent_credentials())

    bucket_name = 'opera-bucket-rtc'
    folderPath = 'products/2023-05-04_globalrun_2021-04-11_to_2021-04-22_local_incidance_angle'
    targetCollectionPath = 'projects/opera-one/assets/RTC/LIA-2023-05-09'
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
        asset_name = filename.split('.tif')[0].replace('.','_').replace('_local_incidence_angle','')
        if asset_name in targetIDs:
            continue
        else:
            keyList.append(key)

    print(len(keyList))

    pool = mp.Pool(mp.cpu_count())
    #uploadAsset(keyList[0])
    pool.map(uploadAsset,keyList)
    pool.close()
