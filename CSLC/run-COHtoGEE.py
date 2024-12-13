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
    bucket_name = 'opera-bucket-cslc'
    folderPath = 'products/cslc_historical_20190701_to_20190718_v2/coh'
    targetCollectionPath = 'projects/opera-one/assets/CSLC/2019-07-01-COH-v2'
    #folderPath = 'products/cslc_historical_20190701_to_20190718_v2/amp/'
    #targetCollectionPath = 'projects/opera-one/assets/CSLC/2019-07-01-AMP-v2'
    collectionSubPath = targetCollectionPath.split('/assets/')[-1]
    filename = key.split('/')[-1]
    gcsurl = f'gs://{bucket_name}/{key}'
    asset_name = filename.split('.tif')[0].replace('.','_')
    asset_id = collectionSubPath + '/' + asset_name
    try:
        session = AuthorizedSession(ee.data.get_persistent_credentials())
        s = filename.split('_')
        #print(s)
        tile = s[0]
        start_time = datetime.strptime(s[1],'%Y%m%dT%H%M%SZ').strftime("%Y-%m-%dT%H:%M:%SZ")
        end_time = datetime.strptime(s[2],'%Y%m%dT%H%M%SZ').strftime("%Y-%m-%dT%H:%M:%SZ")
        #prod_time = datetime.strptime(s[8].split('.')[0],'%Y%m%dT%H%M%SZ').strftime("%Y-%m-%dT%H:%M:%SZ")
        pass_d = s[-1].split('.')[0]
        band = s[3]
        request = {
            'type': 'IMAGE',
            'gcs_location': {
                'uris': [gcsurl]
            },
            'properties': {
                'band': band,
                'endTime': end_time,
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
    except:
        print(f'Failed to upload {asset_id}')

if __name__ == '__main__':
    session = AuthorizedSession(ee.data.get_persistent_credentials())

    bucket_name = 'opera-bucket-cslc'
    folderPath = 'products/cslc_historical_20190701_to_20190718_v2/coh'
    targetCollectionPath = 'projects/opera-one/assets/CSLC/2019-07-01-COH-v2'
    #folderPath = 'products/cslc_historical_20190701_to_20190718_v2/amp/'
    #targetCollectionPath = 'projects/opera-one/assets/CSLC/2019-07-01-AMP-v2'
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

    #uploadAsset(keyList[0])
    pool = mp.Pool(mp.cpu_count())
    #run_rtc_transfer(keyPairs[0])
    pool.map(uploadAsset,keyList)
    pool.close()
