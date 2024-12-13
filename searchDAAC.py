import os
from pprint import pprint
import requests
import math
import config

minLon = config.MINLON
maxLon = config.MAXLON
minLat = config.MINLAT
maxLat = config.MAXLAT
startDate = config.STARTDATE
endDate = config.ENDDATE
product = config.PRODUCT
band = config.BAND
if product == 'DSWx-HLS':
    conceptID = 'C2617126679-POCLOUD'
    provider = 'POCLOUD'
bbox = f'{minLon},{minLat},{maxLon},{maxLat}'

search = {
        'concept_id': conceptID,
        'temporal': f"{startDate}/{endDate}",
        'page_size': 2000,
        'page_num': 1,
        'bounding_box': bbox
        }

CMR_OPS = 'https://cmr.earthdata.nasa.gov/search'
url = f'{CMR_OPS}/{"granules"}'

response = requests.get(url, 
                        params=search,
                        headers={
                            'Accept': 'application/json'
                            }
                       )
print(response.status_code)
if response.status_code != 200:
    print(f'Data search ended in response {response.status_code}')

numHits = int(response.headers['cmr-hits'])
print(numHits)

numPages = math.ceil(numHits/2000)
print(numPages)

download_urls = []
for p in range(numPages):
    page = p+1
    response = requests.get(url, 
                        params={
                            'concept_id': conceptID,
                            'temporal': f"{startDate}/{endDate}",
                            'page_size': 2000,
                            'page_num': page,
                            'bounding_box': bbox,
                            'page_num':page
                            },
                        headers={
                            'Accept': 'application/json'
                            }
                       )
    print(response.status_code)
    if response.status_code != 200:
        print(f'Data search ended in response {response.status_code}')
    granules = response.json()['feed']['entry']
    for granule in granules:
        for links in granule['links']:
            link = links['href']
            if link.split(':')[0] == 'https' and link.split('_')[-1] == band+'.tif':
                download_urls.append(link)
with open(config.URL_LIST, 'w') as f:
    for link in download_urls:
        f.write(link+'\n')