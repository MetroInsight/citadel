import requests
import time, random
import json
import pdb

base_url = 'http://127.0.0.1:8080/api'
point_url = base_url + '/point'

def test_mongodb():
    from mongoengine import connect
    
    m = ("metroinsight", "127.0.0.1", 27017)
    
    from app.models.metadata import Point
    
    print(Point.objects.to_json())
    print("mongodb initiated correctly")


test_point_metadata = {
        'name': 'example_point_8',
        'tags': { 
            'point_type':'Temperature',
            'unit':'DEG_F',
            'latitude': 0,
            'longitude': 0
            } 
        }


def test_add_point():
    print('Init adding point test')
    resp = requests.post(
            base_url+'/point/',
            json=test_point_metadata)
    try:
        assert(resp.status_code==201)
    except:
        print("Cannot add point correctly")
        print("known reason: {0}".format(resp.text))
    print('Done adding point test')
        

def test_find_point():
    print('Init finding a point test')
    query = {'name': test_point_metadata['name']}
    params = {"query": json.dumps(query)}
    resp = requests.get(point_url, params=params)
    assert(resp.status_code==200)
    found_point = resp.json()[0]
    for key, val in test_point_metadata.items():
        assert(found_point[key]==val)
    print('Done finding a point test')

def test_find_all_points():
    print('Init find all points test')
    resp = requests.get(point_url)
    found_point = resp.json()[0]
    for key, val in test_point_metadata.items():
        assert(found_point[key]==val)
    print('Done find all points test')

def test_delete_point():
    print('Init point delete test')
    # find uuid
    query = {'name': test_point_metadata['name']}
    params = {"query": json.dumps(query)}
    uuid = requests.get(point_url, params=params).json()[0]['uuid']
    # delete the uuid
    resp = requests.delete(base_url+'/point/{0}'.format(uuid))
    assert(resp.status_code==200)

    # find the uuid has no point (confirm delete succeeded)
    num_points = len(requests.get(point_url, params=params).json())
    assert(num_points==0)

    print('Done point delete test')

def test_timeseries():
    uuid = '585c7025-0b51-4be5-9723-17293c52bde9'
    url = base_url+'/{0}/timeseries/'.format(uuid)
    data = {
            'samples':[
                {
                    "time": int(time.time()),
                    "value": random.randint(0,100)
                    }
                ]
            }
    response = requests.post(url,json=data)
    print(response)

if __name__ == '__main__':

    test_add_point()
    test_find_point()
    test_find_all_points()
    test_delete_point()

    #test_mongodb()
    #test_timeseries()
