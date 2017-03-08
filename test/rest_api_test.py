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
            'point_type':'temperature',
            'unit':'DEG_F',
            'latitude': 0, 'longitude': 0
            } 
        }

def test_add_point():
    print('Init adding point test')
    resp = requests.post(
            base_url+'/point/',
            json=test_point_metadata)
    if resp.status_code!=201:
        print("Cannot add point correctly")
        print("known reason: {0}".format(resp.text))
        if resp.json()['reason']=='Given name already exists':
            pass
        else:
            assert(False)
    uuid = resp.json()['uuid']
    print(resp.text)
    print('Done adding point test')


def _get_uuid(query):
    params = {'query': json.dumps(query)}
    sensor = requests.get(point_url, params=params).json()[0]
    return sensor['uuid']

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
    uuid = _get_uuid(query)

    # delete the uuid
    resp = requests.delete(base_url+'/point/{0}'.format(uuid))
    assert(resp.status_code==200)

    # find the uuid has no point (confirm delete succeeded)
    params = {"query": json.dumps(query)}
    num_points = len(requests.get(point_url, params=params).json())
    assert(num_points==0)

    print('Done point delete test')

start_time = '1488830000'
end_time = '1488840000'

test_ts_data = {
        start_time: 777,
        end_time: 555,
        }

def test_put_timeseries():
    print('Init put timeseries test')
    query = {'name': test_point_metadata['name']}
    uuid = _get_uuid(query)
    data = {'samples': test_ts_data}
    ts_url = point_url + '/{0}/timeseries'.format(uuid)
    resp = requests.post(ts_url, json=data)
    assert(resp.status_code==200)
    print('Done put timeseries test')

def test_get_timeseries():
    print('Init get timeseries test')
    query = {'name': test_point_metadata['name']}
    uuid = _get_uuid(query)
    params = {
            'start_time': str(int(start_time) - 1000),
            'end_time': str(int(end_time) + 1000)
            }
    ts_url = point_url + '/{0}/timeseries'.format(uuid)
    resp = requests.get(ts_url, params=params)
    print(resp.text)
    assert(resp.json()['data'] == test_ts_data)
    print('Done get timeseries test')

def test_delete_timeseries():
    print('Init delete timeserie partially test')
    query = {'name': test_point_metadata['name']}
    uuid = _get_uuid(query)
    delete_start_time = str(int(start_time) - 100)
    delete_end_time = str(int(start_time) + 10)

    ts_url = point_url + '/{0}/timeseries'.format(uuid)
    params = {
            'start_time': delete_start_time,
            'end_time': delete_end_time
            }
    resp = requests.delete(ts_url, params=params)
    assert(resp.status_code==200)

    params = {
            'start_time': str(int(start_time) - 1000),
            'end_time': str(int(end_time) + 1000)
            }
    ts_url = point_url + '/{0}/timeseries'.format(uuid)
    resp = requests.get(ts_url, params=params)
    assert(len(resp.json()['data'])==1)
    print('Done delete timeserie partially test')

if __name__ == '__main__':
    test_add_point()
    test_find_point()
    test_find_all_points()
    test_put_timeseries()
    test_get_timeseries()
    test_delete_timeseries()
    test_delete_point()