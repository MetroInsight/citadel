import requests
from copy import deepcopy
import time, random
import json
import pdb
from citadel import Citadel

import config

base_url = 'http://' + config.SERVER_NAME

citadel = Citadel(base_url)

test_point_metadata = {
        'name': 'example_point_8',
        'tags': { 
            'point_type':'temperature',
            'unit':'DEG_F',
            'source_reference': 'http://aaa.com',
            'license': 'gplv2'
            },
        'geometry':{
            'coordinates':[0,0],
            'type':'Point'
            }
        }


test_point_metadata_2 = deepcopy(test_point_metadata)
test_point_metadata_2['name'] = 'example_point_7'

metadata_dict = {
        test_point_metadata['name']: test_point_metadata,
        test_point_metadata_2['name']: test_point_metadata_2
        }

def test_create_point(metadata):
    print('Init adding point test')
    res = citadel.create_point(metadata)
    if not res['success']:
        print(res['result']['reason'])
        assert(False)
    print('Done adding point test')


def _get_uuid_by_name(name):
    res = citadel.query_points(name=name)
    if res['success']:
        return res['result']['point_list'][0]['uuid']
    else:
        print(res['result']['reason'])
        assert(False)

def test_find_one_point():
    print('Init finding a point test')
    query = test_point_metadata['name']
    res = citadel.query_points(tag_query=query)
    if res['result']:
        print(res['result'])
    else:
        print(res['result']['reason'])
        assert(False)

def test_find_all_points():
    print('Init find all points test')
    res = citadel.query_points()
    if res['result']:
        print(res['result'])
    else:
        print(res['result']['reason'])
        assert(False)
    print('Done find all points test')


geo_query = {
        'type': 'bounding_box',
        'geometry_list': [[-1,-1],[1,1]] 
        # [[westsouth_lng, westsouth_lat], [eastnorth_lng, eastnorth_lat]]
        }

def test_geo_query(geo_query):
    print('Init geo query')
    res = citadel.query_points(geo_query=geo_query)
    if res['result']:
        print(res['result'])
    else:
        print(res['result']['reason'])
        assert(False)
    print('Done geo query')



def test_delete_point():
    print('Init point delete test')
    # find uuid
    uuid = _get_uuid_by_name(test_point_metadata['name'])

    # delete the uuid
    res = citadel.delete_point(uuid)
    if not res['success']:
        print(res['result']['reason'])
        assert(False)

    print('Done point delete test')

start_time = '1488830000'
end_time = '1488840000'

test_ts_data = {
        start_time: 777,
        end_time: 555,
        }

def test_put_timeseries(ts_data):
    print('Init put timeseries test')
    uuid = _get_uuid_by_name(name=test_point_metadata['name'])
    res = citadel.put_timeseries(uuid, ts_data)
    if not res['success']:
        print(res['result'])
        assert(False)
    print('Done put timeseries test')

def test_get_timeseries():
    print('Init get timeseries test')
    uuid = _get_uuid_by_name(test_point_metadata['name'])
    start_time = str(int(start_time) - 1000)
    end_time = str(int(end_time) + 1000)
    res = citadel.get_timeseries(uuid, start_time, end_time)
    if res['success']:
        data = res['result']['data']
        if data!=test_ts_data:
            print('Incorrect data')
            print('Original data: ', test_ts_data)
            print('Received data: ', data)
            assert(False)
    else:
        print(res['result']['reason'])
        assert(False)
    print('Done get timeseries test')

def test_delete_timeseries():
    print('Init delete timeserie partially test')
    uuid = _get_uuid_by_name(test_point_metadata['name'])
    delete_start_time = str(int(start_time) - 100)
    delete_end_time = str(int(start_time) + 10)

    res = citadel.delete_timeseries(uuid, start_time, end_time)
    if not res['success']:
        print(res['result']['reason'])
        assert(False)
    print('Done delete timeserie partially test')

if __name__ == '__main__':
    try:    
        test_create_point(test_point_metadata)
        test_create_point(test_point_metadata_2)
        test_find_one_point()
        test_find_all_points()
        #test_geo_query(geo_query)
        test_put_timeseries(test_ts_data)
        test_get_timeseries()
        test_delete_timeseries()
        test_delete_point()
    except:
        test_delete_point()

