from requests import post, get
import time, random

base_url = 'http://127.0.0.1:8080'

def test_mongodb():
	from mongoengine import connect

	m = ("metroinsight", "127.0.0.1", 27017)

	from app.models.metadata import Point

	print (Point.objects)

def test_point_api():
	response = post(base_url+'/api/point/',
		json = {'name': 'example_point_8',
				'tags': { 
                            'point_type':'Temperature',
						    'unit':'DEG_F',
                            'latitude': 0,
                            'longitude': 0
                        } 
		})

	print (response)

	print (get('http://127.0.0.1:8080/api/point/').json())

def test_timeseries():
	uuid = '585c7025-0b51-4be5-9723-17293c52bde9'
	url = base_url+'/api/timeseries/'+uuid
	data = {
			'samples':[
				{
					"time": int(time.time()),
					"value": random.randint(0,100)
				}
			]
		}
	response = post(url,json=data)

	print(response)

#test_point_api()
#test_timeseries()
test_mongodb()
