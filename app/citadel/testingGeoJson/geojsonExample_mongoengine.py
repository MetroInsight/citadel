import pprint
import sys
from mongoengine import *
import json
from geojson import Point
connect('citadel')

class Point(Document):
	name=StringField(required=True,unique=True)
        geo_point=PointField()

# Function to insert data into mongo db
def insert():
     Name = raw_input('Enter Name :')
     print(Name)
     lat = float(input('Enter Lat :'))
     lng = float(input('Enter Lng :'))
     latp = Point((lat,lng))
     Point(name=Name,geo_point=[lat,lng]).save()
     print('\nInserted data successfully\n')
    
def read():
    try:
     Name = raw_input('Enter Name :')
     point=Point.objects(name=Name).first()
     print(point.name)
     print(point.geo_point)
     print('\nread data successfully\n')
    except Exception as e:
        print(str(e))

def findPoints():
  try:
    #Name = raw_input('Enter Name :')
    print('first query')
    point=Point.objects(geo_point__geo_within_box=[(-100,100),(-100,100)])
    for p in point:
     print(p.geo_point)
    print('2nd query')
    point=Point.objects(geo_point__geo_within_center=[(0,0),10000])
    for p in point:
     print(p.geo_point)
  except Exception as e:
    print(str(e))

def main():

    while(1):
    # chossing option to do CRUD operations
        selection = input('\nSelect 1 to insert, 2 to read, 3 to find\n')
    
        if selection == 1:
            insert()
        elif selection == 2:
            read()
        elif selection == 3:
            findPoints()
        else:
            print('\n INVALID SELECTION \n')

if __name__ == "__main__": main()




