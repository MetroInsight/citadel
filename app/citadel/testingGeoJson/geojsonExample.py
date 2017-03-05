from flask import Flask
import sys
from pymongo import MongoClient, GEO2D
client = MongoClient('localhost:27017')
db = client.Points
db.Points.create_index([("loc", GEO2D)])

# Function to insert data into mongo db
def insert():
    try:
     PointId = input('Enter Point id :')
     Name = input('Enter Name :')
     lat = float(input('Enter Lat :'))
     lng = float(input('Enter Lng :'))
     db.Points.insert_one(
        {
        "id": PointId,
         "name":Name,
         "loc":[lat,lng]  
        })
     print('\nInserted data successfully\n')
    except Exception as e:
        print(str(e))

# function to read records from mongo db
def read():
    try:
     ptcol = db.Points.find()
     print('\n All data from Points Database \n')
     for pt in ptcol:
        print(pt)

    except Exception as e:
        print(str(e))



def findPoints():
 lat = float(input('Enter Lat :'))
 lng = float(input('Enter Lng :'))
 try:
  ptcol = db.Points.find({"loc": {"$near": [lat, lng]}}).limit(2)
  for pt in ptcol:
   print(pt)
 except Exception as e:
   print(str(e))


def main():

    while(1):
    # chossing option to do CRUD operations
        selection = input('\nSelect 1 to insert, 2 to read, 3 to find\n')
    
        if selection == '1':
            insert()
        elif selection == '2':
            read()
        elif selection == '3':
            findPoints()
        else:
            print('\n INVALID SELECTION \n')

if __name__ == "__main__": main()




