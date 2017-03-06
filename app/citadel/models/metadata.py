import enum

from mongoengine import *
from mongoengine.errors import NotUniqueError 

from ..schema import schema

class Point(Document):
    name = StringField(required=True, unique=True)
    uuid = StringField(required=True, unique=True)
    tags = DictField() 
    geo_point = PointField()

