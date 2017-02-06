from mongoengine import *

class Tag(EmbeddedDocument):
    key = StringField()
    value = StringField()

class Point(Document):
    name = StringField(required=True, unique=True)
    uuid = StringField(required=True, unique=True)
    tags = ListField(EmbeddedDocumentField(Tag)) 
