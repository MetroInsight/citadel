import enum
import pdb

from mongoengine import *
from mongoengine.errors import NotUniqueError 

from schema import schema

def metadata_initialization(app):
    connect(app.config['MONGODB_DATABASE'],
            host=app.config['MONGODB_HOST'],
            port=app.config['MONGODB_PORT'])


class Point(Document):
    name = StringField(required=True, unique=True)
    uuid = StringField(required=True, unique=True)
    tags = DictField() 

def schema_projection(metadata):
    """ 
    Input: metadata (dict)
    Output: Normalized metadata (dict)
    """
    norm_metadata = dict()
    try:
        for tag, data_type in schema.items():
            if isinstance(data_type, enum.EnumMeta):
                schema_value = data_type[metadata[tag]].name
            else:
                schema_value = str(data_type(metadata[tag]))
            norm_metadata[tag] = schema_value
            del metadata[tag]
    except KeyError as e:
        raise e
    for tag, value in metadata.items():
        norm_metadata[tag] = value

    #schema_validation(norm_metadata) # Jason: Do we need this? Maybe not.

    return norm_metadata
        

# Define Exceptions
# TODO: names should be normalized across different implementations later.
