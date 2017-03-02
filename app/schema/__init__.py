from enum import Enum
import pdb

from .unit import Unit
from .point_type import PointType
from ..common_exceptions import NoMatchedSchemaError

# Read schema file (temporarily define here)
# TODO: This should be generated from a formal schema file
schema = {
        'unit': Unit,
        #'point_type': PointType,
        'location_altitude': float,
        'location_longitude': float,
        }

def schema_validation_deprecated(metadata):
    for tag, data_type in schema.items():
        try:
            given_value = metadata[tag]
        except KeyError as e:
            return e
        except:
            pdb.set_trace()
        if not isinstance(given_value, data_type):
            raise NoMatchedSchemaError("{0} has wrong type. It is supposed to be {1}".format(str(given_value), str(data_type)))
