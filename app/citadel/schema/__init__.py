from enum import Enum
import pdb

from .unit import Unit
from .point_type import PointType
from ..common_exceptions import NoMatchedSchemaError

# Read schema file (temporarily define here)
# TODO: This should be generated from a formal schema file
schema = {
        'unit': Unit,               # Unit
        'point_type': PointType,    # Point type
        'latitude': float,          # longitude of a point's geo location
        'longitude': float,         # longitude of a point's geo location
        'source_reference':str,     # reference link to the data source.
        'license':str,              # license type, this should have a enum
        }
