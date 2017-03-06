from enum import Enum
import pdb

from .unit import Unit
from .point_type import PointType
from ..common_exceptions import NoMatchedSchemaError

# Read schema file (temporarily define here)
# TODO: This should be generated from a formal schema file
schema = {
        'unit': Unit,
        'point_type': PointType,
        'latitude': float,
        'longitude': float,
        }
