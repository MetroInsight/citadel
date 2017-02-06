from flask import Blueprint
from flask_restplus import Api, Resource

api_blueprint = Blueprint('api', __name__)
api = Api(api_blueprint)

from .point import ns as point_namespace
api.add_namespace(point_namespace)

from .timeseries import ns as timeseries_namespace
api.add_namespace(timeseries_namespace)
