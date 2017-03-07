from flask import Blueprint
from flask_restplus import Api, Resource
from flask import json

api_blueprint = Blueprint('api', __name__)
api = Api(api_blueprint, version='0.1', title='Citadel API'\
            , description='', doc='/doc/')

from .point import point_api
api.add_namespace(point_api)

#from .timeseries import ts_ns as timeseries_namespace
#api.add_namespace(timeseries_namespace)

#print("==========")
#print(json.dumps(api.__schema__))
#print("==========")
