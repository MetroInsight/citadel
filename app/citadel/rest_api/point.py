"""
citadel.rest_api.point

This module handles point create, view and delete functions
"""
import sys
from uuid import uuid4
from copy import deepcopy
import pdb
import json

from flask import request, jsonify
from flask.views import MethodView
#from ..models import Point
from flask_restplus import Api, Resource, Namespace, fields
import arrow

from . import api
from . import responses
from .. import timeseriesdb
from ..models import timeseries as ts
from ..models.metadata import Point
from ..schema.converters import schema_converter
from mongoengine import NotUniqueError

point_api = Namespace('point', description='Operations related to points')

m_point = api.model('Point',{
        'uuid': fields.String(
            description='Unique identifier of point'),
        'tags': fields.Raw(
            description='key, value pairs in dictionary format'),
        'name': fields.String(
            description='Unique human readable identifier of point')
    })

m_message = {
        'success': fields.Boolean(),
        'reason': fields.String(),
        'uuid': fields.String()
        }

m_timeseries = {
        'success': fields.Boolean()
        }



parser = point_api.parser()
parser.add_argument('name', required=True)

point_create_success_msg = 'Point created'
point_create_fail_msg = 'Failed to create point'
point_delete_success_msg = 'Point deleted'
point_delete_fail_msg = 'Failed to delete point'

influxdb_time_format = "2009-11-10T23:00:00Z"


@point_api.route('/')
class PointListAPI(Resource):

    @point_api.response(200, 'Points found')
    @point_api.marshal_list_with(m_point)
    def get(self):
        #data = request.get_json(force=True)
        query_str = request.args.get('query')
        if query_str:
            query = json.loads(query_str)
            return list(Point.objects(__raw__=query))
        else:
            return list(Point.objects())

    @point_api.response(201, point_create_success_msg)
    @point_api.response(409, point_create_fail_msg)
    @point_api.marshal_with(m_message)
    def post(self):
        """Creates a point"""
        data = request.get_json(force=True)
        point_name = data['name']
        tags = data['tags']
        uuid = str(uuid4())
        try:
            normalized_tags = schema_converter(tags)
        except KeyError as err:
            resp_data = {
                    'success': False,
                    'reason': 'Not matched to the schema: ' + str(err)
                    }
            status_code = 409
            return resp_data, 409
        try:
            res = Point(name=point_name, uuid=uuid, tags=normalized_tags).save()
            resp_data = {
                'success': True,
                'reason': '',
                'uuid': uuid,
                }
            status_code = 201
        except Exception as err:
            resp_data = {
                    'success': False,
                    'msg': point_create_fail_msg
                    }
            if isinstance(err, NotUniqueError): 
                # TODO: This needs to recieve implementation-agnostic Exception
                #       I.e., we need Custom Exception classes
                resp_data['reason'] = 'Given name already exists'
                status_code = 409
            else:
                resp_data['reason'] = str(err)
                status_code = 400
            return resp_data, status_code
        return resp_data, status_code


@point_api.route('/<string:uuid>')
class PointAPI(Resource):

    @point_api.marshal_with(m_point)
    def get(self):
        return Point.objects(uuid=uuid).first()

    @point_api.response(200, point_delete_success_msg)
    @point_api.response(404, point_delete_fail_msg)
    @point_api.marshal_with(m_message)
    def delete(self, uuid):
        # delete from metadata db (mongodb for now)
        point = Point.objects(uuid=uuid)
        if len(point)==0:
            resp_data = {
                    'success': False,
                    'reason': 'UUID Not found: {0}'.format(uuid)
                }
            status_code = 404
        else:
            resp_data = {
                    'success': True
                }
            point.get().delete()
            status_code = 200
        
        # delete from timeseries db (influxdb for now)
        ts.delete_point(uuid)

        return resp_data, status_code

@point_api.param('uuid', 'Unique identifier of point')
@point_api.route('/<string:uuid>/timeseries')
class TimeSeriesAPI(Resource):

    def get(self, uuid):
        """
        Reads the time series data of a point for the requested range

        Parameters:
        "uuid": <point uuid>
        "start_time": <unix timestamp of start time in seconds>
        "end_time": <unix timestamp of end time in seconds>

        Returns (JSON):
        {
            "data":{
                "name": <point uuidi>
                "series": [
                    "columns": [column definitions]
                ]
                "values":[list of point values]
            }
            "success": <True or False>
        }

        """
        start_time = request.args.get('start_time')
        end_time = request.args.get('end_time')
        points = ts.read_ts_data(uuid, start_time, end_time)
        response = dict(responses.success_true)
        response.update({'data': points})
        return response

    def post(self, uuid):
        """
        Parameters:
        {
            "samples": [
                {
                    "time": unix timestamp in seconds
                    "value": value
                },
                { more times and values }
            ]
        }
        Returns:
        {
            "success": <True or False>
            "error": error message
        }
        """
        data = request.get_json(force=True)
        result = ts.write_ts_data(uuid, data)
        if result:
            response = dict(responses.success_true)
        else:
            response = dict(responses.success_false)
            response.update({'error': 'Error occurred when writing to InfluxDB'})

        return response

    @point_api.marshal_with(m_message)
    def delete(self, uuid):
        start_time = request.args.get('start_time')
        end_time = request.args.get('end_time')
        ts.delete_ts_data(uuid, start_time, end_time)
        response = dict(responses.success_true)
        response['uuid'] = uuid
        return response
