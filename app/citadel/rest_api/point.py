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

# API Models
# ALL FIELD SHOULD HAVE AN EXAMPLE AT LEAST FOR DOCUMENTATION

def model_to_json(m):
    d = dict()
    for key,val in m.items():
        d[key] = val.example
    return json.dumps(d)

point_api = Namespace('point', description='Operations related to points')

m_geo_point = point_api.model('GeoPoint', {
    'coordinates': fields.List(
        fields.Float, min_items=2, max_items=2, example=[0.1234, -0.1234]),
    'type': fields.String(example='Point')
    },
    example={'coordinates':[32.881679, -117.233344],'type':'Point'},
    description='Geological point with latitude and longitude')

""" comment out for possible future purpose 
m_geo_point_list = point_api.model('GeoPointList', {
    'geo_point_list': fields.List(fields.Nested(m_geo_point)),
    'geo_type': fields.String()
    },
    example={'geo_point_list':
                [{'geo_point':[32.881679, -117.233344]}], 'geo_type': 'point'
            },
    description='Geological representation which could be either a point, \
            a line, or a countour'
    )
"""


m_point_post = point_api.model('PointPost',{
    'tags': fields.Raw(
        example={'tag1':'value1', 'tag2':'value2'},
        description='key, value pairs in dictionary format'),
    'name': fields.String(
        description='Unique human readable identifier of point',
        example='point_name_1'),
    'geometry': fields.Nested(m_geo_point, example=model_to_json(m_geo_point))
    },
    description='Representation of a data point',
    example={
        'tags': {
            'tag1': 'value1',
            'tag2': 'value2'
            },
        'name': 'user_defined_name1',
        'geometry': {
            'coordinates':[32.881679, -117.233344],
            'type': 'point'
            }
        }
    )

m_point = point_api.inherit('Point', m_point_post, {
    'uuid': fields.String(
        description='Unique identifier of point',
        example='random_uuid_1')
    },
    )
m_point.example = dict(list(json.loads(model_to_json(m_point_post)).items())\
                  +[('uuid','random_uuid_1')])

m_point_list = point_api.model('PointList', {
    'point_list': fields.List(fields.Nested(m_point),
        example=[model_to_json(m_point)])
    })

m_message = point_api.model('Message',{
    'success': fields.Boolean(example=True),
    'reason': fields.String(example='reason_string'),
    'uuid': fields.String(example='random_uuid_1')
    })

m_timeseries = point_api.model('Timeseries',{
    'success': fields.Boolean(example=True)
    })

m_timeseries_post = point_api.model('TimeseriesPost', {
    'samples': fields.Raw(
    description='Dictionary where key=timestamp integer \
            and value=data value',
    example={'timestamp1':'value1', 'timestampe2':'value2'}
    )
    })


point_create_success_msg = 'Point created'
point_create_fail_msg = 'Failed to create point'
point_delete_success_msg = 'Point deleted'
point_delete_fail_msg = 'Failed to delete point'

influxdb_time_format = "2009-11-10T23:00:00Z"

point_query_parser = point_api.parser()
point_query_parser.add_argument('query', type=str, location='args',
        help=model_to_json(m_point_post)
        )
point_query_parser.add_argument('geo_query', type=str, location='args')
# TODO: Can this be more specified to have certain JSON structure in the str?

@point_api.doc()
@point_api.route('/')
class PointGenericAPI(Resource):

#    @point_api.doc(body=m_point)
    @point_api.expect(point_query_parser)
    @point_api.response(200, 'Points found', m_point)
    @point_api.marshal_list_with(m_point_list)
    def get(self):
        """ Query to points """
        args = point_query_parser.parse_args()
        query_str = args.get('query')

        if query_str:
            tag_query = json.loads(query_str)
        else:
            tag_query = {}
            
        geo_query_str = args.get('geo_query')
        if geo_query_str:
            geo_query = json.loads(geo_query_str)
            if geo_query['type']=='bounding_box':
                west_south = geo_query['geometry_list'][0]
                east_north = geo_query['geometry_list'][1]
                query_result = Point.objects(\
                        __raw__=tag_query,\
                        geometry__geo_within_box=[west_south, east_north])
        else:
            query_result = Point.objects(__raw__=tag_query)

        return {'point_list': query_result}

    @point_api.doc(body=m_point_post)
    @point_api.response(201, point_create_success_msg)
    @point_api.response(409, point_create_fail_msg)
    @point_api.marshal_with(m_message)
    def post(self):
        """ 
        Creates a point
        """
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

        # Currently only geo_point type is supported
        # TODO: Extend this to include line, shape, etc.
        if data['geometry']['type'].lower() == 'point':
            lat = data['geometry']['coordinates'][0]
            lng = data['geometry']['coordinates'][1]
        else:
            raise Exception

        try:
            res = Point(
                    name=point_name, 
                    uuid=uuid, 
                    tags=normalized_tags, 
                    geometry=[lng, lat]
                    ).save()
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


@point_api.route('/<string:uuid>')
class PointAPI(Resource):

    @point_api.marshal_with(m_point)
    def get(self):
        """ Get metadata of a point with given UUID """
        return Point.objects(uuid=uuid).first()

    @point_api.response(200, point_delete_success_msg)
    @point_api.response(404, point_delete_fail_msg)
    @point_api.marshal_with(m_message)
    def delete(self, uuid):
        """ Deletes a point with given UUID """
        
        # delete from timeseries db (influxdb for now)
        try:
            ts.delete_point(uuid)
        except:
            pass

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

    @point_api.doc(body=m_timeseries_post)
    def post(self, uuid):
        """ Posts timeseries data of a point """
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
        """
        Deletes timeseries data of a point in the requested time range.
        """
        start_time = request.args.get('start_time')
        end_time = request.args.get('end_time')
        ts.delete_ts_data(uuid, start_time, end_time)
        response = dict(responses.success_true)
        response['uuid'] = uuid
        return response
