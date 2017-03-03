"""
citadel.rest_api.point

This module handles point create, view and delete functions
"""
import sys
from uuid import uuid4
from copy import deepcopy

from flask import request, jsonify
from flask.views import MethodView
#from ..models import Point
from flask_restplus import Api, Resource, Namespace, fields

from . import api
from . import responses
from ..models.metadata import Point
from ..schema.converters import schema_converter
from mongoengine import NotUniqueError

point_api = Namespace('point', description='Operations related to points')
tag = api.model('Tag',{
        fields.String(required=True, description='tag key'): 
        fields.String(required=True, description='tag value')
    })
m_point = api.model('Point',{
        'uuid': fields.String(description='Unique identifier of point'),
        'tags': fields.List(fields.Nested(tag)),
        'name': fields.String(
            description='Unique human readable identifier of point')
    })

parser = point_api.parser()
parser.add_argument('name', required=True)

m_message = {
        'msg': fields.String(),
        'reason': fields.String(),
        'uuid': fields.String()
        }

point_created_msg = 'Point created'
point_create_fail_msg = 'Failed to create point'


@point_api.route('/')
class PointListAPI(Resource):

    @point_api.response(200, 'Points found')
    @point_api.marshal_list_with(m_point)
    def get(self):
        return list(Point.objects)

    @point_api.response(201, point_created_msg)
    @point_api.response(409, point_create_fail_msg)
    #@point_api.expect(m_point)
    @point_api.marshal_with(m_message)
    def post(self):
        """Creates a point"""
        data = request.json
        point_name = data['name']
        tags = data['tags']
        uuid = str(uuid4())
        try:
            normalized_tags = schema_converter(tags)
        except KeyError as err:
            resp_data = {
                    'msg': point_create_fail_msg,
                    'reason': 'Not matched to the schema: ' + str(err)
                    }
            status_code = 409
            return resp_data, 409
        try:
            res = Point(name=point_name, uuid=uuid, tags=normalized_tags).save()
            resp_data = {
                'msg': point_created_msg,
                'reason': '',
                'uuid': uuid,
                }
            status_code = 201
        except Exception as err:
            resp_data = {
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
    def get(self, id):
        return Point.objects(uuid=uuid).first()
