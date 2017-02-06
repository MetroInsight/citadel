"""
citadel.rest_api.point

This module handles point create, view and delete functions
"""

from flask import request, jsonify
from flask.views import MethodView
from . import responses
from ..models import Point
from uuid import uuid4
from flask_restplus import Api, Resource, Namespace, fields
import sys
from . import api

ns = Namespace('point', description='Operations related to points')

tag = api.model('Tag',{
        'key': fields.String(required=True, description='tag key'),
        'value': fields.String(required=True, description='tag value')
    })

point = api.model('Point',{
        'name': fields.String(required=True, description='Name of the point'),
        'uuid': fields.String(description='Unique identifier of point'),
        'tags': fields.List(fields.Nested(tag))
    })

parser = ns.parser()
parser.add_argument('name', required=True)


@ns.route('/')
class PointListAPI(Resource):
    @ns.marshal_list_with(point)
    def get(self):
        return list(Point.objects)

    @ns.response(201, "Point created")
    @ns.expect(point)
    def post(self):
        """
        Creates a point
        """
        data = request.json
        point_name = data['name']
        tags = data['tags']
        uuid=str(uuid4())
        res = Point(name = point_name, uuid = uuid, tags=tags).save() 
        return uuid

@ns.route('/<string:id>')
class PointAPI(Resource):
    @ns.marshal_with(point)
    def get(self, id):
        return Point.objects(uuid=id).first()

    

if __name__ == '__main__':
    app.run(debug=True)
