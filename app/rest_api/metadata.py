from flask import request
from flask_restplus import Api, Resource, Namespace, fields

ns = Namespace('metadata', description='Operations related to metadata')