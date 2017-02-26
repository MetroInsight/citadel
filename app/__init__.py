from flask import Flask
import config
#from mongoengine import connect, register_connection
from flask_oauthlib.provider import OAuth2Provider
from influxdb import InfluxDBClient
from flask_restplus import Api, Resource
from models.metadata_interface import metadata_initialization

oauth = OAuth2Provider()

#Create WSGI application object
app = Flask(__name__)
app.config.from_object(config)
#app.secret_key = 'VerySecretKeyMakeItLongAndKeepItSecret'

metadata_initialization(app)

oauth.init_app(app)

#Connect to InfluxDB timeseries database
timeseriesdb = InfluxDBClient(
        host=app.config['INFLUXDB_HOST'],
        port=app.config['INFLUXDB_PORT'],
        username=app.config['INFLUXDB_USERNAME'],
        password=app.config['INFLUXDB_PASSWORD'],
        database=app.config['INFLUXDB_DATABASE']
    )

#Register blueprints (i.e. flask template for apps)
from .rest_api import api_blueprint
app.register_blueprint(api_blueprint, url_prefix = '/api')

