
from flask import Flask
from flask_oauthlib.provider import OAuth2Provider
from flask_restplus import Api, Resource
from mongoengine import *
from influxdb import InfluxDBClient
import config


def metadata_db_init(app):
    connect(app.config['MONGODB_DATABASE'],
                host=app.config['MONGODB_HOST'],
                port=app.config['MONGODB_PORT'])


def timeseries_db_init(app):
    ts_db = InfluxDBClient(
            host=app.config['INFLUXDB_HOST'],
            port=app.config['INFLUXDB_PORT'],
            username=app.config['INFLUXDB_USERNAME'],
            password=app.config['INFLUXDB_PASSWORD'],
            database=app.config['INFLUXDB_DATABASE'],
    )
    return ts_db


oauth = OAuth2Provider()

#Create WSGI application object
app = Flask(__name__)
app.config.from_object(config)
#app.secret_key = 'VerySecretKeyMakeItLongAndKeepItSecret'

metadata_db_init(app)

#Connect to InfluxDB timeseries database
timeseriesdb = timeseries_db_init(app)

oauth.init_app(app)


#Register blueprints (i.e. flask template for apps)
from .rest_api import api_blueprint
app.register_blueprint(api_blueprint, url_prefix = '/api')

