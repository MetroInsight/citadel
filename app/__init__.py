
from flask import Flask
from flask_oauthlib.provider import OAuth2Provider
from flask_restplus import Api, Resource

import config
from .models.metadata_interface import metadata_db_init
from .models.ts_data_interface import ts_db_init

oauth = OAuth2Provider()

#Create WSGI application object
app = Flask(__name__)
app.config.from_object(config)
#app.secret_key = 'VerySecretKeyMakeItLongAndKeepItSecret'

metadata_db_init(app)

#Connect to InfluxDB timeseries database
ts_db_init(app)

oauth.init_app(app)


#Register blueprints (i.e. flask template for apps)
from .rest_api import api_blueprint
app.register_blueprint(api_blueprint, url_prefix = '/api')

