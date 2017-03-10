# config parameters for flask
# TODO: change to class structure for supporting multiple dev environments

# Citadel Configuration
CITADEL_HOST = '0.0.0.0'
#CITADEL_HOST = '137.110.160.125'
CITADEL_PORT = 8080
SECRET_KEY = "MetroInsight_Secret_Key_Make_This_Complicated"
TOKEN_EXPIRATION = 3600
SERVER_NAME = 'citadel.ucsd.edu:8080'
#SWAGGER_VALIDATOR_URL = '0.0.0.0:5000'


# Metadata DB Configuration 
METADATA_DB_TYPE = 'mongodb'

# Connection parameters for MongoDB
MONGODB_DATABASE = 'citadel'
MONGODB_HOST = '127.0.0.1'
MONGODB_PORT = 27017

# Timeseries DB Configuration
TIMESERIES_DB_TYPE = 'influxdb'

## Connection parameters for InfluxDB
INFLUXDB_DATABASE = 'citadel'
INFLUXDB_HOST = '127.0.0.1'
INFLUXDB_PORT = 8086
# TODO: change to less privileged user
INFLUXDB_USERNAME = 'citadel'
INFLUXDB_PASSWORD = 'citadel'

# Use for development only
DEBUG = True
