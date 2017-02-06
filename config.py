#config parameters for flask
#TODO: change to class structure for supporting multiple dev environments

#Connection parameters for MongoDB
MONGODB_DATABASE = 'metroinsight'
MONGODB_HOST = '127.0.0.1'
MONGODB_PORT = 27017

#Connection parameters for InfluxDB
INFLUXDB_DATABASE = 'metroinsight'
INFLUXDB_HOST = '127.0.0.1'
INFLUXDB_PORT = 8086
#TODO: change to less privileged user
INFLUXDB_USERNAME = 'root'
INFLUXDB_PASSWORD = 'root'

SECRET_KEY = "MetroInsight_Secret_Key_Make_This_Complicated"
TOKEN_EXPIRATION = 3600

NAME = 'citadel'

#Use for development only
DEBUG = True
