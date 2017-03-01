from influxdb import InfluxDBClient

ts_db = None

def ts_db_init(app):
    global ts_db 
    ts_db = InfluxDBClient(
            host=app.config['INFLUXDB_HOST'],
            port=app.config['INFLUXDB_PORT'],
            username=app.config['INFLUXDB_USERNAME'],
            password=app.config['INFLUXDB_PASSWORD'],
            database=app.config['INFLUXDB_DATABASE'],
            
        )

# TODO: Currently interface is implemented in the rest_api/timeseries.py
#       They need to be implemented here and then just called there.
#       Either as a Class or a list of functions.
