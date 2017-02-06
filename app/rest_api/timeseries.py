from flask import request, jsonify
from . import responses
from .. import timeseriesdb
from .helper import jsonString, timestamp_to_time_string
import sys, time, influxdb
from flask_restplus import Api, Resource, Namespace, fields
import time

ns = Namespace('timeseries', description='Operations related to timeseries')

@ns.route('/<string:uuid>')
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

        query_string = 'select * from "%s"' % uuid #influxdb only take double quotes in query
        print(query_string) 
        start_time = request.args.get('start_time')
        end_time = request.args.get('end_time')
        if not start_time:
            query_string += "order by time desc limit 1"
        elif not end_time:
            query_string += "where time > " + str(start_time)
        else:
            query_string += " where time > "+str(start_time)+" and time < " + str(end_time)
        #start_time = timestamp_to_time_string(float(start_time))
        #end_time = timestamp_to_time_string(float(end_time))

        #query_string = "select * from "+ uuid +" where time > '"+start_time+"' and time < '" + end_time + "'"
        data = timeseriesdb.query(query_string)
        response = dict(responses.success_true)
        response.update({'data': data.raw})
        return response

    def post(self, uuid):
        """
        Parameters:
        {
            "samples": [
                {
                    "time": unix timestamp in seconds
                    "value": value
                },
                { more times and values }
            ]
        }
        Returns:
        {
            "success": <True or False>
            "error": error message
        }
        """
        points = []
        data = request.json
        for sample in data['samples']:
            data_dict = {
                    'measurement': uuid,
                    'time': sample['time'],
                    'fields':{
                            'value': sample['value']         
                        }
                }
            points.append(data_dict)
        
            
            result = timeseriesdb.write_points(points, time_precision='s')
            response = dict(responses.success_true)
        else:
            response = dict(responses.success_false)
            response.update({'error': 'Error occurred when writing to InfluxDB'})

        return response
