import time
import json

from flask import request

def jsonString(obj, pretty=False):
    """
    Creates a json object. If pretty is specified as True,
    proper formatter is added
    Parameters:
        obj: object that needs to be converted to a json object
        pretty: Boolean specifying whether json object has to be formatted
    Returns:
        converted JSON object
    """
    if pretty == True:
        return json.dumps(obj, sort_keys=True, indent=4, separators=(',', ': '))+ '\n'
    else:
        return json.dumps(obj)

def timestamp_to_time_string(t):
    """
    Converts a unix timestamp to a string representation of the timestamp
    Params:
        t: A unix timestamp float
    Returns:
        A string representation of time
    """
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(t)) + str(t - int(t))[1:10] + 'Z'
