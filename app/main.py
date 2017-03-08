#run a test server
import pdb
from flask import json, current_app

from citadel import app
from citadel.rest_api import api
from config import *
import argparse

if __name__ == '__main__':
    debug = True
    host = CITADEL_HOST
    port = CITADEL_PORT

    parser = argparse.ArgumentParser(description='Run Citadel web service.')
    parser.add_argument('-host', dest='host', default=host,
        help='ip bind address (default: ' + host + ')')
    parser.add_argument('-p', dest='port', type=int, default=port,
        help='port to listen on (default: ' + str(port) + ')')

    args = parser.parse_args()

    app.run(args.host, args.port, debug)
