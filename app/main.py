#run a test server
import argparse

import pdb
import json

from citadel import app
from citadel.rest_api import api
from config import *

@app.route("/")
def hello():
    return "Hello Citizens!"


if __name__ == '__main__':
    debug = True
    host = CITADEL_HOST
    port = CITADEL_PORT
    if port!=80:
        server_name = SERVER_NAME + ':' + str(port)
    else:
        server_name = SERVER_NAME

    parser = argparse.ArgumentParser(description='Run Citadel web service.')
    parser.add_argument('-host', dest='host', default=host,
        help='ip bind address (default: ' + host + ')')
    parser.add_argument('-p', dest='port', type=int, default=port,
        help='port to listen on (default: ' + str(port) + ')')

    args = parser.parse_args()

    # set SERVER_NAME for swagger
    app.config['SERVER_NAME'] = server_name

    with app.app_context():
        with open('doc/api/swagger_schema.json', 'w') as fp:
            json.dump(api.__schema__, fp)

    app.run(args.host, args.port, debug)
