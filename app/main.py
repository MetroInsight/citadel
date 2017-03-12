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
    debug = False
    host = CITADEL_HOST
    port = CITADEL_PORT
    server_name = SERVER_NAME

    parser = argparse.ArgumentParser(description='Run Citadel web service.')
    parser.add_argument('-d', dest='debug', action='store_true',
        help='turn on debugging mode. WARNING: insecure - do not use on public machine.')
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

    # enable threading so requests for static content don't take hang
    app.run(args.host, args.port, args.debug, threaded=True)
