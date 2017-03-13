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


def run():

    parser = argparse.ArgumentParser(description='Run Citadel web service.')
    parser.add_argument('-d', dest='debug', action='store_true', default=False,
        help='turn on debugging mode. WARNING: insecure - do not use on public machine.')
    parser.add_argument('-host', dest='host', default=CITADEL_HOST,
        help='ip bind address (default: ' + CITADEL_HOST + ')')
    parser.add_argument('-p', dest='port', type=int, default=CITADEL_PORT,
        help='port to listen on (default: ' + str(CITADEL_PORT) + ')')

    args = parser.parse_args()

    if args.port!=80:
        server_name = SERVER_NAME + ':' + str(args.port)
    else:
        server_name = SERVER_NAME

    # set SERVER_NAME for swagger
    app.config['SERVER_NAME'] = server_name

#    with app.app_context():
#        with open('doc/api/swagger_schema.json', 'w') as fp:
#            json.dump(api.__schema__, fp)

    # enable threading so requests for static content don't hang
    app.run(args.host, args.port, args.debug, threaded=True)

if __name__ == '__main__':
    run()
