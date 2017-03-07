#run a test server
from citadel import app
from config import *
import argparse

@app.route("/")
def hello():
    return "Hello World!"

#app.run(host='0.0.0.0', port=80, debug=True)
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
