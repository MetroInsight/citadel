#run a test server
import pdb
from flask import json, current_app

from citadel import app
from citadel.rest_api import api
from config import *

if __name__ == '__main__':
    with app.app_context():
        print("==========")
        print(json.dumps(api.__schema__))
        print("==========")
    print(app.config)
    app.run(host=CITADEL_HOST, port=CITADEL_PORT, debug=True)
