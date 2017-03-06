#run a test server
from citadel import app
from config import *

@app.route("/")
def hello():
    return "Hello World!"

#app.run(host='0.0.0.0', port=80, debug=True)
if __name__ == '__main__':
    app.run(host=CITADEL_HOST, port=CITADEL_PORT, debug=True)
