#run a test server
from citadel import app

@app.route("/")
def hello():
    return "Hello World!"

#app.run(host='0.0.0.0', port=80, debug=True)
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=True)
