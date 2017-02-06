#run a test server
from app import app

@app.route("/")
def hello():
    return "Hello World!"

app.run(host='127.0.0.1', port=8080, debug=True)
