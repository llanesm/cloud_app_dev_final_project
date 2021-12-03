from google.cloud import datastore
from flask import Flask
import boat
import load

app = Flask(__name__)
client = datastore.Client()
app.register_blueprint(boat.bp)
app.register_blueprint(load.bp)


@app.route('/')
def index():
    return "Please navigate to /boats or /loads to use this API"


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)
