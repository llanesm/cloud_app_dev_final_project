from google.cloud import datastore
from flask import request, render_template, Flask
import requests

from authlib.integrations.flask_client import OAuth

from blueprints import boat, load, owner
from configuration import constants

app = Flask(__name__)
app.register_blueprint(boat.bp)
app.register_blueprint(load.bp)
app.register_blueprint(owner.bp)
app.secret_key = 'SECRET_KEY'

client = datastore.Client()

oauth = OAuth(app)

auth0 = oauth.register(
    'auth0',
    client_id=constants.CLIENT_ID,
    client_secret=constants.CLIENT_SECRET,
    api_base_url="https://" + constants.DOMAIN,
    access_token_url="https://" + constants.DOMAIN + "/oauth/token",
    authorize_url="https://" + constants.DOMAIN + "/authorize",
    client_kwargs={
        'scope': 'openid profile email',
    },
)


@app.route('/')
def index():
    return render_template('welcome.html')


@app.route('/login', methods=['POST'])
def login_user():
    body = {'grant_type': 'password',
            'username': request.values['login_email'],
            'password': request.values['login_password'],
            'client_id': constants.CLIENT_ID,
            'client_secret': constants.CLIENT_SECRET
            }
    headers = {'content-type': 'application/json'}
    url = 'https://' + constants.DOMAIN + '/oauth/token'
    res = requests.post(url, json=body, headers=headers).json()
    return render_template('user_info.html', jwt=res['id_token'])


if __name__ == '__main__':
    app.run(host='localhost', port=8080, debug=True)
