from flask import Blueprint, request, make_response
from google.cloud import datastore
import json
from configuration import constants

client = datastore.Client()

bp = Blueprint('owner', __name__, url_prefix='/owners')


@bp.route('', methods=['GET'])
def owners_get_boats():
    if request.method == 'GET':
        query = client.query(kind='owners')
        results = list(query.fetch())
        for e in results:
            e[constants.SELF] = request.base_url + '/' + str(e.key.id)
        return json.dumps(results), 200

    res = make_response('Method not allowed')
    res.status_code = 405
    res.headers.set('Allow', 'GET')
    return res
