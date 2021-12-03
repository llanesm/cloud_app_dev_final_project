import datetime
from flask import Blueprint, request
from google.cloud import datastore
import json
import constants

client = datastore.Client()

bp = Blueprint('load', __name__, url_prefix='/loads')


@bp.route('', methods=['POST', 'GET'])
def loads_get_post():
    if request.method == 'POST':
        content = request.get_json()

        # check for invalid input
        if content.get(constants.volume) is None or content.get(constants.content) is None:
            return {"Error": "The request object is missing at least one of the required attributes"}, 400
        print(datetime.date.today())
        new_load = datastore.entity.Entity(key=client.key(constants.loads))
        new_load.update({constants.volume: content[constants.volume], constants.content: content[constants.content],
                         constants.creation_date: datetime.datetime.now(), constants.carrier: None})
        client.put(new_load)
        content["id"] = new_load.key.id
        content[constants.carrier] = new_load[constants.carrier]
        content[constants.creation_date] = str(new_load[constants.creation_date])
        content["self"] = request.base_url + "/" + str(new_load.key.id)
        return json.dumps(content, indent=2, sort_keys=True), 201

    elif request.method == 'GET':
        query = client.query(kind=constants.loads)
        q_limit = int(request.args.get('limit', '3'))
        q_offset = int(request.args.get('offset', '0'))
        g_iterator = query.fetch(limit=q_limit, offset=q_offset)
        pages = g_iterator.pages
        results = list(next(pages))
        if g_iterator.next_page_token:
            next_offset = q_offset + q_limit
            next_url = request.base_url + "?limit=" + str(q_limit) + "&offset=" + str(next_offset)
        else:
            next_url = None
        for e in results:
            e["id"] = e.key.id
            e["self"] = request.base_url + "/" + str(e.key.id)
        output = {constants.loads: results}
        if next_url:
            output["next"] = next_url
        return output

    return 'Method not recognized'


@bp.route('/<load_id>', methods=['DELETE', 'GET'])
def loads_get_delete(load_id):
    if request.method == 'GET':
        load_key = client.key(constants.loads, int(load_id))
        load = client.get(key=load_key)
        if load is None:
            return {"Error": "No load with this load_id exists"}, 404
        if load[constants.carrier] is not None:
            carrier = {
                "id": load[constants.carrier],
                "self": request.url_root + constants.boats + '/' + str(load[constants.carrier])
            }
        else:
            carrier = None

        results = {
            "id": str(load.key.id),
            constants.volume: load[constants.volume],
            constants.content: load[constants.content],
            constants.creation_date: load[constants.creation_date],
            constants.carrier: carrier,
            "self": request.base_url
        }
        return results, 200

    elif request.method == 'DELETE':
        load_key = client.key(constants.loads, int(load_id))
        load = client.get(key=load_key)
        if load is None:
            return {"Error": "No load with this load_id exists"}, 404

        # remove load from boat
        if load[constants.carrier] is not None:
            boat_key = client.key(constants.boats, load[constants.carrier])
            boat = client.get(key=boat_key)
            boat[constants.loads].remove(int(load_id))
            boat.update({constants.loads: boat[constants.loads]})
            client.put(boat)

        client.delete(load_key)
        return '', 204

    return 'Method not recognized'
