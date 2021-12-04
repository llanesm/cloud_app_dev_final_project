from flask import Blueprint, request
from google.cloud import datastore
import json
import constants

client = datastore.Client()

bp = Blueprint('boat', __name__, url_prefix='/boats')


@bp.route('', methods=['POST', 'GET'])
def boats_get_post():
    if request.method == 'POST':
        content = request.get_json()

        # check for invalid input
        if content.get(constants.name) is None or content.get(constants.type) is None or content.get(
                constants.length) is None:
            return {"Error": "The request object is missing at least one of the required attributes"}, 400

        new_boat = datastore.entity.Entity(key=client.key(constants.boats))
        new_boat.update({constants.name: content[constants.name], constants.type: content[constants.type],
                         constants.length: int(content[constants.length]), constants.loads: []})
        client.put(new_boat)
        content["id"] = new_boat.key.id
        content[constants.loads] = []
        content["self"] = request.base_url + "/" + str(new_boat.key.id)
        return json.dumps(content, indent=2, sort_keys=True), 201

    elif request.method == 'GET':
        query = client.query(kind=constants.boats)
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
        output = {constants.boats: results}
        if next_url:
            output["next"] = next_url
        return json.dumps(output)

    return 'Method not recognized'


@bp.route('/<boat_id>', methods=['DELETE', 'GET'])
def boats_get_delete(boat_id):
    if request.method == 'DELETE':
        boat_key = client.key(constants.boats, int(boat_id))
        boat = client.get(key=boat_key)
        if boat is None:
            return {"Error": "No boat with this boat_id exists"}, 404

        # remove load from boat
        for load_id in boat["loads"]:
            load_key = client.key(constants.loads, load_id)
            load = client.get(key=load_key)
            load.update({constants.carrier: None})
            client.put(load)

        client.delete(boat_key)
        return '', 204

    elif request.method == 'GET':
        boat_key = client.key(constants.boats, int(boat_id))
        boat = client.get(key=boat_key)
        if boat is None:
            return {"Error": "No boat with this boat_id exists"}, 404
        loads_display = []
        for load_id in boat[constants.loads]:
            loads_display.append({
                "id": load_id,
                "self": request.url_root + 'loads/' + str(load_id)
            })
        results = {
            "id": boat.key.id,
            constants.name: boat[constants.name],
            constants.type: boat[constants.type],
            constants.length: boat[constants.length],
            constants.loads: loads_display,
            "self": request.base_url
        }
        return results, 200


@bp.route('/<boat_id>/loads', methods=['GET'])
def boats_loads(boat_id):
    if request.method == 'GET':
        boat_key = client.key(constants.boats, int(boat_id))
        boat = client.get(key=boat_key)
        if boat is None:
            return {"Error": "No boat with this boat_id exists"}, 404

        # gather all the loads in the boat
        results = {"loads": []}
        for load_id in boat[constants.loads]:
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

            results["loads"].append({
                "id": str(load.key.id),
                constants.volume: load[constants.volume],
                constants.content: load[constants.content],
                constants.creation_date: load[constants.creation_date],
                constants.carrier: carrier,
                "self": request.url_root + constants.loads + '/' + str(load.key.id)
            })

        return results, 200

    return 'Method not recognized'


@bp.route('/<boat_id>/loads/<load_id>', methods=['PUT', 'DELETE'])
def boats_assigned_removed_loads(boat_id, load_id):
    if request.method == 'PUT':
        # retrieve resources
        load_key = client.key(constants.loads, int(load_id))
        boat_key = client.key(constants.boats, int(boat_id))
        load = client.get(key=load_key)
        boat = client.get(key=boat_key)

        # error check
        if load is None or boat is None:
            return {"Error": "The specified boat and/or slip does not exist"}, 404
        elif load[constants.carrier] is not None:
            return {"Error": "The load is already assigned to a boat"}, 403

        # do the thing
        boat[constants.loads].append(load.key.id)
        boat.update({constants.loads: boat[constants.loads]})
        load.update({constants.carrier: boat.key.id})
        client.put(boat)
        client.put(load)
        return '', 204

    elif request.method == 'DELETE':
        # retrieve resources
        load_key = client.key(constants.loads, int(load_id))
        boat_key = client.key(constants.boats, int(boat_id))
        load = client.get(key=load_key)
        boat = client.get(key=boat_key)

        # error check
        if load is None or boat is None or load[constants.carrier] != boat.key.id:
            return {"Error": "No load with this load_id is on the boat with this boat_id"}, 404

        # do the thing
        boat[constants.loads].remove(load.key.id)
        boat.update({constants.loads: boat[constants.loads]})
        load.update({constants.carrier: None})
        client.put(load)
        client.put(boat)
        return '', 204

    return 'Method not recognized'
