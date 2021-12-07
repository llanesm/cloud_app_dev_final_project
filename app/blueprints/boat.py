from flask import Blueprint, request, make_response
from google.cloud import datastore
import json
from configuration import constants
from json2html import json2html

from util.verify import verify_jwt, AuthError

client = datastore.Client()

bp = Blueprint('boat', __name__, url_prefix='/boats')


@bp.route('', methods=['POST', 'GET'])
def boats_get_post():
    if request.method == 'POST':
        if request.mimetype != constants.JSON_TYPE:
            return {'Error': 'Accepted media type: ' + constants.JSON_TYPE}, 415

        content = request.get_json()

        try:
            payload = verify_jwt(request)
        except AuthError:
            return {'Error': 'Invalid JWT'}, 401

        # check for invalid input
        if content.get(constants.NAME) is None or content.get(constants.TYPE) is None or content.get(
                constants.LENGTH) is None:
            return {'Error': 'The request object is missing at least one of the required attributes'}, 400

        if boat_name_exists(content.get(constants.NAME)):
            return {'Error': 'Boat with that name already exists'}, 403

        new_boat = datastore.entity.Entity(key=client.key(constants.BOATS))
        new_boat.update({constants.NAME: content[constants.NAME], constants.TYPE: content[constants.TYPE],
                         constants.LENGTH: int(content[constants.LENGTH]), constants.OWNER: payload['sub'],
                         constants.LOADS: []})
        client.put(new_boat)
        content['id'] = new_boat.key.id
        content[constants.SELF] = request.base_url + '/' + str(new_boat.key.id)
        content[constants.LOADS] = []
        res = make_response(json.dumps(content))
        res.mimetype = constants.JSON_TYPE
        res.status_code = 201
        return res

    elif request.method == 'GET':
        # check for JWT
        try:
            payload = verify_jwt(request)
        except AuthError:
            payload = None

        # form datastore query
        query = client.query(kind=constants.BOATS)
        if payload is not None:
            query.add_filter('owner', '=', payload['sub'])
        else:
            query.add_filter('public', '=', True)

        # paginate query results
        q_limit = int(request.args.get('limit', '5'))
        q_offset = int(request.args.get('offset', '0'))
        g_iterator = query.fetch(limit=q_limit, offset=q_offset)
        pages = g_iterator.pages
        results = list(next(pages))
        if g_iterator.next_page_token:
            next_offset = q_offset + q_limit
            next_url = request.base_url + '?limit=' + str(q_limit) + '&offset=' + str(next_offset)
        else:
            next_url = None

        # format results
        for e in results:
            e['id'] = e.key.id
            e[constants.SELF] = request.base_url + '/' + str(e.key.id)
            loads_display = []
            for load_id in e[constants.LOADS]:
                loads_display.append({
                    'id': load_id,
                    constants.SELF: request.url_root + constants.LOADS + '/' + str(load_id)
                })
            e[constants.LOADS] = loads_display

        output = {constants.BOATS: results}
        if next_url:
            output['next'] = next_url
        return json.dumps(output)

    res = make_response('Method not allowed')
    res.status_code = 405
    res.mimetype = constants.JSON_TYPE
    res.headers.set('Allow', 'POST, GET')
    return res


@bp.route('/<boat_id>', methods=['DELETE', 'GET', 'PATCH', 'PUT'])
def boats_get_delete_update(boat_id):
    if request.method == 'DELETE':
        boat_key = client.key(constants.BOATS, int(boat_id))
        boat = client.get(key=boat_key)

        try:
            payload = verify_jwt(request)
        except AuthError:
            return {'Error': 'Invalid JWT'}, 401

        if boat is None:
            return {'Error': 'No boat with this boat_id exists'}, 404
        if payload['sub'] != boat['owner']:
            return {'Error': 'Boat owned by someone else'}, 403

        for load_id in boat[constants.LOADS]:
            load_key = client.key(constants.LOADS, load_id)
            load = client.get(key=load_key)
            load.update({constants.CARRIER: None})
            client.put(load)

        client.delete(boat_key)
        return '', 204

    elif request.method == 'GET':
        boat_key = client.key(constants.BOATS, int(boat_id))
        boat = client.get(key=boat_key)

        try:
            payload = verify_jwt(request)
        except AuthError:
            return {'Error': 'Invalid JWT'}, 401

        if boat is None:
            return {'Error': 'No boat with this boat_id exists'}, 404
        if payload['sub'] != boat['owner']:
            return {'Error': 'Boat owned by someone else'}, 403

        loads_display = []
        for load_id in boat[constants.LOADS]:
            loads_display.append({
                'id': load_id,
                constants.SELF: request.url_root + constants.LOADS + '/' + str(load_id)
            })
        res = {
            'id': boat.key.id,
            constants.NAME: boat[constants.NAME],
            constants.TYPE: boat[constants.TYPE],
            constants.LENGTH: boat[constants.LENGTH],
            constants.OWNER: boat[constants.OWNER],
            constants.SELF: request.base_url,
            constants.LOADS: loads_display
        }

        if constants.JSON_TYPE in request.accept_mimetypes:
            res = make_response(json.dumps(res))
            res.mimetype = constants.JSON_TYPE
            res.status_code = 200
            return res

        elif constants.HTML_TYPE in request.accept_mimetypes:
            res = make_response(json2html.convert(json=json.dumps(res)))
            res.mimetype = constants.HTML_TYPE
            res.status_code = 200
            return res

        return {'Error': 'Client must accept either ' + constants.JSON_TYPE + ' or ' + constants.HTML_TYPE}, 406

    elif request.method == 'PUT':
        # error check
        if request.mimetype != 'application/json':
            return {'Error': 'Accepted media type: ' + constants.JSON_TYPE}, 415
        if constants.JSON_TYPE not in request.accept_mimetypes:
            return {'Error': 'Client must accept media type' + constants.JSON_TYPE}, 406
        content = request.get_json()
        if content.get(constants.NAME) is None or content.get(constants.TYPE) is None or content.get(
                constants.LENGTH) is None:
            return {'Error': 'The request object is missing at least one of the required attributes'}, 400

        # fetch and validate boat
        boat_key = client.key(constants.BOATS, int(boat_id))
        boat = client.get(key=boat_key)

        try:
            payload = verify_jwt(request)
        except AuthError:
            return {'Error': 'Invalid JWT'}, 401

        if boat is None:
            return {'Error': 'No boat with this boat_id exists'}, 404
        if payload['sub'] != boat['owner']:
            return {'Error': 'Boat owned by someone else'}, 403

        if boat_name_exists(content.get(constants.NAME)) and boat[constants.NAME] != content.get(constants.NAME):
            return {'Error': 'Boat with that name already exists'}, 403

        boat.update({constants.NAME: content[constants.NAME], constants.TYPE: content[constants.TYPE],
                     constants.LENGTH: int(content[constants.LENGTH])})
        client.put(boat)

        res = {
            'id': boat.key.id,
            constants.NAME: boat[constants.NAME],
            constants.TYPE: boat[constants.TYPE],
            constants.LENGTH: boat[constants.LENGTH],
            constants.SELF: request.base_url
        }
        res = make_response(json.dumps(res))
        res.mimetype = constants.JSON_TYPE
        res.content_location = request.base_url
        res.default_status, res.status_code = 303, 303
        return res

    elif request.method == 'PATCH':
        if request.mimetype != constants.JSON_TYPE:
            return {'Error': 'Accepted media type: ' + constants.JSON_TYPE}, 415
        if constants.JSON_TYPE not in request.accept_mimetypes:
            return {'Error': 'Client must accept media type' + constants.JSON_TYPE}, 406

        content = request.get_json()

        boat_key = client.key(constants.BOATS, int(boat_id))
        boat = client.get(key=boat_key)

        try:
            payload = verify_jwt(request)
        except AuthError:
            return {'Error': 'Invalid JWT'}, 401

        # check for invalid id
        if boat is None:
            return {'Error': 'No boat with this boat_id exists'}, 404
        if payload['sub'] != boat['owner']:
            return {'Error': 'Boat owned by someone else'}, 403

        update_body = {}
        if content.get(constants.NAME) is not None:
            if boat_name_exists(content.get(constants.NAME)) and boat[constants.NAME] != content.get(constants.NAME):
                return {'Error': 'Boat with that name already exists'}, 403
            update_body[constants.NAME] = content[constants.NAME]
        if content.get(constants.LENGTH) is not None:
            update_body[constants.LENGTH] = content[constants.LENGTH]
        if content.get(constants.TYPE) is not None:
            update_body[constants.TYPE] = content[constants.TYPE]
        boat.update(update_body)
        client.put(boat)

        res = {
            'id': boat.key.id,
            constants.NAME: boat[constants.NAME],
            constants.TYPE: boat[constants.TYPE],
            constants.LENGTH: boat[constants.LENGTH],
            constants.SELF: request.base_url
        }
        res = make_response(json.dumps(res))
        res.mimetype = constants.JSON_TYPE
        res.status_code = 200
        return res

    res = make_response('Method not allowed')
    res.status_code = 405
    res.headers.set('Allow', 'DELETE, GET, PATCH, PUT')
    return res


@bp.route('/<boat_id>/loads/<load_id>', methods=['PUT', 'DELETE'])
def boats_assigned_removed_loads(boat_id, load_id):
    if request.method == 'PUT':
        # retrieve resources
        load_key = client.key(constants.LOADS, int(load_id))
        boat_key = client.key(constants.BOATS, int(boat_id))
        load = client.get(key=load_key)
        boat = client.get(key=boat_key)

        try:
            payload = verify_jwt(request)
        except AuthError:
            return {'Error': 'Invalid JWT'}, 401

        if load is None or boat is None:
            return {'Error': 'The specified boat and/or load does not exist'}, 404
        if payload['sub'] != boat['owner']:
            return {'Error': 'Boat owned by someone else'}, 403
        elif load[constants.CARRIER] is not None:
            return {'Error': 'The load is already assigned to a boat'}, 403

        # do the thing
        boat[constants.LOADS].append(load.key.id)
        boat.update({constants.LOADS: boat[constants.LOADS]})
        load.update({constants.CARRIER: boat.key.id})
        client.put(boat)
        client.put(load)
        res = make_response('')
        res.mimetype = constants.JSON_TYPE
        res.status_code = 204
        return res

    elif request.method == 'DELETE':
        # retrieve resources
        load_key = client.key(constants.LOADS, int(load_id))
        boat_key = client.key(constants.BOATS, int(boat_id))
        load = client.get(key=load_key)
        boat = client.get(key=boat_key)

        try:
            payload = verify_jwt(request)
        except AuthError:
            return {'Error': 'Invalid JWT'}, 401

        if load is None or boat is None or load[constants.CARRIER] != boat.key.id:
            return {'Error': 'No load with this load_id is on the boat with this boat_id'}, 404
        if payload['sub'] != boat['owner']:
            return {'Error': 'Boat owned by someone else'}, 403

        # do the thing
        boat[constants.LOADS].remove(load.key.id)
        boat.update({constants.LOADS: boat[constants.LOADS]})
        load.update({constants.CARRIER: None})
        client.put(load)
        client.put(boat)
        res = make_response('')
        res.mimetype = constants.JSON_TYPE
        res.status_code = 204
        return res

    res = make_response('Method not allowed')
    res.status_code = 405
    res.headers.set('Allow', 'DELETE, GET, PATCH, PUT')
    return res


def boat_name_exists(boat_name):
    query = client.query(kind=constants.BOATS)
    results = list(query.fetch())
    for e in results:
        if e[constants.NAME] == boat_name:
            return True
    return False
