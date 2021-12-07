import datetime

from flask import Blueprint, request, make_response
from google.cloud import datastore
import json
from configuration import constants
from json2html import json2html

client = datastore.Client()

bp = Blueprint('load', __name__, url_prefix='/loads')


@bp.route('', methods=['POST', 'GET'])
def loads_get_post():
    if request.method == 'POST':
        if request.mimetype != constants.JSON_TYPE:
            return {"Error": "Accepted media type: " + constants.JSON_TYPE}, 415

        content = request.get_json()

        # check for invalid input
        if content.get(constants.VOLUME) is None or content.get(constants.CONTENT) is None:
            return {"Error": "The request object is missing at least one of the required attributes"}, 400

        creation_date = datetime.datetime.now()
        new_load = datastore.entity.Entity(key=client.key(constants.LOADS))
        new_load.update({constants.VOLUME: content[constants.VOLUME], constants.CONTENT: content[constants.CONTENT],
                         constants.CREATION_DATE: creation_date, constants.CARRIER: None})

        client.put(new_load)
        content["id"] = new_load.key.id
        content[constants.SELF] = request.base_url + "/" + str(new_load.key.id)
        content[constants.CARRIER] = None
        content[constants.CREATION_DATE] = str(creation_date)
        res = make_response(json.dumps(content))
        res.mimetype = constants.JSON_TYPE
        res.status_code = 201
        return res

    elif request.method == 'GET':
        query = client.query(kind=constants.LOADS)
        q_limit = int(request.args.get('limit', '5'))
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
            e['id'] = e.key.id
            e[constants.SELF] = request.base_url
            e[constants.CREATION_DATE] = str(e[constants.CREATION_DATE])
            if e[constants.CARRIER] is not None:
                e[constants.CARRIER] = {
                    'id': e[constants.CARRIER],
                    constants.SELF: request.url_root + constants.BOATS + '/' + str(e[constants.CARRIER])
                }
        output = {constants.LOADS: results}
        if next_url:
            output['next'] = next_url
        return json.dumps(output)

    res = make_response('Method not allowed')
    res.status_code = 405
    res.headers.set('Allow', 'POST, GET')
    return res


@bp.route('/<load_id>', methods=['DELETE', 'GET', 'PATCH', 'PUT'])
def loads_get_delete_update(load_id):
    if request.method == 'DELETE':
        load_key = client.key(constants.LOADS, int(load_id))
        load = client.get(key=load_key)

        if load is None:
            return {"Error": "No load with this load_id exists"}, 404

        if load[constants.CARRIER] is not None:
            boat_key = client.key(constants.BOATS, load[constants.CARRIER])
            boat = client.get(key=boat_key)
            boat[constants.LOADS].remove(load.key.id)
            client.put(boat)

        client.delete(load_key)
        return '', 204

    elif request.method == 'GET':
        load_key = client.key(constants.LOADS, int(load_id))
        load = client.get(key=load_key)
        if load is None:
            return {"Error": "No load with this load_id exists"}, 404
        res = {
            "id": load.key.id,
            constants.VOLUME: load[constants.VOLUME],
            constants.CONTENT: load[constants.CONTENT],
            constants.CREATION_DATE: str(load[constants.CREATION_DATE]),
            constants.CARRIER: load[constants.CARRIER],
            constants.SELF: request.base_url
        }

        if load[constants.CARRIER] is not None:
            res[constants.CARRIER] = {
                'id': load[constants.CARRIER],
                constants.SELF: request.url_root + constants.BOATS + '/' + str(load[constants.CARRIER])
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

        return {"Error": "Client must accept either " + constants.JSON_TYPE + " or " + constants.HTML_TYPE}, 406

    elif request.method == 'PUT':
        if request.mimetype != 'application/json':
            return {"Error": "Accepted media type: " + constants.JSON_TYPE}, 415
        if constants.JSON_TYPE not in request.accept_mimetypes:
            return {"Error": "Client must accept media type" + constants.JSON_TYPE}, 406

        # check body for errors
        content = request.get_json()
        if content.get(constants.VOLUME) is None or content.get(constants.CONTENT) is None:
            return {"Error": "The request object is missing at least one of the required attributes"}, 400

        # fetch and validate load
        load_key = client.key(constants.LOADS, int(load_id))
        load = client.get(key=load_key)
        if load is None:
            return {"Error": "The specified load does not exist"}, 404

        load.update({constants.VOLUME: content[constants.VOLUME], constants.CONTENT: content[constants.CONTENT]})
        client.put(load)

        res = {
            "id": load.key.id,
            constants.VOLUME: load[constants.VOLUME],
            constants.CONTENT: load[constants.CONTENT],
            constants.CREATION_DATE: load[constants.CREATION_DATE],
            constants.CARRIER: load[constants.CARRIER],
            constants.SELF: request.base_url
        }
        res = make_response(json.dumps(res))
        res.mimetype = constants.JSON_TYPE
        res.content_location = request.base_url
        res.default_status, res.status_code = 303, 303
        return res

    elif request.method == 'PATCH':
        if request.mimetype != constants.JSON_TYPE:
            return {"Error": "Accepted media type: " + constants.JSON_TYPE}, 415
        if constants.JSON_TYPE not in request.accept_mimetypes:
            return {"Error": "Client must accept media type" + constants.JSON_TYPE}, 406

        content = request.get_json()

        load_key = client.key(constants.LOADS, int(load_id))
        load = client.get(key=load_key)

        # check for invalid id
        if load is None:
            return {"Error": "No load with this load_id exists"}, 404

        update_body = {}
        if content.get(constants.VOLUME) is not None:
            update_body[constants.VOLUME] = content[constants.VOLUME]
        if content.get(constants.CONTENT) is not None:
            update_body[constants.CONTENT] = content[constants.CONTENT]
        load.update(update_body)
        client.put(load)

        res = {
            "id": load.key.id,
            constants.NAME: load[constants.NAME],
            constants.TYPE: load[constants.TYPE],
            constants.LENGTH: load[constants.LENGTH],
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
