from flask import Flask, request, Response, abort, jsonify, json
import requests
import re
import sys
import os
import time
import threading

dictionary = {}

REPLICAS = os.environ.get('VIEW').split(',')  
SHARDS = {}     #Dict of shard # to list of nodes
SOCKET = os.environ.get('SOCKET_ADDRESS')
SHARD_COUNT = int(os.environ.get('SHARD_COUNT'))
app = Flask(__name__)

current_shard = None
vectorclock = {}
versionlist = []

###################### Shard Operations ######################
@app.route('/key-value-store-shard/node-shard-id', methods=['GET'])
def getShardId():
    global current_shard
    return app.response_class(
        json={
            "message":"Shard ID of the node retrieved successfully", 
            "shard-id":current_shard
        }, 
        status=200
    )

@app.route('/key-value-store-shard/shard-ids', methods=['GET'])
def shardIds():
    global SHARDS
    return app.response_class(
        json={
            "message":"Shard IDs retrieved successfully",
            "shard-ids":",".join(SHARDS.keys())
        },
        status=200
    )    

@app.route('/key-value-store-shard/shard-id-members/<id>', methods=['GET'])
def shardMembers(id):
    global SHARDS
    return app.response_class(
        json={
            "message":"Members of shard ID retrieved successfully",
            "shard-id-members":",".join(getNodesInShard(id))
        },
        status=200
    )    

@app.route('/key-value-store-shard/reshard', methods=['GET'])
def reshard():
    print('Todo')

@app.route('/key-value-store-shard/shard-id-key-count/<shardid>',methods = ['GET'])
def keyCount(shardid):
    return len(getNodesInShard(shardid))
###################### Shard Helper Functions ######################
def getShardID(value): 
    global SHARD_COUNT
    return hash(value)%SHARD_COUNT + 1

def getNodesInShard(id):
    global SHARDS
    return SHARDS[id]

def addNodeToShards(socket):
    global SHARDS
    shard = getShardID(socket)
    if shard not in SHARDS:
        SHARDS[shard] = []
    SHARDS[shard].append(socket)

def removeNodeFromShards(socket):
    SHARDS[getShardID(socket)].remove(socket)

###################### View Operations ######################
@app.route('/key-value-store-view/', methods=['GET'])
def getView():
    global REPLICAS
    global SOCKET
    # out = REPLICAS #this stores the live replicas

    for repl in REPLICAS:  # verify liveness
        # time.sleep(2)
        URL = 'http://' + repl + '/ping/'
        try:
            requests.get(url=URL, timeout=10)
        except requests.exceptions.ConnectionError:
            REPLICAS.remove(repl)
            delView(repl)
    data = {"message": "View retrieved successfully",
            "view": ",".join(REPLICAS)}  # idk lol
    response = app.response_class(response=json.dumps(
        data), status=200, mimetype='application/json')
    return response


@app.route('/key-value-store-view/', methods=['DELETE'])
def delView(socket=None):
    global REPLICAS
    global SOCKET
    global SHARDS
    if socket == None:
        socket = request.get_json()['socket-address']
    if socket not in REPLICAS:
        data = {"error": "Socket address does not exist in the view",
                "message": "Error in DELETE"}
        response = app.response_class(response=json.dumps(
            data), status=404, mimetype='application/json')
        return response
    for repl in REPLICAS:
        URL = 'http://' + repl + '/view-broadcast-receive/'+socket
        try:
            requests.delete(url=URL)
        except requests.exceptions.ConnectionError:
            print(repl, 'is dead', file=sys.stderr)

    if len(getNodesInShard(getShardID(socket))) <= 2:    #Not sure if replica will be removed by now
        reshard()

    data = {"message": "Replica deleted successfully from the view"}
    response = app.response_class(response=json.dumps(
        data), status=200, mimetype='application/json')
    return response


@app.route('/key-value-store-view/', methods=['PUT'])
def putView():
    global REPLICAS
    global SOCKET
    global dictionary
    socket = request.get_json()
    socket = socket['socket-address']
    if socket in REPLICAS:
        print('socket is ', socket, file=sys.stderr)
        print('SOcket exists', file=sys.stderr)
        data = {"error": "Socket address already exists in the view",
                "message": "Error in PUT", "dict": dictionary, "vl": versionlist}
        response = app.response_class(response=json.dumps(
            data), status=404, mimetype='application/json')
        return response
    for repl in REPLICAS:
        URL = 'http://' + repl + '/view-broadcast-receive/'+socket
        print('trying ', URL, file=sys.stderr)
        try:
            # print(url,file=sys.stderr)
            requests.put(url=URL)
            # print(repl,file=sys.stderr)
        except requests.exceptions.ConnectionError:
            print(repl, 'is dead', file=sys.stderr)
    
    data = {"message": "Replica added successfully to the view",
            "dict": dictionary, "vl": versionlist}
    response = app.response_class(response=json.dumps(
        data), status=200, mimetype='application/json')
    return response
#######################################################################

###################### Receiving view broadcasts ######################
@app.route('/view-broadcast-receive/<socket>', methods=['DELETE'])
def delSelfView(socket):
    global REPLICAS
    global SHARDS
    print('Deleting', socket, file=sys.stderr)
    if socket not in REPLICAS:
        data = {"error": "Socket address does not exist in the view",
                "message": "Error in DELETE"}
        response = app.response_class(response=json.dumps(
            data), status=404, mimetype='application/json')
        return response
    
    REPLICAS.remove(socket)
    removeNodeFromShards(socket)

    #Returning Response
    data = {"message": "Replica deleted successfully from the view"}

    response = app.response_class(response=json.dumps(
        data), status=200, mimetype='application/json')
    return response


@app.route('/view-broadcast-receive/<socket>', methods=['PUT'])
def putSelfView(socket):
    global REPLICAS
    print('PUTTING', socket, file=sys.stderr)
    if socket in REPLICAS:
        data = {"error": "Socket address already exists in the view",
                "message": "Error in PUT"}
        response = app.response_class(response=json.dumps(
            data), status=404, mimetype='application/json')
        return response

    REPLICAS.append(socket)
    addNodeToShards(socket)

    data = {"message": "Replica successfully added to view"}
    response = app.response_class(response=json.dumps(
        data), status=200, mimetype='application/json')
    return response

######################## Key Value Store Ops ##########################
@app.route('/key-value-store/<key>', methods=['GET'])
def get(key):
    global dictionary
    global versionlist
    response = ""
    if key not in dictionary:
        response = broadcast_request(key)
        return response
    else:
        keyData = dictionary[key]
        if isinstance(keyData[2], list): 
            causal_meta = list_to_string(keyData[2])
        else:
            causal_meta = str(keyData[1])

        data = {"doesExist": True,
                "message": "Retrieved successfully", "version": str(keyData[1]), "causal-metadata":
                    causal_meta,  "value": keyData[0]}
        response = app.response_class(response=json.dumps(
            data), status=200, mimetype='application/json')
        return response


@app.route('/key-value-store/<key>', methods=['PUT'])
def put(key):
    global dictionary
    global versionlist
    global REPLICAS
    response = ""
    value = get_value()
    causal_meta = get_causal_meta()

    # Becuase it is empty, the replica knows that the request is not causally dependent
    # on any other PUT operation. Therefore, it generates unique version <V1> for the
    # PUT operation, stores the key, value, the version, and corresponding causal metadata
    # (empty in this case)
    if causal_meta == "": #If you are putting the first message
        keyData = [value,1,""]  # individual key
        dictionary[key] = keyData

        broadcast_request(key)
        versionlist.append(1)
        data = {"message": "Added successfully",
                "version": "1", "causal-metadata": "1"}
        response = app.response_class(response=json.dumps(
            data), status=201, mimetype='application/json')
        return response
    else:
        try:
            holdThread(causal_meta)
            if key in dictionary:
                message = "Updated successfully"
                status = 200
            else:
                message = "Added successfully"
                status = 201

            keyData = [value, versionlist[-1]+1,versionlist]  # individual key
            dictionary[key] = keyData
            versionlist.append(versionlist[-1]+1)

            broadcast_request(key)

            data = {"message": message, "version": str(
                versionlist[-1]), "causal-metadata": list_to_string(versionlist)}
            response = app.response_class(response=json.dumps(
                data), status=status, mimetype='application/json')
            return response
        except:
            while not dictionary:
                onStart()
            return put(key)

@app.route('/key-value-store/<key>', methods=['DELETE'])
def delete(key):
    global dictionary
    global versionlist
    response = ""
    causal_meta = get_causal_meta()

    holdThread(causal_meta)
    if key in dictionary:
        keyData = ["",versionlist[-1]+1,versionlist]  # individual key
        dictionary[key] = keyData
        versionlist.append(versionlist[-1]+1)

        broadcast_request(key)

        data = {"message": "Deleted successfully", "version": str(
            versionlist[-1]), "causal-metadata": list_to_string(versionlist)}
        response = app.response_class(response=json.dumps(
            data), status=200, mimetype='application/json')
        return response
    else:
        data = {"DoesExist": False, "error": "key doesn't exist",
                "message": "error in deleting"}
        response = app.response_class(response=json.dumps(
            data), status=201, mimetype='application/json')
        return response

def holdThread(causal_meta):
    while list_to_string(versionlist) != causal_meta:
        time.sleep(0.5)

################## Key Value Store Broadcast Receving ##################
@app.route('/kvs-broadcast-receive/<key>', methods=['GET'])
def broadcastget(key):
    global dictionary
    global versionlist
    response = ""
    if key not in dictionary:
        data = {"doesExist": False, "error": "Key does not exist",
                "message": "Error in GET"}
        response = app.response_class(response=json.dumps(
            data), status=404, mimetype='application/json')
        return response

    else:
        keyData = dictionary[key]
        if isinstance(keyData[2], list):
            causal_meta = list_to_string(keyData[2])
        else:
            causal_meta = str(keyData[1])

        data = {"doesExist": True,
                "message": "Retrieved successfully", "version": str(keyData[1]), "causal-metadata":
                    causal_meta,  "value": keyData[0]}
        response = app.response_class(response=json.dumps(
            data), status=200, mimetype='application/json')
        return response


@app.route('/kvs-broadcast-receive/<key>', methods=['PUT'])
def broadcastput(key):
    global dictionary
    global versionlist
    response = ""
    value = get_value()
    causal_meta = get_causal_meta()

    if causal_meta == "":

        keyData = [value,1,""]  # individual key
        dictionary[key] = keyData
        versionlist.append(1)

        data = {"message": "Replicated successfully", "version": "1"}
        response = app.response_class(response=json.dumps(
            data), status=200, mimetype='application/json')
        return response
    else:
        if list_to_string(versionlist) == causal_meta:

            keyData = [value, versionlist[-1]+1,versionlist]  # individual key
            dictionary[key] = keyData
            versionlist.append(versionlist[-1]+1)

            data = {"message": "Replicated successfully",
                    "version": str(versionlist[-1])}
            response = app.response_class(response=json.dumps(
                data), status=200, mimetype='application/json')
            return response

        else:
            l = list_to_string(versionlist)
            data = {"error": " causal metadata not matching",
                    "version": l, "causal_meta": causal_meta}
            response = app.response_class(response=json.dumps(
                data), status=200, mimetype='application/json')
            return response


@app.route('/kvs-broadcast-receive/<key>', methods=['PUT'])
def broadcastdelete(key):
    global dictionary
    global versionlist
    response = ""
    causal_meta = get_causal_meta()

    if list_to_string(versionlist) == causal_meta:
        if key in dictionary:
            keyData = ["",versionlist[-1]+1,versionlist]  # individual key
            dictionary[key] = keyData
            versionlist.append(versionlist[-1]+1)
            data = {"message": "Deleted successfully", "version": str(
                versionlist[-1]), "causal-metadata": list_to_string(versionlist)}
            response = app.response_class(response=json.dumps(
                data), status=200, mimetype='application/json')
            return response
        else:
            data = {"doesExist": False, "error": "key doesn't exist",
                    "message": "error in deleting"}
            response = app.response_class(response=json.dumps(
                data), status=404, mimetype='application/json')
            return response
    # else
        # wait till previous versions are done

######################### HELPER FUNCTIONS #########################
@app.route('/ping/', methods=['GET'])
def ping():
    return Response("{'Live': 'True'}", status=200, mimetype='application/json')

def list_to_string(_versionlist):
    liststr = ""
    for i in _versionlist[:-1]:
        liststr += str(i) + ","
    liststr += str(_versionlist[-1])
    return liststr


def alertShutdown():
    global SOCKET
    delView(SOCKET)


def onStart():
    global REPLICAS
    global SOCKET
    global dictionary
    global versionlist
    global current_shard
    current_shard = getShardID(SOCKET)
    if REPLICAS:
        for repl in REPLICAS:
            addNodeToShards(repl)
        for repl in REPLICAS:
            if repl != SOCKET:
                time.sleep(1)
                try:
                    URL = 'http://' + repl + '/key-value-store-view/'
                    print("TRYING URL ", URL, file=sys.stderr)
                    request = requests.put(
                        url=URL, json={'socket-address': SOCKET}, timeout=10)
                    print(request, file=sys.stderr)
                    if request.json()['dict']:
                        dictionary = request.json()['dict']
                    if request.json()['vl']:
                        versionlist = request.json()['vl']
                        print(versionlist, file=sys.stderr)

                    break
                except requests.exceptions.ConnectionError:
                    print(repl, 'failed', file=sys.stderr)

def get_ip(address):
    return address.split(":")[0]


def get_sender_ip():
    return request.remote_addr


def get_value():
    jdict = request.get_json()
    value = jdict["value"]
    return value


def data_has_value():
    if request.get_json()['value']:
        return True
    else:
        return False


def get_causal_meta():
    jdict = request.get_json()
    value = jdict["causal-metadata"]
    return value


def data_has_cm():
    if request.get_json()['causal-metadata']:
        return True
    else:
        return False


def get_curr_version():
    global vectorclock
    return vectorclock[SOCKET]


def broadcast_request(key):
    for repl in REPLICAS:
        if repl != SOCKET:
            URL = 'http://' + repl + '/kvs-broadcast-receive/' + key
            try:
                resp = requests.request(
                    method=request.method,
                    url=URL,
                    headers={key: value for (key, value)
                             in request.headers if key != 'Host'},
                    data=request.get_data(),
                    timeout=10,
                    allow_redirects=False)

                excluded_headers = ['content-encoding',
                                    'content-length', 'transfer-encoding', 'connection']
                headers = [(name, value) for (name, value) in resp.raw.headers.items()
                           if name.lower() not in excluded_headers]

                response = Response(resp.content, resp.status_code, headers)
                return response

            # handles error if main instance is not running
            except requests.exceptions.ConnectionError:
                print(repl, 'is dead', file=sys.stderr)
                delView(repl)


def makeKeyData(key, value):
    global dictionary
    keyData = []  # individual key
    keyData.append(value)  # value of key
    keyData.append(versionlist[-1]+1)  # version
    keyData.append(versionlist)  # corresponding causal metadata
    dictionary[key] = keyData


if __name__ == '__main__':
    onStart()
    app.run(debug=False, host='0.0.0.0', port=8080)
