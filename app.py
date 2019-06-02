from flask import Flask, request, Response, abort, jsonify, json
import requests
import re
import sys
import os
import time
import hashlib

DICTIONARY = {}

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
def getNodeShardId():
    global current_shard
    data={"message":"Shard ID of the node retrieved successfully", 
        "shard-id":current_shard}
    return app.response_class(response=json.dumps(data),status=200,mimetype='application/json')

@app.route('/key-value-store-shard/shard-ids', methods=['GET'])
def shardIds():
    global SHARDS
    data = {"message":"Shard IDs retrieved successfully",
            "shard-ids":",".join([str(x) for x in SHARDS.keys()])}
    return app.response_class(response=json.dumps(data),status=200,mimetype='application/json')    

@app.route('/key-value-store-shard/shard-id-members/<id>', methods=['GET'])
def shardMembers(id):
    global SHARDS
    data = {"message":"Members of shard ID retrieved successfully",
            "shard-id-members":",".join(getNodesInShard(int(id)))}
    return app.response_class(response=json.dumps(data), status=200,mimetype='application/json')    

@app.route('/key-value-store-shard/shard-id-key-count/<shardid>',methods = ['GET'])
def keyCount(shardid):
    data = {"message":"Key count of shard ID retrieved successfully",
        "shard-id-key-count":len(getNodesInShard(int(shardid)))}
    return app.response_class(response=json.dumps(data), status=200,mimetype='application/json')    

@app.route('/key-value-store-shard/add-member/<socket>', methods = ['PUT'])
def addNodeToShards(socket):
    global SHARDS
    SHARDS[int(socket)].append(request.get_json()['socket-address'])
    broadcastShardOverwrite()

@app.route('/key-value-store-shard/reshard', methods=['PUT'])
def reshard():
    global current_shard
    global SHARDS
    global SHARD_COUNT
    global SOCKET
    global REPLICAS
    dict = request.get_json()
    if int(dict['shard-count'])*2 > len(REPLICAS):
        data = {"message": 'Not enough nodes to provide fault-tolerance with the given shard count!'}
        response = app.response_class(response=json.dumps(
            data), status=400, mimetype='application/json')
        return response
    else:
        SHARDS={}
        SHARD_COUNT=dict['shard-count']
        rIndex = 0
        for i in range(1,dict['shard-count']+1):
            SHARDS[i] = []
            while len(SHARDS[i]) < 2:
                SHARDS[i].append(REPLICAS[rIndex])
                rIndex += 1
        sIndex = 1
        while rIndex < len(REPLICAS):
            SHARDS[sIndex].append(REPLICAS[rIndex])
            rIndex += 1
            sIndex = (sIndex+1) % SHARD_COUNT

    for node in REPLICAS:
        URL = 'http://' + node + '/replace-shard-view/'
        try:
            requests.put(url=URL, json={'shard-dict':SHARDS})
        except requests.exceptions.ConnectionError:
            delView(node)
    print('RESHARDED DICT:',SHARDS,file=sys.stderr)
    broadcastShardOverwrite()

###################### Shard Broadcast Receiving ######################
@app.route('/replace-shard-view/', methods=['PUT']) 
def replaceShardView():
    global SHARDS
    print(request.get_json(),file=sys.stderr)
    SHARDS = request.get_json()['shard-dict']
    return app.response_class(response=json.dumps(
            {'accepted':'true'}), status=200, mimetype='application/json')

@app.route('/request-kvs/', methods=['GET'])
def requestKvs():
    global DICTIONARY
    data = {"kvs": DICTIONARY, "vl": versionlist}
    response = app.response_class(response=json.dumps(
        data), status=200, mimetype='application/json')
    return response

@app.route('/request-shard-view/',methods=['GET'])
def requestShardView():
    global SHARDS
    data = {"shards": SHARDS}
    response = app.response_class(response=json.dumps(
        data), status=200, mimetype='application/json')
    return response

@app.route('/shard-broadcast-receive/',methods=['PUT'])
def putNodeInShard(socket):
    global SHARDS
    

###################### Shard Helper Functions ######################
def broadcastShardOverwrite():
    global SHARDS
    data = {"shard-dict":SHARDS}
    for node in REPLICAS:
        try:
            URL = 'http://' + node + '/replace-shard-view/'
            requests.put(url=URL,data=json.dumps(data))
        except requests.exceptions.ConnectionError:
            delView(node)

def getShardID(value):
    global SHARD_COUNT
    hash = hashlib.md5()
    hash.update(value.encode('utf-8'))
    return (int(hash.hexdigest(), 16)%SHARD_COUNT + 1)

def getNodesInShard(id):
    global SHARDS
    print(SHARDS, id, file=sys.stderr)
    return SHARDS[id]

def addNodesBalanced(repl):
    global current_shard
    minindex = 0
    minval = 0
    for i in range(1, SHARD_COUNT+1):
        if len(SHARDS[i]) < 2:
            SHARDS[i].append(repl)
            if repl == SOCKET:
                current_shard = i
            return
        else:
            if minval > len(SHARDS[i]):
                minindex = i
                minval = len(SHARDS[i])
    SHARDS[i].append(repl)
    if repl == SOCKET:
        current_shard = i

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
            requests.get(url=URL, timeout=5)
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

    data = {"message": "Replica deleted successfully from the view"}
    response = app.response_class(response=json.dumps(
        data), status=200, mimetype='application/json')
    return response


@app.route('/key-value-store-view/', methods=['PUT'])
def putView():
    global REPLICAS
    global SOCKET
    global DICTIONARY
    socket = request.get_json()
    socket = socket['socket-address']
    if socket in REPLICAS:
        data = {"error": "Socket address already exists in the view",
                "message": "Error in PUT", "dict": DICTIONARY, "vl": versionlist}
        response = app.response_class(response=json.dumps(
            data), status=404, mimetype='application/json')
        return response
    for repl in REPLICAS:
        URL = 'http://' + repl + '/view-broadcast-receive/'+socket
        try:
            # print(url,file=sys.stderr)
            requests.put(url=URL)
            # print(repl,file=sys.stderr)
        except requests.exceptions.ConnectionError:
            print(repl, 'is dead', file=sys.stderr)
    
    data = {"message": "Replica added successfully to the view",
            "dict": DICTIONARY, "vl": versionlist}
    response = app.response_class(response=json.dumps(
        data), status=200, mimetype='application/json')
    return response
#######################################################################

###################### Receiving view broadcasts ######################
@app.route('/view-broadcast-receive/<socket>', methods=['DELETE'])
def delSelfView(socket):
    global REPLICAS
    global SHARDS
    if socket not in REPLICAS:
        data = {"error": "Socket address does not exist in the view",
                "message": "Error in DELETE"}
        response = app.response_class(response=json.dumps(
            data), status=404, mimetype='application/json')
        return response
    
    REPLICAS.remove(socket)

    #Returning Response
    data = {"message": "Replica deleted successfully from the view"}

    response = app.response_class(response=json.dumps(
        data), status=200, mimetype='application/json')
    return response


@app.route('/view-broadcast-receive/<socket>', methods=['PUT'])
def putSelfView(socket):
    global REPLICAS
    if socket in REPLICAS:
        data = {"error": "Socket address already exists in the view",
                "message": "Error in PUT"}
        response = app.response_class(response=json.dumps(
            data), status=404, mimetype='application/json')
        return response

    REPLICAS.append(socket)

    data = {"message": "Replica successfully added to view"}
    response = app.response_class(response=json.dumps(
        data), status=200, mimetype='application/json')
    return response

######################## Key Value Store Ops ##########################

@app.route('/key-value-store/<key>', methods=['GET'])
def get(key):
    global DICTIONARY
    global versionlist
    response = ""
    shard_id = getShardID(key)  #determines the shard-id of key
    if shard_id != current_shard:                   #not current node's shard-id
        response = forward_request(key, shard_id)
        return response
    else:
        if key not in DICTIONARY:
            response = broadcast_request(key, shard_id)
            return response
        else:
            keyData = DICTIONARY[key]
            if isinstance(keyData[2], list):
                causal_meta = list_to_string(keyData[2])
            else:
                causal_meta = str(keyData[1])

            data = {"doesExist": True,
                    "message": "Retrieved successfully", "version": str(keyData[1]), "causal-metadata":
                        causal_meta,  "value": keyData[0], "shard-id": shard_id}
            response = app.response_class(response=json.dumps(
                data), status=200, mimetype='application/json')
            return response


@app.route('/key-value-store/<key>', methods=['PUT'])
def put(key):
    global DICTIONARY
    global versionlist
    global REPLICAS
    response = ""
    value = get_value()
    causal_meta = get_causal_meta()
    shard_id = getShardID(key)  #determines the shard-id of key
    if shard_id != current_shard:                   #not current node's shard-id
        response = forward_request(key, shard_id)
        return response
    else:
        # Becuase it is empty, the replica knows that the request is not causally dependent
        # on any other PUT operation. Therefore, it generates unique version <V1> for the
        # PUT operation, stores the key, value, the version, and corresponding causal metadata
        # (empty in this case)
        if causal_meta == "": #If you are putting the first message
            keyData = [value,1,""]  # individual key
            DICTIONARY[key] = keyData

            broadcast_request(key, shard_id)
            versionlist.append(1)
            data = {"message": "Added successfully",
                "version": "1", "causal-metadata": "1", "shard-id": shard_id}
            response = app.response_class(response=json.dumps(
                data), status=201, mimetype='application/json')
            return response
        else:
            try:
                holdThread(causal_meta)
                if key in DICTIONARY:
                    message = "Updated successfully"
                    status = 200
                else:
                    message = "Added successfully"
                    status = 201

                keyData = [value, versionlist[-1]+1,versionlist]  # individual key
                DICTIONARY[key] = keyData
                versionlist.append(versionlist[-1]+1)

                broadcast_request(key, shard_id)

                data = {"message": message, "version": str(
                    versionlist[-1]), "causal-metadata": list_to_string(versionlist), "shard-id": shard_id}
                response = app.response_class(response=json.dumps(
                    data), status=status, mimetype='application/json')
                return response
            except:
                while not DICTIONARY:
                    onStart()
                return put(key)

@app.route('/key-value-store/<key>', methods=['DELETE'])
def delete(key):
    global DICTIONARY
    global versionlist
    global current_shard
    response = ""
    causal_meta = get_causal_meta()
    shard_id = getShardID(key)  #determines the shard-id of key
    if shard_id != current_shard:                   #not current node's shard-id
        response = forward_request(key, shard_id)
        return response
    else:
        holdThread(causal_meta)
        if key in DICTIONARY:
            keyData = ["",versionlist[-1]+1,versionlist]  # individual key
            DICTIONARY[key] = keyData
            versionlist.append(versionlist[-1]+1)

            broadcast_request(key, shard_id)

            data = {"message": "Deleted successfully", "version": str(
                versionlist[-1]), "causal-metadata": list_to_string(versionlist), "shard-id": current_shard}
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
    global versionlist
    while list_to_string(versionlist) != causal_meta:
        time.sleep(0.5)


################## Key Value Store Broadcast Receving ##################
@app.route('/kvs-broadcast-receive/<key>', methods=['GET'])
def broadcastget(key):
    global DICTIONARY
    global versionlist
    response = ""
    if key not in DICTIONARY:
        data = {"doesExist": False, "error": "Key does not exist",
                "message": "Error in GET"}
        response = app.response_class(response=json.dumps(
            data), status=404, mimetype='application/json')
        return response

    else:
        keyData = DICTIONARY[key]
        if isinstance(keyData[2], list):
            causal_meta = list_to_string(keyData[2])
        else:
            causal_meta = str(keyData[1])

        data = {"doesExist": True,
                "message": "Retrieved successfully", "version": str(keyData[1]), "causal-metadata":
                    causal_meta,  "value": keyData[0], "shard-id": current_shard}
        response = app.response_class(response=json.dumps(
            data), status=200, mimetype='application/json')
        return response


@app.route('/kvs-broadcast-receive/<key>', methods=['PUT'])
def broadcastput(key):
    global DICTIONARY
    global versionlist
    response = ""
    value = get_value()
    causal_meta = get_causal_meta()

    if causal_meta == "":

        keyData = [value,1,""]  # individual key
        DICTIONARY[key] = keyData
        versionlist.append(1)

        data = {"message": "Replicated successfully", "version": "1"}
        response = app.response_class(response=json.dumps(
            data), status=200, mimetype='application/json')
        return response
    else:
        if list_to_string(versionlist) == causal_meta:

            keyData = [value, versionlist[-1]+1,versionlist]  # individual key
            DICTIONARY[key] = keyData
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
    global DICTIONARY
    global versionlist
    response = ""
    causal_meta = get_causal_meta()

    if list_to_string(versionlist) == causal_meta:
        if key in DICTIONARY:
            keyData = ["",versionlist[-1]+1,versionlist]  # individual key
            DICTIONARY[key] = keyData
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

def onStart():
    global REPLICAS
    global SOCKET
    global SHARDS
    global DICTIONARY
    global versionlist
    global current_shard
    if REPLICAS:
        for repl in REPLICAS:
            if repl != SOCKET:
                try:
                    URL = 'http://' + repl + '/key-value-store-view/'
                    request = requests.put(
                        url=URL, data=json.dumps({'socket-address': SOCKET}), timeout=5)

                    URL = 'http://' + repl + '/request-shard-view/'
                    request = requests.get(url=URL, timeout=5).json()
                    for shardId, nodes in request['shards'].items():
                        SHARDS[int(shardId)] = nodes
                    break
                except requests.exceptions.ConnectionError:
                    print(repl, 'failed to respond to view put request', file=sys.stderr)
    
    if not SHARDS:
        print('First online node, creating shards',file=sys.stderr)
        for i in range(1,SHARD_COUNT+1):
            SHARDS[i] = []
        for repl in REPLICAS:
            addNodesBalanced(repl)
    else:
        for shardId, nodes in SHARDS.items():
            if SOCKET in nodes:
                current_shard = shardId

    for node in getNodesInShard(current_shard):
        try:
            URL = 'http://' + node + '/request-kvs/'
            request = requests.get(url=URL, timeout=5).json()
            DICTIONARY = request['kvs']
            versionlist = request['vl']
            break
        except requests.exceptions.ConnectionError:
            print(node,'failed to respond to kvs request',file=sys.stderr)

    print('-------------------------------------',file=sys.stderr)
    print('--- Container Started ---',file=sys.stderr)
    print('Current Shard:', current_shard,file=sys.stderr)
    print('Shard Mapping:',SHARDS, file=sys.stderr)
    print('Dictionary:', DICTIONARY, file=sys.stderr)
    print('-------------------------------------\n',file=sys.stderr)

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

def forward_request(key, shard_id):
    global SHARDS
    nodes = getNodesInShard(shard_id)
    for repl in nodes:
        URL = 'http://' + repl + '/key-value-store/' + key
        print(URL, file=sys.stderr)
        try:
            resp = requests.request(
                method=request.method,
                url=URL,
                headers={key: value for (key, value)
                in request.headers if key != 'Host'},
                data=request.get_data(),
                timeout=5,
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

def broadcast_request(key, shard_id):
    nodes = getNodesInShard(shard_id)
    print(nodes, file=sys.stderr)
    for repl in nodes:
        if repl != SOCKET:
            URL = 'http://' + repl + '/kvs-broadcast-receive/' + key
            print(URL, file=sys.stderr)
            try:
                resp = requests.request(
                    method=request.method,
                    url=URL,
                    headers={key: value for (key, value)
                             in request.headers if key != 'Host'},
                    data=request.get_data(),
                    timeout=5,
                    allow_redirects=False)

                excluded_headers = ['content-encoding',
                                    'content-length', 'transfer-encoding', 'connection']
                headers = [(name, value) for (name, value) in resp.raw.headers.items()
                           if name.lower() not in excluded_headers]

                response = Response(resp.content, resp.status_code, headers)
                if request.method == "GET":
                    return response

            # handles error if main instance is not running
            except requests.exceptions.ConnectionError:
                print(repl, 'is dead', file=sys.stderr)
                delView(repl)


if __name__ == '__main__':
    onStart()
    app.run(debug=False, host='0.0.0.0', port=8080)
