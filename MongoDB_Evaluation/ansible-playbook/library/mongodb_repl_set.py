#!/usr/bin/python

from ansible.module_utils.basic import AnsibleModule
import pymongo
import bson
import os
from bson.json_util import dumps

def mongo_repl_initiated(db):
    replStatus = {}
    try:
        replStatus = db.command("replSetGetStatus")
    except pymongo.errors.OperationFailure:
        return False, replStatus
    return replStatus[u'ok'] == 1.0, replStatus

def mongo_repl_set_initiate(data):
    replSetMembers, port = data["replSetMembers"], data["port"]
    replSetMembers = replSetMembers.split(",")    
    replSetMember = replSetMembers[0]
    client = pymongo.MongoClient('mongodb://{0}:{1}/'.format(replSetMember, port))
    db = client.admin    
    
    already_initiated, result = mongo_repl_initiated(db)
    if already_initiated:
        return False, False, result
    
    members = [{"_id": i, "host": "mongoconfig-{0}:27019".format(i+1)} for i in range(len(replSetMembers))]
    config = {"_id": "configReplSet", "configsvr": True, "members": members}    
    replSetInitiateResult = db.command("replSetInitiate", config)
    if replSetInitiateResult[u'ok'] != 1.0:
        return True, False, replSetInitiateResult    
    replSetInitiateResult = mongo_repl_initiated(db)
    client.close()

    return False, True, replSetInitiateResult
    


def main():
    module_args = dict(state=dict(type='str', required=True),replSetMembers=dict(type='str', required=True), port=dict(type='int', required=True))
    choice_map = {"initiated": mongo_repl_set_initiate}
    module = AnsibleModule(argument_spec=module_args)
    is_error, has_changed, result = choice_map.get(module.params['state'])(module.params)

    if not is_error:
        module.exit_json(msg="Already initiated",changed=has_changed, meta=dumps(result))
    else:
        module.fail_json(msg="Error initiating MongoDB replica set", meta=dumps(result))

if __name__ == '__main__':
    main()