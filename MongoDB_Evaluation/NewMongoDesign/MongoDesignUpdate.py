#########################
# Update to flip the "def" key into a value in the "files.samp" sub-document in Variant documents
#########################

from collections import OrderedDict

from bson import CodecOptions, SON, json_util
from pymongo import MongoClient
import collections
import sys, json, os, pprint

client = MongoClient(os.environ["MONGODEV_INSTANCE"])
db = client["admin"]
db.authenticate(os.environ["MONGODEV_UNAME"], os.environ["MONGODEV_PASS"])

db = client["eva_testing"]
srcCollHandle = db["variants_hsap_87_87"]
destCollHandle = db["variants_hsap_87_87_mod"]

docHandles = srcCollHandle.find().limit(100)
for docHandle in docHandles:
    filesDocIndex = 0
    for fileSubDoc in docHandle["files"]:
        print(docHandle["_id"])
        sampleDoc = fileSubDoc["samp"]
        if "def" in sampleDoc:
            sampleDoc[sampleDoc["def"]] = "def"
            sampleDoc.pop("def", None)
            fileSubDoc["samp"] = sampleDoc
            docHandle["files"][filesDocIndex] = fileSubDoc
            print("Inserting document: {0}".format(docHandle["_id"]))
            destCollHandle.insert(docHandle)
            #srcCollHandle.find_and_modify(query={"_id": docHandle["_id"]},update={"$set": {"files.{0}.samp".format(filesDocIndex): sampleDoc}})
        filesDocIndex += 1