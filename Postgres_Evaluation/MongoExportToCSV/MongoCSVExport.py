from collections import OrderedDict

from bson import CodecOptions, SON, json_util
from pymongo import MongoClient
import collections
import sys, json, os, pprint

client = MongoClient(os.environ["MONGODEV_INSTANCE"])
db = client["admin"]
db.authenticate(os.environ["MONGODEV_UNAME"], os.environ["MONGODEV_PASS"])

db = client["eva_testing"]
srcCollHandle = db["variants_hsap_87_87_sample"]

sampleDoc = srcCollHandle.find_one()