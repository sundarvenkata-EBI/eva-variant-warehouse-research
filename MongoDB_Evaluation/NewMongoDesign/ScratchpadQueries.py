from collections import OrderedDict
from commonpyutils import guiutils
from bson import CodecOptions, SON, json_util
from pymongo import MongoClient
import pymongo
import collections, datetime, random
import sys, json, os, pprint

mongoProdClient = MongoClient(guiutils.promptGUIInput("Host", "Host"))
mongoProdUname = guiutils.promptGUIInput("User", "User")
mongoProdPwd = guiutils.promptGUIInput("Pass", "Pass", "*")
mongoProdDBHandle = mongoProdClient["admin"]
mongoProdDBHandle.authenticate(mongoProdUname, mongoProdPwd)
mongoProdDBHandle = mongoProdClient["eva_hsapiens_grch37"]

mongoProdCollHandle = mongoProdDBHandle["variants_1_1"]
mongoProdCollHandle_2 = mongoProdDBHandle["variants_1_2"]

numRuns = 30
minChromPos= 50000000
maxChromPos = 100000000
cumulativeExecTime = 0
margin = 1000000
for i in range(0, numRuns):
    pos = random.randint(minChromPos, maxChromPos)
    startTime = datetime.datetime.now()
    resultList = list(mongoProdCollHandle_2.find({"chr":"15",  "start": {"$gt": pos - margin},"start": {"$lte": pos + margin}, "end": {"$gte": pos}, "end": {"$lt": pos + margin + margin}, "files.samp.0|0": {"$exists": "true"}}).sort([("chr", pymongo.ASCENDING), ("start", pymongo.ASCENDING)]).limit(1000))
    endTime = datetime.datetime.now()
    cumulativeExecTime += ((endTime - startTime).total_seconds())
print("Average Execution time:{0}".format(cumulativeExecTime/numRuns))