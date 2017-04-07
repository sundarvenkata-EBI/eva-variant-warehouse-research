# Look at variant collection scan performance with and without MARGIN
from collections import OrderedDict
from commonpyutils import guiutils
from bson import CodecOptions, SON, json_util
from pymongo import MongoClient
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
cumulativeExecTime = 0
minChromPos= 6000000
maxChromPos = 100000000
margin = 1000000
for i in range(0,numRuns):
    # Use random positions for each run to avoid caching effects
    pos = random.randint(minChromPos, maxChromPos)
    startTime = datetime.datetime.now()
    marginScanResultList = list(mongoProdCollHandle.find({"chr":"10", "ref": "T", "alt":"C", "start": {"$gt": pos - margin},"start": {"$lte": pos + margin}, "end": {"$gte": pos}, "end": {"$lt": pos + margin + margin}}).limit(1000))
    endTime = datetime.datetime.now()
    cumulativeExecTime += (endTime - startTime).total_seconds()
print("Average Execution Time with Margin Scan for variants_1_1: {0}".format (str(cumulativeExecTime/numRuns)))


cumulativeExecTime = 0
for i in range(0,numRuns):
    pos = random.randint(minChromPos, maxChromPos)
    startTime = datetime.datetime.now()
    nonMarginScanResultList = list(mongoProdCollHandle.find({"chr":"10", "ref": "T", "alt":"C", "start": {"$gte": pos},"start": {"$lte": pos + margin}, "end": {"$gte": pos}, "end": {"$lte": pos + margin}}).limit(1000))
    endTime = datetime.datetime.now()
    cumulativeExecTime += (endTime - startTime).total_seconds()
print("Average Execution Time with No Margin Scan for variants_1_1: {0}".format (str(cumulativeExecTime/numRuns)))


numRuns = 30
cumulativeExecTime = 0
minChromPos= 20000000
maxChromPos = 100000000
margin = 1000000
for i in range(0,numRuns):
    # Use random positions for each run to avoid caching effects
    pos = random.randint(minChromPos, maxChromPos)
    startTime = datetime.datetime.now()
    marginScanResultList = list(mongoProdCollHandle_2.find({"chr":"15", "ref": "T", "alt":"C", "start": {"$gt": pos - margin},"start": {"$lte": pos + margin}, "end": {"$gte": pos}, "end": {"$lt": pos + margin + margin}}).limit(1000))
    endTime = datetime.datetime.now()
    cumulativeExecTime += (endTime - startTime).total_seconds()
print("Average Execution Time with Margin Scan for variants_1_2: {0}".format (str(cumulativeExecTime/numRuns)))


cumulativeExecTime = 0
for i in range(0,numRuns):
    pos = random.randint(minChromPos, maxChromPos)
    startTime = datetime.datetime.now()
    nonMarginScanResultList = list(mongoProdCollHandle_2.find({"chr":"15", "ref": "T", "alt":"C", "start": {"$gte": pos},"start": {"$lte": pos + margin}, "end": {"$gte": pos}, "end": {"$lte": pos + margin}}).limit(1000))
    endTime = datetime.datetime.now()
    cumulativeExecTime += (endTime - startTime).total_seconds()
print("Average Execution Time with No Margin Scan for variants_1_2: {0}".format (str(cumulativeExecTime/numRuns)))
