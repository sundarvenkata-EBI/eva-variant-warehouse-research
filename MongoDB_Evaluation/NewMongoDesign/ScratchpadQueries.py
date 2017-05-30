from collections import OrderedDict
from commonpyutils import guiutils
from bson import CodecOptions, SON, json_util
from pymongo import MongoClient
import pymongo
import collections, datetime, random
import sys, json, os, pprint
import multiprocessing, psycopg2, socket
from multiprocessing import Process, Pipe

mongoDevClient = MongoClient(os.environ["MONGODEV_INSTANCE"])
mongoDevDBHandle = mongoDevClient["admin"]
mongoDevDBHandle.authenticate(os.environ["MONGODEV_UNAME"], os.environ["MONGODEV_PASS"])
mongoDevDBHandle = mongoDevClient["eva_testing"]

mongoProdClient = MongoClient("mongodb://{0}".format(os.environ["MONGO_PROD_INSTANCES"]))
mongoProdUname = guiutils.promptGUIInput("User", "User")
mongoProdPwd = guiutils.promptGUIInput("Pass", "Pass", "*")
mongoProdDBHandle = mongoProdClient["admin"]
mongoProdDBHandle.authenticate(mongoProdUname, mongoProdPwd)
mongoProdDBHandle = mongoProdClient.get_database("eva_hsapiens_grch37", read_preference= pymongo.ReadPreference.SECONDARY_PREFERRED, read_concern=pymongo.read_concern.ReadConcern(level="local"))

mongoProdCollHandle = mongoProdDBHandle["variants_1_1"]
mongoProdCollHandle_2 = mongoProdDBHandle["variants_1_2"]

chromosome_LB_UB_Map = [{ "_id" : "1", "minStart" : 10020, "maxStart" : 249240605, "numEntries" : 12422239 },
{ "_id" : "2", "minStart" : 10133, "maxStart" : 243189190, "numEntries" : 13217397 },
{ "_id" : "3", "minStart" : 60069, "maxStart" : 197962381, "numEntries" : 10891260 },
{ "_id" : "4", "minStart" : 10006, "maxStart" : 191044268, "numEntries" : 10427984 },
{ "_id" : "5", "minStart" : 10043, "maxStart" : 180905164, "numEntries" : 9742153 },
{ "_id" : "6", "minStart" : 61932, "maxStart" : 171054104, "numEntries" : 9340928 },
{ "_id" : "7", "minStart" : 10010, "maxStart" : 159128653, "numEntries" : 8803393 },
{ "_id" : "8", "minStart" : 10059, "maxStart" : 146303974, "numEntries" : 8458842 },
{ "_id" : "9", "minStart" : 10024, "maxStart" : 141153428, "numEntries" : 6749462 },
{ "_id" : "10", "minStart" : 60222, "maxStart" : 135524743, "numEntries" : 7416994 },
{ "_id" : "11", "minStart" : 61248, "maxStart" : 134946509, "numEntries" : 7690584 },
{ "_id" : "12", "minStart" : 60076, "maxStart" : 133841815, "numEntries" : 7347630 },
{ "_id" : "13", "minStart" : 19020013, "maxStart" : 115109865, "numEntries" : 5212835 },
{ "_id" : "14", "minStart" : 19000005, "maxStart" : 107289456, "numEntries" : 4989875 },
{ "_id" : "15", "minStart" : 20000003, "maxStart" : 102521368, "numEntries" : 4607392 },
{ "_id" : "16", "minStart" : 60008, "maxStart" : 90294709, "numEntries" : 5234679 },
{ "_id" : "17", "minStart" : 47, "maxStart" : 81195128, "numEntries" : 4652428 },
{ "_id" : "18", "minStart" : 10005, "maxStart" : 78017157, "numEntries" : 4146560 },
{ "_id" : "19", "minStart" : 60360, "maxStart" : 59118925, "numEntries" : 3821659 },
{ "_id" : "20", "minStart" : 60039, "maxStart" : 62965384, "numEntries" : 3512381 },
{ "_id" : "21", "minStart" : 9411199, "maxStart" : 48119868, "numEntries" : 2082680 },
{ "_id" : "22", "minStart" : 16050036, "maxStart" : 51244515, "numEntries" : 2172028 },
{ "_id" : "X", "minStart" : 60003, "maxStart" : 155260479, "numEntries" : 5893713 },
{ "_id" : "Y", "minStart" : 10003, "maxStart" : 59363485, "numEntries" : 504508 }]

numRuns = 30
minChromPos= 60000
maxChromPos = 100000000
cumulativeExecTime = 0
margin = 1000000
print("Start Time:{0}".format(datetime.datetime.now()))
for i in range(0, numRuns):
    pos = random.randint(minChromPos, maxChromPos)
    # Return all samples with homozygous reference for allele X at position X on chromosome X
    # resultList = list(mongoDevDBHandle["variant_chr21_1_1_sample_mod"].find({"$and":[{"chr":"X"},{"start": {"$gt": pos - margin}},{"start": {"$lte": pos + margin}}, {"end": {"$gte": pos}}, {"end": {"$lt": pos + margin + margin}},
    #                                                       {"files":
    #                                                            {"$elemMatch":
    #                                                                 {"samp.phased": {"$elemMatch": {"gt": "1|1"}}}}}
    #                                                       ]}, {"files.samp.phased.$.si":1}).sort([("chr", pymongo.ASCENDING), ("start", pymongo.ASCENDING)]).limit(1000))
    # Proxy Query
    step = 200000
    startFirstPos = pos - margin
    startLastPos = pos + margin
    endFirstPos = pos
    endLastPos = pos + margin + margin
    while(True):
        startTime = datetime.datetime.now()
        query = {"chr":"1","start": {"$gt": startFirstPos, "$lte": startFirstPos + step}, "end": {"$gt": startFirstPos, "$lte": endLastPos}, "files.samp.def": "0|0"}
        #print(query)
        #raw_input()
        resultList = list(mongoProdCollHandle_2.find(query).sort([("chr", pymongo.ASCENDING), ("start", pymongo.ASCENDING)]))
        startFirstPos += step
        endFirstPos += step
        endTime = datetime.datetime.now()
        duration = (endTime - startTime).total_seconds()
        cumulativeExecTime += duration
        print("Returned {0} records in {1} seconds".format(len(resultList), duration))
        if (startFirstPos >= startLastPos) or (endFirstPos >= endLastPos): break

print("Average Execution time:{0}".format(cumulativeExecTime/numRuns))

numRuns = 30
minChromPos= 60000
maxChromPos = 110000000
cumulativeExecTime = 0
margin = 1000000
for i in range(0, numRuns):
    pos = random.randint(minChromPos, maxChromPos)
    startTime = datetime.datetime.now()
    # Return all variants present in a specific sample at chromosome X and a range
    fileID = mongoDevDBHandle["files_1_1"].find_one({"fname" : "ALL.chr21.phase3_shapeit2_mvncall_integrated_v3plus_nounphased.rsID.genotypes.vcf.gz" })["fid"]
    numericSampleIndex = mongoDevDBHandle["files_1_1"].find_one({"fname" : "ALL.chr21.phase3_shapeit2_mvncall_integrated_v3plus_nounphased.rsID.genotypes.vcf.gz" }
, {"samp.HG00116":1})["samp"]["HG00116"]
    query = {"$and": [{"chr":"21"}, {"start": {"$gte": 9411513}}, {"end": {"$lte": 10411513}}, {"files.fid": }]}

    resultList = list(mongoDevDBHandle["variant_chr21_1_1_sample_mod"].find(
        {"chr": "X", "start": {"$gt": pos - margin}, "start": {"$lte": pos + margin}, "end": {"$gte": pos},
         "end": {"$lt": pos + margin + margin}, "files.samp.0|0": {"$exists": "true"}}).sort(
        [("chr", pymongo.ASCENDING), ("start", pymongo.ASCENDING)]).limit(1000))
    endTime = datetime.datetime.now()
    cumulativeExecTime += ((endTime - startTime).total_seconds())
print("Average Execution time:{0}".format(cumulativeExecTime/numRuns))

def getScanResults(chromosome, startFirstPos, step, endLastPos):
    localMongoProdClient = MongoClient("mongodb://{0}".format(os.environ["MONGO_PROD_INSTANCES"]))
    localMongoProdDBHandle = localMongoProdClient["admin"]
    localMongoProdDBHandle.authenticate(mongoProdUname, mongoProdPwd)
    localMongoProdDBHandle = localMongoProdClient.get_database("eva_hsapiens_grch37",
                                                     read_preference=pymongo.ReadPreference.SECONDARY_PREFERRED,
                                                     read_concern=pymongo.read_concern.ReadConcern(level="local"))
    localMongoProdCollHandle = localMongoProdDBHandle["variants_1_2"]

    query = {"chr": chromosome, "start": {"$gt": startFirstPos, "$lte": startFirstPos + step},
             "end": {"$gte": startFirstPos, "$lt": endLastPos}}
    localStartTime = datetime.datetime.now()
    resultList = list(localMongoProdCollHandle.find(query).sort([("chr", pymongo.ASCENDING), ("start", pymongo.ASCENDING)]))
    localEndTime = datetime.datetime.now()
    localDuration = (localEndTime - localStartTime).total_seconds()
    print("Mongo: Returned {0} records in {1} seconds".format(len(resultList), localDuration))
    print("****************")
    localMongoProdClient.close()
    return resultList

# Execution times for Mongo Multi Scan - Parallel processing
numRuns = 30
citusCumulativeExecTime = 0
mongoCumulativeExecTime = 0
margin = 1000000
numChrom = 24
print("Start Time for multi-scan:{0}".format(datetime.datetime.now()))
for i in range(0, numRuns):
    currChrom = chromosome_LB_UB_Map[random.randint(0, numChrom-1)]
    minChromPos = currChrom["minStart"]
    maxChromPos = currChrom["maxStart"]
    chromosome = currChrom["_id"]
    pos = random.randint(minChromPos, maxChromPos)
    # Proxy Query
    step = 200000
    startFirstPos = max(pos - margin,0)
    startLastPos = pos + margin
    endFirstPos = pos
    endLastPos = pos + margin + margin
    processList = []
    startTime = datetime.datetime.now()
    while(True):
        processList.append(Process(target=getScanResults, args=(chromosome, startFirstPos, step, endLastPos)))
        startFirstPos += step
        endFirstPos += step
        if (startFirstPos >= startLastPos) or (endFirstPos >= endLastPos): break
    for process in processList:
        process.start()
    for process in processList:
        process.join()
    endTime = datetime.datetime.now()
    duration = (endTime - startTime).total_seconds()
    mongoCumulativeExecTime += duration
print("Average Mongo Execution time:{0}".format(mongoCumulativeExecTime/numRuns))