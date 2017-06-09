import datetime
import multiprocessing
import traceback, random, pymongo
from multiprocessing import Process
from commonpyutils import guiutils
from pymongo import MongoClient
from pyspark import SparkConf, SparkContext

def variantSampleJoin(devMongoHost, (mongoProdHost, mongoProdUser, mongoProdPwd), chromosome, lowerBound, upperBound, filesCacheLookup):
    devMongoClient = MongoClient(devMongoHost)
    prodClient = MongoClient(mongoProdHost)
    prodMongoHandle = prodClient["admin"]
    prodMongoHandle.authenticate(mongoProdUser, mongoProdPwd)

    prodMongoHandle = prodClient["eva_hsapiens_grch37"]
    variantCollHandle = prodMongoHandle["variants_1_2"]
    sampleCollHandle = devMongoClient["eva_testing"]["sample_enc"]

    numScans = 0
    totalNumRecords = 0
    mongoCumulativeExecTime = 0
    margin = 1000000
    print("Start Time for multi-scan:{0}".format(datetime.datetime.now()))
    pos = random.randint(lowerBound, upperBound)
    # Proxy Query
    step = 20000
    startFirstPos = pos
    startLastPos = pos + margin
    endFirstPos = pos
    endLastPos = pos + margin + margin
    while (True):

        startTime = datetime.datetime.now()
        varQuery = {"chr": chromosome, "start": {"$gt": startFirstPos, "$lte": startFirstPos + step},
                 "end": {"$gte": startFirstPos, "$lt": endLastPos}}
        varResultList = list(variantCollHandle.find(varQuery,{"files":0}).sort([("chr", pymongo.ASCENDING), ("start", pymongo.ASCENDING)]))
        numVarRecords = len(varResultList)

        sampleQuery = {"chr": chromosome, "start": {"$gt": startFirstPos, "$lte": startFirstPos + step},
                    "end": {"$gte": startFirstPos, "$lt": endLastPos}}
        sampleResultList = list(sampleCollHandle.find(sampleQuery).sort([("chr", pymongo.ASCENDING), ("start", pymongo.ASCENDING)]))
        numSampleRecords = len(sampleResultList)

        sampleResultIndex = 0
        for varResult in varResultList:
            sampleResult = sampleResultList[sampleResultIndex]
            if varResult["_id"] == sampleResult["_id"]:
                varResult["sample"] = sampleResult["files"]
                sampleResultIndex += 1
                if sampleResultIndex >= numSampleRecords: break

        endTime = datetime.datetime.now()
        duration = (endTime - startTime).total_seconds()
        mongoCumulativeExecTime += duration
        print("Mongo: Joined {0} records in {1} seconds".format(numVarRecords, duration))
        print("****************")
        startFirstPos += step
        endFirstPos += step
        totalNumRecords += numVarRecords
        numScans += 1
        if (startFirstPos >= startLastPos) or (endFirstPos >= endLastPos): break
    devMongoClient.close()
    prodClient.close()
    return (totalNumRecords, mongoCumulativeExecTime)


mongoProdHost = guiutils.promptGUIInput("MongoDB Production Host:", "MongoDB Production Host:")
mongoProdUser = guiutils.promptGUIInput("MongoDB Production User:", "MongoDB Production User:")
mongoProdPwd = guiutils.promptGUIInput("MongoDB Production Password:", "MongoDB Production Password:", "*")

prodClient = MongoClient(mongoProdHost)
prodMongoHandle = prodClient["admin"]
prodMongoHandle.authenticate(mongoProdUser, mongoProdPwd)
prodMongoHandle = prodClient["eva_hsapiens_grch37"]
filesCollHandle_grch37 = prodMongoHandle["files_1_2"]
filesCache = list(filesCollHandle_grch37.find())
filesCacheLookup = {}
for doc in filesCache:
    filesCacheLookup[doc["fid"] + "_" + doc["sid"]] = doc
prodClient.close()

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
entry = chromosome_LB_UB_Map[0]
variantSampleJoin("172.22.69.141", (mongoProdHost, mongoProdUser, mongoProdPwd), entry["_id"], entry["minStart"], entry["maxStart"], filesCacheLookup)

conf = SparkConf().setMaster("spark://172.22.69.141:7077").setAppName("MongoTest")
sc = SparkContext(conf=conf)
sc.setLogLevel("INFO")
chromosome_entries = sc.parallelize(chromosome_LB_UB_Map[0:10], 10)
chromosome_entries.map(lambda entry: variantSampleJoin("172.22.69.141", (mongoProdHost, mongoProdUser, mongoProdPwd), entry["_id"], entry["minStart"], entry["maxStart"], filesCacheLookup)).collect()
sc.stop()
