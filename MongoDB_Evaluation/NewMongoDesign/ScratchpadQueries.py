from collections import OrderedDict
from commonpyutils import guiutils
from bson import CodecOptions, SON, json_util
from pymongo import MongoClient
import pymongo
import collections, datetime, random
import sys, json, os, pprint

mongoDevClient = MongoClient(os.environ["MONGODEV_INSTANCE"])
mongoDevDBHandle = mongoDevClient["admin"]
mongoDevDBHandle.authenticate(os.environ["MONGODEV_UNAME"], os.environ["MONGODEV_PASS"])
mongoDevDBHandle = mongoDevClient["eva_testing"]

mongoProdClient = MongoClient(guiutils.promptGUIInput("Host", "Host"))
mongoProdUname = guiutils.promptGUIInput("User", "User")
mongoProdPwd = guiutils.promptGUIInput("Pass", "Pass", "*")
mongoProdDBHandle = mongoProdClient["admin"]
mongoProdDBHandle.authenticate(mongoProdUname, mongoProdPwd)
mongoProdDBHandle = mongoProdClient["eva_hsapiens_grch37"]

mongoProdCollHandle = mongoProdDBHandle["variants_1_1"]
mongoProdCollHandle_2 = mongoProdDBHandle["variants_1_2"]

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
        query = {"$and":[{"chr":"X"},{"start": {"$gt": startFirstPos}},{"start": {"$lte": startFirstPos + step}},{"end": {"$gt": startFirstPos}}, {"end": {"$lte": endLastPos}},
                                                          {"files.samp.def": "0|0"}
                                                          ]}
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