import datetime
import random

import getpass
import psycopg2
from commonpyutils import guiutils
import pymongo
from pymongo import MongoClient

# Mongo credentials
mongoProdClient = MongoClient(guiutils.promptGUIInput("Host", "Host"))
mongoProdUname = guiutils.promptGUIInput("User", "User")
mongoProdPwd = guiutils.promptGUIInput("Pass", "Pass", "*")
mongoProdDBHandle = mongoProdClient["admin"]
mongoProdDBHandle.authenticate(mongoProdUname, mongoProdPwd)
mongoProdDBHandle = mongoProdClient["eva_hsapiens_grch37"]
mongoProdCollHandle_2 = mongoProdDBHandle["variants_1_2"]

# Citus credentials
postgresHost = getpass._raw_input("PostgreSQL Host:\n")
postgresUser = getpass._raw_input("PostgreSQL Username:\n")
postgresConnHandle = psycopg2.connect("dbname='postgres' user='{0}' host='{1}' password=''".format(postgresUser, postgresHost))
resultCursor = postgresConnHandle.cursor()

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

# Execution times for Multi Scan
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
    while(True):
        startTime = datetime.datetime.now()
        resultCursor.execute(
            "select * from public_1.variant where chrom = '{0}' and start_pos > {1} and start_pos <= {2} and end_pos >= {3} and end_pos < {4} order by var_id".format(
                chromosome, startFirstPos, startFirstPos + step, startFirstPos, endLastPos))
        resultList = resultCursor.fetchall()
        endTime = datetime.datetime.now()
        duration = (endTime - startTime).total_seconds()
        citusCumulativeExecTime += duration
        print("Citus: Returned {0} records in {1} seconds".format(len(resultList), duration))

        startTime = datetime.datetime.now()
        query = {"chr": chromosome, "start": {"$gt": startFirstPos, "$lte": startFirstPos + step},
                 "end": {"$gte": startFirstPos, "$lt": endLastPos}}
        resultList = list(mongoProdCollHandle_2.find(query, {"_id":1, "chr": 1, "start": 1, "end" : 1, "type": 1, "len": 1, "ref": 1, "alt": 1}).sort([("chr", pymongo.ASCENDING), ("start", pymongo.ASCENDING)]))
        endTime = datetime.datetime.now()
        duration = (endTime - startTime).total_seconds()
        mongoCumulativeExecTime += duration
        print("Mongo: Returned {0} records in {1} seconds".format(len(resultList), duration))
        print("****************")
        startFirstPos += step
        endFirstPos += step
        if (startFirstPos >= startLastPos) or (endFirstPos >= endLastPos): break
print("Average Citus Execution time:{0}".format(citusCumulativeExecTime/numRuns))
print("Average Mongo Execution time:{0}".format(mongoCumulativeExecTime/numRuns))

# Execution times for Single Scan
numRuns = 30
minChromPos= 2000000
maxChromPos = 100000000
citusCumulativeExecTime = 0
mongoCumulativeExecTime = 0
margin = 1000000
chromosome = "1"
print("Start Time for single-scan:{0}".format(datetime.datetime.now()))
for i in range(0, numRuns):
    pos = random.randint(minChromPos, maxChromPos)
    # Proxy Query
    # step = 200000
    startFirstPos = pos - margin
    startLastPos = pos + margin
    endFirstPos = pos
    endLastPos = pos + margin + margin

    startTime = datetime.datetime.now()
    resultCursor.execute(
        "select * from public_1.variant where chrom = '{0}' and start_pos > {1} and start_pos <= {2} and end_pos >= {3} and end_pos < {4} order by var_id".format(
            chromosome, startFirstPos, startLastPos, endFirstPos, endLastPos))
    resultList = resultCursor.fetchall()
    endTime = datetime.datetime.now()
    duration = (endTime - startTime).total_seconds()
    citusCumulativeExecTime += duration
    print("Citus: Returned {0} records in {1} seconds".format(len(resultList), duration))

    startTime = datetime.datetime.now()
    query = {"chr": chromosome, "start": {"$gt": startFirstPos, "$lte": startLastPos},"end": {"$gte": endFirstPos, "$lt": endLastPos}}
    resultList = list(mongoProdCollHandle_2.find(query, {"_id":1, "chr": 1, "start": 1, "end" : 1, "type": 1, "len": 1, "ref": 1, "alt": 1}).sort([("chr", pymongo.ASCENDING), ("start", pymongo.ASCENDING)]))
    # startFirstPos += step
    # endFirstPos += step
    endTime = datetime.datetime.now()
    duration = (endTime - startTime).total_seconds()
    mongoCumulativeExecTime += duration
    print("Mongo: Returned {0} records in {1} seconds".format(len(resultList), duration))
    print("****************")
print("Average Citus Execution time:{0}".format(citusCumulativeExecTime/numRuns))
print("Average Mongo Execution time:{0}".format(mongoCumulativeExecTime/numRuns))


# Execution times for Single Scan with filter on non-indexed field
numRuns = 30
minChromPos= 2000000
maxChromPos = 100000000
citusCumulativeExecTime = 0
mongoCumulativeExecTime = 0
margin = 1000000
chromosome = "1"
print("Start Time for single-scan:{0}".format(datetime.datetime.now()))
for i in range(0, numRuns):
    pos = random.randint(minChromPos, maxChromPos)
    # Proxy Query
    # step = 200000
    startFirstPos = pos - margin
    startLastPos = pos + margin
    endFirstPos = pos
    endLastPos = pos + margin + margin

    startTime = datetime.datetime.now()
    resultCursor.execute(
        "select * from public_1.variant where chrom = '{0}' and start_pos > {1} and start_pos <= {2} and end_pos >= {3} and end_pos < {4} and VAR_TYPE = '{5}' order by var_id".format(
            chromosome, startFirstPos, startLastPos, endFirstPos, endLastPos, "INDEL"))
    resultList = resultCursor.fetchall()
    endTime = datetime.datetime.now()
    duration = (endTime - startTime).total_seconds()
    citusCumulativeExecTime += duration
    print("Citus: Returned {0} records in {1} seconds".format(len(resultList), duration))

    startTime = datetime.datetime.now()
    query = {"chr": chromosome, "start": {"$gt": startFirstPos, "$lte": startLastPos},"end": {"$gte": endFirstPos, "$lt": endLastPos}, "type": "INDEL"}
    resultList = list(mongoProdCollHandle_2.find(query, {"_id":1, "chr": 1, "start": 1, "end" : 1, "type": 1, "len": 1, "ref": 1, "alt": 1}).sort([("chr", pymongo.ASCENDING), ("start", pymongo.ASCENDING)]))
    # startFirstPos += step
    # endFirstPos += step
    endTime = datetime.datetime.now()
    duration = (endTime - startTime).total_seconds()
    mongoCumulativeExecTime += duration
    print("Mongo: Returned {0} records in {1} seconds".format(len(resultList), duration))
    print("****************")
print("Average Citus Execution time:{0}".format(citusCumulativeExecTime/numRuns))
print("Average Mongo Execution time:{0}".format(mongoCumulativeExecTime/numRuns))

# Execution times for joins in Citus and Mongo
numRuns = 30
minChromPos= 2000000
maxChromPos = 100000000
citusCumulativeExecTime = 0
mongoCumulativeExecTime = 0
margin = 1000000
chromosome = "1"
print("Start Time for multi-scan:{0}".format(datetime.datetime.now()))
for i in range(0, numRuns):
    pos = random.randint(minChromPos, maxChromPos)
    # Proxy Query
    step = 200000
    startFirstPos = pos - margin
    startLastPos = pos + margin
    endFirstPos = pos
    endLastPos = pos + margin + margin
    while(True):
        startTime = datetime.datetime.now()
        resultCursor.execute(
            "select * from public_1.variant where chrom = '{0}' and start_pos > {1} and start_pos <= {2} and end_pos >= {3} and end_pos < {4} order by var_id".format(
                chromosome, startFirstPos, startFirstPos + step, startFirstPos, endLastPos))
        resultList = resultCursor.fetchall()
        resultCursor.execute(
            "select * from public_1.variant_sample where chrom = '{0}' and start_pos > {1} and start_pos <= {2} and end_pos >= {3} and end_pos < {4} order by var_id, sample_index".format(
                chromosome, startFirstPos, startFirstPos + step, startFirstPos, endLastPos))
        joinList = resultCursor.fetchall()
        numJoinResults = len(joinList)
        finalResultList = {}
        i = 0
        for result in resultList:
            var_id = result[0]
            if (i >= numJoinResults) or (joinList[i][0] != var_id):
                finalResultList[var_id] = (result, None)
            else:
                j = i + 1
                while(j<numJoinResults and joinList[j][0] == var_id): j += 1
                finalResultList[var_id] = (result, joinList[i:j])
                i = j
        endTime = datetime.datetime.now()
        duration = (endTime - startTime).total_seconds()
        citusCumulativeExecTime += duration
        print("Citus: Returned {0} records in {1} seconds".format(len(resultList), duration))

        # startTime = datetime.datetime.now()
        # query = {"chr": chromosome, "start": {"$gt": startFirstPos, "$lte": startFirstPos + step},
        #          "end": {"$gte": startFirstPos, "$lt": endLastPos}}
        # resultList = list(mongoProdCollHandle_2.find(query, {"_id":1, "chr": 1, "start": 1, "end" : 1, "type": 1, "len": 1, "ref": 1, "alt": 1}).sort([("chr", pymongo.ASCENDING), ("start", pymongo.ASCENDING)]))
        # endTime = datetime.datetime.now()
        # duration = (endTime - startTime).total_seconds()
        # mongoCumulativeExecTime += duration
        # print("Mongo: Returned {0} records in {1} seconds".format(len(resultList), duration))
        # print("****************")
        # startFirstPos += step
        # endFirstPos += step
        # if (startFirstPos >= startLastPos) or (endFirstPos >= endLastPos): break
print("Average Citus Execution time:{0}".format(citusCumulativeExecTime/numRuns))
print("Average Mongo Execution time:{0}".format(mongoCumulativeExecTime/numRuns))

# Execution times for parallel Multi Scan
numRuns = 30
minChromPos= 2000000
maxChromPos = 100000000
citusCumulativeExecTime = 0
mongoCumulativeExecTime = 0
margin = 1000000
chromosome = "1"
print("Start Time for multi-scan:{0}".format(datetime.datetime.now()))
for i in range(0, numRuns):
    pos = random.randint(minChromPos, maxChromPos)
    # Proxy Query
    step = 200000
    startFirstPos = pos - margin
    startLastPos = pos + margin
    endFirstPos = pos
    endLastPos = pos + margin + margin
    while(True):
        startTime = datetime.datetime.now()
        resultCursor.execute(
            "select * from public_1.variant where chrom = '{0}' and start_pos > {1} and start_pos <= {2} and end_pos >= {3} and end_pos < {4} order by var_id".format(
                chromosome, startFirstPos, startFirstPos + step, startFirstPos, endLastPos))
        resultList = resultCursor.fetchall()
        endTime = datetime.datetime.now()
        duration = (endTime - startTime).total_seconds()
        citusCumulativeExecTime += duration
        print("Citus: Returned {0} records in {1} seconds".format(len(resultList), duration))

        startTime = datetime.datetime.now()
        query = {"chr": chromosome, "start": {"$gt": startFirstPos, "$lte": startFirstPos + step},
                 "end": {"$gte": startFirstPos, "$lt": endLastPos}}
        resultList = list(mongoProdCollHandle_2.find(query, {"_id":1, "chr": 1, "start": 1, "end" : 1, "type": 1, "len": 1, "ref": 1, "alt": 1}).sort([("chr", pymongo.ASCENDING), ("start", pymongo.ASCENDING)]))
        endTime = datetime.datetime.now()
        duration = (endTime - startTime).total_seconds()
        mongoCumulativeExecTime += duration
        print("Mongo: Returned {0} records in {1} seconds".format(len(resultList), duration))
        print("****************")
        startFirstPos += step
        endFirstPos += step
        if (startFirstPos >= startLastPos) or (endFirstPos >= endLastPos): break
print("Average Citus Execution time:{0}".format(citusCumulativeExecTime/numRuns))
print("Average Mongo Execution time:{0}".format(mongoCumulativeExecTime/numRuns))

mongoProdClient.close()
postgresConnHandle.close()