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

# Execution times for Multi Scan
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
        startFirstPos += step
        endFirstPos += step
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

postgresConnHandle.close()