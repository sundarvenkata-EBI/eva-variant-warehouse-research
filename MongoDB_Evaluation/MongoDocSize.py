import sqlite3, getpass, bson, multiprocessing, csv, hashlib
from pymongo import MongoClient
from multiprocessing import Process

client = MongoClient(getpass._raw_input("MongoDB Production Host:\n"))
mongodbHandle = client["admin"]
mongodbHandle.authenticate(getpass._raw_input("MongoDB Production User:\n"),
                           getpass.getpass("MongoDB Production Password:\n"))

mongodbHandle = client["eva_hsapiens_grch37"]
srcCollHandle_grch37 = mongodbHandle["variants_1_2"]

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

numProcessors = multiprocessing.cpu_count()
def maxCalc(variantDocs, batchNumber):
    if variantDocs:
        #region Initialize CSV file handles and writers
        maxCalcCSVHandle = open('maxCalc_{0}.csv'.format(batchNumber), 'wb')
        maxCalcCSVWriter = csv.writer(maxCalcCSVHandle, delimiter='\t', quotechar='', quoting = csv.QUOTE_NONE)

        maxDocSize = 0
        maxSizeDocID = None
        for variantDoc in variantDocs:
            variantID = variantDoc["chr"] + "_" + str(variantDoc["start"]).zfill(12) + "_" + str(
                variantDoc["end"]).zfill(12) + "_" + hashlib.md5(
                variantDoc["ref"] + "_" + variantDoc["alt"]).hexdigest()
            docSize = len(bson.BSON.encode(variantDoc))
            if docSize > maxDocSize:
                maxDocSize = docSize
                maxSizeDocID = variantID

        maxCalcCSVWriter.writerow([variantID, maxDocSize])
        maxCalcCSVHandle.close()


for doc in chromosome_LB_UB_Map:
    chromosome = doc["_id"]
    lowerBound = doc["minStart"]
    upperBound = doc["maxStart"]
    startRangeBegin = lowerBound
    step = 200000
    batchNumber = 0
    while startRangeBegin < upperBound:
        startRangeEnd = startRangeBegin + step
        query = {"chr": chromosome, "start": {"$gte": startRangeBegin, "$lte": startRangeEnd}}
        sampleDocs = list(srcCollHandle_grch37.find(query, no_cursor_timeout=True))
        numRecs = len(sampleDocs)
        if sampleDocs:
            print("Processing batch:{0} for chromosome {1} with {2} records".format(str(batchNumber), chromosome,
                                                                                    str(numRecs)))
            if numRecs <= numProcessors:
                maxCalc(sampleDocs, str(batchNumber) + "_0")
            else:
                processList = [Process(target=maxCalc, args=(
                sampleDocs[i:i + (numRecs / numProcessors)], chromosome + "_" + str(batchNumber) + "_" + str(i))) for i in
                               range(0, numRecs, (numRecs / numProcessors))]
                for process in processList:
                    process.start()
                for process in processList:
                    process.join()
        startRangeBegin += (step+1)
        batchNumber += 1