# A variation of MongoCSVExport.py that does not involve any ID generation

from collections import OrderedDict
# from commonpyutils import guiutils
from bson import CodecOptions, SON, json_util
from pymongo import MongoClient
import multiprocessing, psycopg2, socket
from multiprocessing import Process, Pipe
#from psycopg2.pool import SimpleConnectionPool
#from contextlib import contextmanager

import collections, datetime, unicodecsv as csv, getpass
import sys, json, os, pprint, hashlib, traceback, ctypes, platform

def getDictValueOrNull(dict, key):
    if key in dict:
        return dict[key]
    return None


def insertDocs(sampleDocs, batchNumber):
    if sampleDocs:
        hgvCSVHandle = open('hgv_{0}.csv'.format(batchNumber), 'wb')
        hgvCSVWriter = csv.writer(hgvCSVHandle, delimiter='\t', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        sampleAttrCSVHandle = open('sampleAttr_{0}.csv'.format(batchNumber), 'wb')
        sampleAttrCSVWriter = csv.writer(sampleAttrCSVHandle, delimiter='\t', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        srcFileCSVHandle = open('srcFile_{0}.csv'.format(batchNumber), 'wb')
        srcFileCSVWriter = csv.writer(srcFileCSVHandle, delimiter='\t', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        ctFileCSVHandle = open('ctFile_{0}.csv'.format(batchNumber), 'wb')
        ctFileCSVWriter = csv.writer(ctFileCSVHandle, delimiter='\t', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        variantFileCSVHandle = open('variantFile_{0}.csv'.format(batchNumber), 'wb')
        variantFileCSVWriter = csv.writer(variantFileCSVHandle, delimiter='\t', quotechar='"',
                                          quoting=csv.QUOTE_MINIMAL)
        for sampleDoc in sampleDocs:
            documentId = sampleDoc["chr"] + "_" + str(sampleDoc["start"]).zfill(12) + "_" + str(sampleDoc["end"]).zfill(12)
            try:
                for doc in getDictValueOrNull(sampleDoc, "hgvs"):
                    hgvCSVWriter.writerow(
                        [documentId, getDictValueOrNull(doc, "type"), getDictValueOrNull(doc, "name")])
                sampleIndex = 0
                for doc in getDictValueOrNull(sampleDoc, "files"):
                    sampDoc = getDictValueOrNull(doc, "samp")
                    if sampDoc:
                        for genotype in sampDoc.keys():
                            if genotype == "def":
                                sampleAttrCSVWriter.writerow(
                                    [documentId, sampleIndex, sampDoc[genotype], None, None, None, 1])
                            else:
                                for elem in sampDoc[genotype]:
                                    sampleAttrCSVWriter.writerow(
                                        [documentId, sampleIndex, genotype, None, None, elem, None])
                        srcFileCSVWriter.writerow(
                            [documentId, sampleIndex, getDictValueOrNull(doc, "fid"), getDictValueOrNull(doc, "sid"),
                             getDictValueOrNull(doc, "fm")])
                    sampleIndex += 1

                annotDoc = getDictValueOrNull(sampleDoc, "annot")
                if annotDoc:
                    ctIndex = 0
                    for ctDoc in annotDoc["ct"]:
                        soArray = getDictValueOrNull(ctDoc, "so")
                        if soArray:
                            soArray =  "{" + ",".join([str(x) for x in soArray]) + "}"
                        ctFileCSVWriter.writerow(
                            [documentId, ctIndex, getDictValueOrNull(ctDoc, "gn"), getDictValueOrNull(ctDoc, "ensg"),
                             getDictValueOrNull(ctDoc, "enst"),
                             getDictValueOrNull(ctDoc, "codon"), getDictValueOrNull(ctDoc, "strand"),
                             getDictValueOrNull(ctDoc, "bt"), getDictValueOrNull(ctDoc, "aaChange"),
                             soArray])
                        ctIndex += 1


                variantFileCSVWriter.writerow([documentId, getDictValueOrNull(sampleDoc, "chr"),
                                               getDictValueOrNull(sampleDoc, "start"),
                                               getDictValueOrNull(sampleDoc, "end"),
                                               getDictValueOrNull(sampleDoc, "type"),
                                               getDictValueOrNull(sampleDoc, "len"),
                                               getDictValueOrNull(sampleDoc, "ref"),
                                               getDictValueOrNull(sampleDoc, "alt")
                                               ])
            except Exception as e:
                print(sampleDoc["_id"])
                traceback.print_exc(file=sys.stdout)
                break

        hgvCSVHandle.close()
        sampleAttrCSVHandle.close()
        srcFileCSVHandle.close()
        ctFileCSVHandle.close()
        variantFileCSVHandle.close()

def get_free_space_mb(dirname):
    """Return folder/drive free space (in megabytes)."""
    if platform.system() == 'Windows':
        free_bytes = ctypes.c_ulonglong(0)
        ctypes.windll.kernel32.GetDiskFreeSpaceExW(ctypes.c_wchar_p(dirname), None, None, ctypes.pointer(free_bytes))
        return free_bytes.value / 1024 / 1024
    else:
        st = os.statvfs(dirname)
        return st.f_bavail * st.f_frsize / 1024 / 1024

def get_tot_allowed_recs():
    # Based on the assumption that it takes 4.5 GB space per million records
    return int(((get_free_space_mb(".")*0.60)/4500))*1e6

def is_registered(chromosome):
    resultCursor = postgresConnHandle.execute("select * from public_1.reg_chrom where chrom = '{0}'".format(chromosome))
    if resultCursor.rowcount > 0:
        return True
    return False


postgresHost = getpass._raw_input("PostgreSQL Host:\n")
postgresUser = getpass._raw_input("PostgreSQL Username:\n")
postgresConnHandle = psycopg2.connect(database='postgres', user=postgresUser,password='',host=postgresHost)

client = MongoClient(getpass._raw_input("MongoDB Production Host:\n"))
mongodbHandle = client["admin"]
mongodbHandle.authenticate(getpass._raw_input("MongoDB Production User:\n"),
                           getpass.getpass("MongoDB Production Password:\n"))

startTime = datetime.datetime.now()
print("Start Time:" + str(startTime))

numProcessors = multiprocessing.cpu_count()
mongodbHandle = client["eva_hsapiens_grch37"]
srcCollHandle = mongodbHandle["variants_1_2"]
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

totalAllowedRecords = int(get_tot_allowed_recs())
totNumRecordsProcessed = 0
for doc in chromosome_LB_UB_Map:
    chromosome = doc["_id"]
    if is_registered(chromosome):
        continue
    else:
        numRecordsToMigrate = doc["numEntries"]
        if (totNumRecordsProcessed + numRecordsToMigrate) > totalAllowedRecords: continue
        postgresConnHandle.execute("insert into public_1.reg_chrom values ('{0}','{1}')".format(chromosome, socket.getfqdn()))
        postgresConnHandle.commit()
        print("Processing chromosome: {0}".format(chromosome))
        step = 20000
        lowerBound = doc["minStart"]
        numRecordsProcessed = 0
        batchNumber = 0
        smallestChunkSize = (numProcessors*5)
        while numRecordsProcessed < numRecordsToMigrate:
            upperBound = lowerBound + step - 1
            query = {"$and": [{"chr": chromosome}, {"start": {"$gte": lowerBound}}, {"start": {"$lte": upperBound}},
                              {"end": {"$gte": lowerBound}}, {"end": {"$lte": upperBound}}]}
            sampleDocs = list(srcCollHandle.find(query, no_cursor_timeout=True))
            numRecs = len(sampleDocs)
            if sampleDocs:
                print("Processing batch:{0} for chromosome {1} with {2} records".format(str(batchNumber), chromosome, str(numRecs)))
                if numRecs <= smallestChunkSize:
                    insertDocs(sampleDocs, str(batchNumber) + "_0")
                else:
                    processList = [Process(target=insertDocs, args=(
                    sampleDocs[i:i + (numRecs / numProcessors)], chromosome + str(batchNumber) + "_" + str(i))) for i in
                                   range(0, numRecs, (numRecs / numProcessors))]
                    for process in processList:
                        process.start()
                    for process in processList:
                        process.join()
            lowerBound = upperBound + 1
            numRecordsProcessed += numRecs
            batchNumber += 1
        totNumRecordsProcessed += numRecordsProcessed

endTime = datetime.datetime.now()
print("End Time:" + str(endTime))
print("Total execution time:" + str((endTime - startTime).total_seconds()))
