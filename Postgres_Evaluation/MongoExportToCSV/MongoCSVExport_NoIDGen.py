# A variation of MongoCSVExport.py that does not involve any ID generation

from collections import OrderedDict
# from commonpyutils import guiutils
from bson import CodecOptions, SON, json_util
from pymongo import MongoClient
# from pgdb import connect
import multiprocessing
from multiprocessing import Process, Pipe
from psycopg2.pool import SimpleConnectionPool
from contextlib import contextmanager

import collections, datetime, unicodecsv as csv, getpass
import sys, json, os, pprint, hashlib, traceback

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
            documentId = hashlib.md5(sampleDoc["_id"].encode("utf-8")).hexdigest()
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
                        ctFileCSVWriter.writerow(
                            [documentId, ctIndex, getDictValueOrNull(ctDoc, "gn"), getDictValueOrNull(ctDoc, "ensg"),
                             getDictValueOrNull(ctDoc, "enst"),
                             getDictValueOrNull(ctDoc, "codon"), getDictValueOrNull(ctDoc, "strand"),
                             getDictValueOrNull(ctDoc, "bt"), getDictValueOrNull(ctDoc, "aaChange"),
                             "{" + ",".join([str(x) for x in getDictValueOrNull(ctDoc, "so")]) + "}"])
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


if __name__ == "__main__":
    client = MongoClient(getpass._raw_input("MongoDB Production Host:\n"))
    mongodbHandle = client["admin"]
    mongodbHandle.authenticate(getpass._raw_input("MongoDB Production User:\n"),
                               getpass.getpass("MongoDB Production Password:\n"))

    mongodbHandle = client["eva_hsapiens_grch37"]
    srcCollHandle = mongodbHandle["variants_1_2"]

    startTime = datetime.datetime.now()
    print("Start Time:" + str(startTime))
    numRecordsToMigrate = 200000
    step = 20000
    numProcessors = multiprocessing.cpu_count()
    lowerBound = 60000
    numRecordsProcessed = 0
    batchNumber = 0
    smallestChunkSize = 10
    while numRecordsProcessed < numRecordsToMigrate:
        upperBound = lowerBound + step - 1
        query = {"$and": [{"chr": "X"}, {"start": {"$gte": lowerBound}}, {"start": {"$lte": upperBound}},
                          {"end": {"$gte": lowerBound}}, {"end": {"$lte": upperBound}}]}
        sampleDocs = list(srcCollHandle.find(query, no_cursor_timeout=True))
        numRecs = len(sampleDocs)
        if sampleDocs:
            print("Processing batch:{0} with {1} records".format(str(batchNumber), str(numRecs)))
            if numRecs <= smallestChunkSize:
                insertDocs(sampleDocs, str(batchNumber) + "_0")
            else:
                processList = [Process(target=insertDocs, args=(
                sampleDocs[i:i + (numRecs / numProcessors)], str(batchNumber) + "_" + str(i))) for i in
                               range(0, numRecs, (numRecs / numProcessors))]
                for process in processList:
                    process.start()
                for process in processList:
                    process.join()
        lowerBound = upperBound + 1
        numRecordsProcessed += numRecs
        batchNumber += 1
    endTime = datetime.datetime.now()
    print("End Time:" + str(endTime))
    print("Total execution time:" + str((endTime - startTime).total_seconds()))
