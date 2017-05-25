# A variation of MongoCSVExport.py that does not involve any ID generation

from collections import OrderedDict
# from commonpyutils import guiutils
from bson import CodecOptions, SON, json_util
from pymongo import MongoClient
import multiprocessing, psycopg2, socket
from multiprocessing import Process, Pipe
#from psycopg2.pool import SimpleConnectionPool
#from contextlib import contextmanager
from optparse import OptionParser

import bitarray, base64
import collections, datetime, unicodecsv as csv, getpass
import sys, json, os, pprint, hashlib, traceback, ctypes, platform

def getDictValueOrNull(dict, key):
    if key in dict:
        return dict[key]
    return None

def getGTBitArray(arr,size):
    resultBitArray = bitarray.bitarray(size)
    resultBitArray.setall(False)
    for elem in arr:
        resultBitArray[elem] = True
    return resultBitArray

def insertDocs(variantDocs, batchNumber):
    if variantDocs:
        #region Initialize CSV file handles and writers
        variantCSVHandle = open('variant_{0}.csv'.format(batchNumber), 'wb')
        variantCSVWriter = csv.writer(variantCSVHandle, delimiter='\t', quotechar='', quoting = csv.QUOTE_NONE)

        filesCSVHandle = open('files_{0}.csv'.format(batchNumber), 'wb')
        filesCSVWriter = csv.writer(filesCSVHandle, delimiter='\t', quotechar='', quoting = csv.QUOTE_NONE)

        samplesCSVHandle = open('samples_{0}.csv'.format(batchNumber), 'wb')
        samplesCSVWriter = csv.writer(samplesCSVHandle, delimiter='\t', quotechar='', quoting = csv.QUOTE_NONE)

        annotCSVHandle = open('ctFile_{0}.csv'.format(batchNumber), 'wb')
        annotCSVWriter = csv.writer(annotCSVHandle, delimiter='\t', quotechar='', quoting = csv.QUOTE_NONE)
        #endregion

        for variantDoc in variantDocs:
            variantID = variantDoc["chr"] + "_" + str(variantDoc["start"]).zfill(12) + "_" + str(variantDoc["end"]).zfill(12) + "_" + hashlib.md5(variantDoc["ref"] + "_" + variantDoc["alt"]).hexdigest()
            chunkID = variantDoc["chr"] + "_" + (variantDoc["start"]/1000000)
            try:
                sampleIndex = 0
                defaultGenotype = None
                for doc in getDictValueOrNull(variantDoc, "files"):
                    fid = getDictValueOrNull(doc, "fid")
                    sid = getDictValueOrNull(doc, "sid")
                    defaultGenotypeSampleSet = set()
                    numSamp = None
                    attrs = None
                    srcBinData = None
                    if fid and sid:
                        if "{0}_{1}".format(fid, sid) in filesMap:
                            numSamp = filesMap["{0}_{1}".format(fid, sid)]
                            defaultGenotypeSampleSet = set(range(0,numSamp))
                    sampDoc = getDictValueOrNull(doc, "samp")
                    if sampDoc:
                        for genotype in sampDoc.keys():
                            if genotype == "def":
                                defaultGenotype = sampDoc[genotype]
                            else:
                                sampleIndexSet = set(sampDoc[genotype])
                                defaultGenotypeSampleSet = defaultGenotypeSampleSet - sampleIndexSet
                                gtBitArray = None
                                if numSamp:
                                    gtBitArray = getGTBitArray(sampleIndexSet, numSamp)
                                gtBitArray = gtBitArray.to01() if gtBitArray else None
                                samplesCSVWriter.writerow(
                                    [variantID, chunkID, sampleIndex, genotype, numSamp, gtBitArray])
                        if defaultGenotype:
                            samplesCSVWriter.writerow(
                                [variantID, chunkID, sampleIndex, defaultGenotype, numSamp, getGTBitArray(defaultGenotypeSampleSet, numSamp).to01()])

                        attrs = getDictValueOrNull(doc, "attrs")
                        if "src" in attrs:
                            srcBinData = base64.b64encode(attrs["src"].__str__())
                            del attrs["src"]
                        if attrs: attrs = json.dumps(attrs, encoding ='latin1')
                    filesCSVWriter.writerow(
                            [variantID, chunkID, sampleIndex, getDictValueOrNull(doc, "fid"), getDictValueOrNull(doc, "sid"),
                             attrs, getDictValueOrNull(doc, "fm"),  srcBinData])
                    sampleIndex += 1

                annotDoc = getDictValueOrNull(variantDoc, "annot")
                overallSOArray = []
                minSiftScore = None
                maxSiftScore = None
                minPphenScore = None
                maxPphenScore = None
                overallXrefArray = []
                if annotDoc:
                    ctIndex = 0

                    for ctDoc in annotDoc["ct"]:
                        soArray = getDictValueOrNull(ctDoc, "so")
                        if soArray:
                            overallSOArray.extend(soArray)
                            soArray =  "{" + ",".join([str(x) for x in soArray]) + "}"
                        else:
                            soArray = "{}"

                        xrefArray = getDictValueOrNull(ctDoc, "xrefs")
                        if xrefArray: overallXrefArray.extend([x["id"] for x in xrefArray])

                        siftDoc = getDictValueOrNull(ctDoc, "sift")
                        pphenDoc = getDictValueOrNull(ctDoc, "polyphen")
                        siftScore = None
                        pphenScore = None
                        siftDesc = None
                        pphenDesc = None
                        if siftDoc:
                            siftScore = siftDoc["sc"]
                            minSiftScore = siftScore if not minSiftScore else min(siftScore, minSiftScore)
                            maxSiftScore = siftScore if not maxSiftScore else max(siftScore, maxSiftScore)
                        if pphenDoc:
                            pphenScore = pphenDoc["sc"]
                            minPphenScore = pphenScore if not minPphenScore else min(pphenScore, minPphenScore)
                            maxPphenScore = pphenScore if not maxPphenScore else max(pphenScore, maxPphenScore)
                        annotCSVWriter.writerow(
                            [variantID, chunkID, ctIndex, getDictValueOrNull(ctDoc, "gn"), getDictValueOrNull(ctDoc, "ensg"),
                             getDictValueOrNull(ctDoc, "enst"),
                             getDictValueOrNull(ctDoc, "codon"), getDictValueOrNull(ctDoc, "strand"),
                             getDictValueOrNull(ctDoc, "bt"), getDictValueOrNull(ctDoc, "aaChange"),
                             soArray, siftScore, siftDesc, pphenScore, pphenDesc])
                        ctIndex += 1

                idArray = getDictValueOrNull(variantDoc, "ids")
                if idArray:
                    idArray = "{" + ",".join([x for x in idArray]) + "}"
                else:
                    idArray = "{}"

                hgvArray = getDictValueOrNull(variantDoc, "hgvs")
                if hgvArray: hgvArray = json.dumps(hgvArray, encoding ='latin1')
                # if hgvArray:
                #     hgvArray = "ARRAY[" + ",".join(["row(" + "'" + x["type"] + "'" + "," + "'" + x["name"] + "'" + ")::public_1.hgv"  for x in hgvArray]) + "]"
                # else:
                #     hgvArray = "{}"

                if overallSOArray:
                    overallSOArray = "{" + ",".join([str(x) for x in overallSOArray]) + "}"
                else:
                    overallSOArray = "{}"

                if overallXrefArray:
                    overallXrefArray = "{" + ",".join([x for x in overallXrefArray]) + "}"
                else:
                    overallXrefArray = "{}"
                variantCSVWriter.writerow([variantID, chunkID, getDictValueOrNull(variantDoc, "chr"),
                                               getDictValueOrNull(variantDoc, "start"),
                                               getDictValueOrNull(variantDoc, "end"),
                                               getDictValueOrNull(variantDoc, "len"),
                                               getDictValueOrNull(variantDoc, "ref"),
                                               getDictValueOrNull(variantDoc, "alt"),
                                               getDictValueOrNull(variantDoc, "type"),
                                               idArray,
                                               hgvArray,
                                               overallSOArray,
                                               minSiftScore,
                                               maxSiftScore,
                                               minPphenScore,
                                               maxPphenScore,
                                               overallXrefArray
                                               ])
            except Exception as e:
                print(variantDoc["_id"])
                traceback.print_exc(file=sys.stdout)
                break

        filesCSVHandle.close()
        samplesCSVHandle.close()
        annotCSVHandle.close()
        variantCSVHandle.close()

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
    resultCursor = postgresConnHandle.cursor()
    resultCursor.execute("select * from public_1.reg_chrom where chrom = '{0}'".format(chromosome))
    if resultCursor.rowcount > 0:
        return True
    return False

parser = OptionParser()
parser.add_option("-l", "--recordlimit", dest="recordLimit",
                  help="Record limit", type="int")
(options, args) = parser.parse_args()

postgresHost = getpass._raw_input("PostgreSQL Host:\n")
postgresUser = getpass._raw_input("PostgreSQL Username:\n")

mongoHost = getpass._raw_input("MongoDB Production Host:\n")
mongoUser = getpass._raw_input("MongoDB Production User:\n")
mongoPass = getpass.getpass("MongoDB Production Password:\n")
client = MongoClient(mongoHost)
mongodbHandle = client["admin"]
mongodbHandle.authenticate(mongoUser,mongoPass)

startTime = datetime.datetime.now()
print("Start Time:" + str(startTime))

numProcessors = multiprocessing.cpu_count()
mongodbHandle = client["eva_hsapiens_grch37"]
srcCollHandle = mongodbHandle["variants_1_2"]

filesMap = {'11480_PRJEB4019':1092,'5506_PRJEB6930':2504,'ERZ015345_PRJEB4019':1092,'ERZ015346_PRJEB4019':1092,'ERZ015347_PRJEB4019':1092,'ERZ015348_PRJEB4019':1092,'ERZ015349_PRJEB4019':1092,'ERZ015350_PRJEB4019':1092,'ERZ015351_PRJEB4019':1092,'ERZ015352_PRJEB4019':1092,'ERZ015353_PRJEB4019':1092,'ERZ015354_PRJEB4019':1092,'ERZ015356_PRJEB4019':1092,'ERZ015357_PRJEB4019':1092,'ERZ015358_PRJEB4019':1092,'ERZ015359_PRJEB4019':1092,'ERZ015361_PRJEB4019':1092,'ERZ015362_PRJEB4019':1092,'ERZ015363_PRJEB4019':1092,'ERZ015365_PRJEB4019':1092,'ERZ015366_PRJEB4019':1092,'ERZ015367_PRJEB4019':1092,'ERZ015369_PRJEB4019':1092,'ERZ015710_PRJEB4019':1092,'ERZ329750_PRJEB15385':2,'ERZ367934_PRJNA289433':10640,'ERZ367935_PRJNA289433':10640,'ERZ367936_PRJNA289433':10640,'ERZ367937_PRJNA289433':10640,'ERZ367938_PRJNA289433':10640,'ERZ367939_PRJNA289433':10640,'ERZ367940_PRJNA289433':10640,'ERZ367941_PRJNA289433':10640,'ERZ367942_PRJNA289433':10640,'ERZ367943_PRJNA289433':10640,'ERZ367944_PRJNA289433':10640,'ERZ367945_PRJNA289433':10640,'ERZ367946_PRJNA289433':10640,'ERZ367947_PRJNA289433':10640,'ERZ367948_PRJNA289433':10640,'ERZ367949_PRJNA289433':10640,'ERZ367950_PRJNA289433':10640,'ERZ367951_PRJNA289433':10640,'ERZ367952_PRJNA289433':10640,'ERZ367953_PRJNA289433':10640,'ERZ367954_PRJNA289433':10640,'ERZ367955_PRJNA289433':10640,'ERZ367956_PRJNA289433':10640,'ERZ390625_PRJNA289433':10640,'ERZX00031_PRJEB6930':2504,'ERZX00032_PRJEB6930':2504,'ERZX00033_PRJEB6930':2504,'ERZX00034_PRJEB6930':2504,'ERZX00035_PRJEB6930':2504,'ERZX00036_PRJEB6930':2504,'ERZX00037_PRJEB6930':2504,'ERZX00038_PRJEB6930':2504,'ERZX00039_PRJEB6930':2504,'ERZX00040_PRJEB6930':2504,'ERZX00041_PRJEB6930':2504,'ERZX00042_PRJEB6930':2504,'ERZX00043_PRJEB6930':2504,'ERZX00044_PRJEB6930':2504,'ERZX00045_PRJEB6930':2504,'ERZX00046_PRJEB6930':2504,'ERZX00047_PRJEB6930':2504,'ERZX00048_PRJEB6930':2504,'ERZX00049_PRJEB6930':2504,'ERZX00051_PRJEB6930':2504,'ERZX00052_PRJEB6930':2504}
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

totalAllowedRecords = min(int(get_tot_allowed_recs()), options.recordLimit)
totNumRecordsProcessed = 0
for doc in chromosome_LB_UB_Map:
    postgresConnHandle = psycopg2.connect(
        "dbname='postgres' user='{0}' host='{1}' password=''".format(postgresUser, postgresHost))
    chromosome = doc["_id"]
    if is_registered(chromosome):
        continue
    else:
        if options.recordLimit:
            numRecordsToMigrate = options.recordLimit
        else:
            numRecordsToMigrate = doc["numEntries"]
        if ((totNumRecordsProcessed + numRecordsToMigrate) > totalAllowedRecords): continue
        dmlCursor = postgresConnHandle.cursor()
        dmlCursor.execute("insert into public_1.reg_chrom values ('{0}','{1}')".format(chromosome, socket.getfqdn()))
        postgresConnHandle.commit()
        postgresConnHandle.close()
        print("Processing chromosome: {0}".format(chromosome))
        step = min(20000, options.recordLimit)
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
                    sampleDocs[i:i + (numRecs / numProcessors)], chromosome + "_" + str(batchNumber) + "_" + str(i))) for i in
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
