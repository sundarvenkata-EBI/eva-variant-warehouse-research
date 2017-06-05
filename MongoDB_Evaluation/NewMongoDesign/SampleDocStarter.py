import socket, fcntl, struct

from pyspark import SparkConf, SparkContext
from pymongo import MongoClient
import pymongo, multiprocessing
from multiprocessing import Process
from commonpyutils import guiutils
import os, copy, bson, datetime

def convertNumberArrayToRange(numArray):
    resultArray = []
    reArrangedArray = sorted(numArray)
    rangeStartIndex = reArrangedArray[0]
    rangeEndIndex = rangeStartIndex
    prevNum = rangeStartIndex
    for i in range(1, len(reArrangedArray)):
        if reArrangedArray[i] == (prevNum + 1):
            prevNum = reArrangedArray[i]
            rangeEndIndex = reArrangedArray[i]
            continue
        else:
            if (rangeEndIndex - rangeStartIndex) >= 5:
                resultArray.append({"s": rangeStartIndex, "e": rangeEndIndex})
            else:
                resultArray.extend(range(rangeStartIndex, rangeEndIndex+1))
            rangeStartIndex = reArrangedArray[i]
            rangeEndIndex = rangeStartIndex
            prevNum = rangeStartIndex

    if (rangeEndIndex - rangeStartIndex) >= 5:
        resultArray.append({"s": rangeStartIndex, "e": rangeEndIndex})
    else:
        resultArray.extend(range(rangeStartIndex, rangeEndIndex + 1))
    return resultArray

def binencode(sampleIndexSet, numSamp):
    bitArray = ['0']*numSamp
    extraAlloc = 0
    if numSamp&31 > 0: extraAlloc = 1
    resultArray = ['']*((numSamp>>5)+extraAlloc)
    for elem in sampleIndexSet:
        bitArray[elem] = '1'
    bitArray = ''.join(bitArray)
    for i in range(0,numSamp,32):
        lookupVal = bitArray[i:i+32]
        if i + 32 > numSamp: lookupVal = lookupVal.zfill(32)
        resultArray[i>>5] = int(lookupVal,2)
    return resultArray

def processVariantDoc(chromosome, lowerBound, upperBound, srcCollHandle_grch37, unencoded_resultCollHandle, encoded_resultCollHandle):
    # print("Starting write for:{0},{1},{2}".format(chromosome, lowerBound, upperBound))
    query = {"chr": chromosome, "start": {"$gte": lowerBound, "$lt": upperBound}, "files.samp": {"$exists": "true"}}
    results = srcCollHandle_grch37.find(query, {"files.samp":1, "files.fid":1,"files.sid":1}).limit(100)
    for variantDoc in results:
        originalDoc = copy.deepcopy(variantDoc)
        filesDocs = variantDoc["files"]
        filesDocIndex = 0
        for filesDoc in filesDocs:
            if ("fid" not in filesDoc) or ("sid" not in filesDoc): continue
            fid = filesDoc["fid"]
            sid = filesDoc["sid"]
            fileCache = filesCacheLookup[fid + "_" + sid]
            if ("st" not in fileCache) or ("samp" not in fileCache): continue
            numSamp = fileCache["st"]["nSamp"]
            sampleDoc = filesDoc["samp"]
            defaultGenotypeSampleSet = set(range(0,numSamp))
            defaultGenotype = None
            for sampleKey in sampleDoc:
                if sampleKey == "def":
                    defaultGenotype = sampleDoc[sampleKey]
                else:
                    sampleIndexSet = set(sampleDoc[sampleKey])
                    defaultGenotypeSampleSet = defaultGenotypeSampleSet - sampleIndexSet
                    sampleDoc[sampleKey] = binencode(sampleIndexSet, numSamp)
            del sampleDoc["def"]
            sampleDoc[defaultGenotype] = binencode(defaultGenotypeSampleSet, numSamp)
            filesDoc["samp"] = sampleDoc
            filesDocs[filesDocIndex] = filesDoc
            originalFilesDocIndex = 0
            while True:
                if originalDoc["files"][originalFilesDocIndex]["fid"] == fid and originalDoc["files"][originalFilesDocIndex]["sid"] == sid:
                    if "def" in originalDoc["files"][originalFilesDocIndex]["samp"]:
                        del originalDoc["files"][originalFilesDocIndex]["samp"]["def"]
                    originalDoc["files"][originalFilesDocIndex]["samp"][defaultGenotype] = convertNumberArrayToRange(defaultGenotypeSampleSet)
                    break
                originalFilesDocIndex += 1
            filesDocIndex += 1


        unencoded_resultCollHandle.insert(originalDoc)
        variantDoc["files"] = filesDocs
        encoded_resultCollHandle.insert(variantDoc)
    # print("Finished write for:{0},{1},{2}".format(chromosome, lowerBound, upperBound))

def sampleSubDocUpdate(devMongoHost, (mongoProdHost, mongoProdUser, mongoProdPwd), chromosome, lowerBound, upperBound, filesCacheLookup):
    devMongoClient = MongoClient(devMongoHost)
    prodClient = MongoClient(mongoProdHost)
    prodMongoHandle = prodClient["admin"]
    prodMongoHandle.authenticate(mongoProdUser, mongoProdPwd)

    prodMongoHandle = prodClient["eva_hsapiens_grch37"]
    srcCollHandle_grch37 = prodMongoHandle["variants_1_2"]

    unencoded_resultCollHandle = devMongoClient["eva_testing"]["sample_unenc_small"]
    encoded_resultCollHandle = devMongoClient["eva_testing"]["sample_enc_small"]

    # numEncTimes = 0
    # cumExecTime = 0
    chunkSize = 2000000
    step = chunkSize/multiprocessing.cpu_count()
    while lowerBound <= upperBound:
        chunkLB = lowerBound
        chunkUB = lowerBound + chunkSize
        procArray = {}
        for i in range(chunkLB, chunkUB, step):
            procObj = Process(target=processVariantDoc, args=(chromosome, i, i+step, srcCollHandle_grch37, unencoded_resultCollHandle, encoded_resultCollHandle))
            procArray[(chunkLB, chunkUB, step)] = procObj
            procObj.start()
        for procKey in procArray.keys():
            procArray[procKey].join()
        lowerBound += chunkSize

    devMongoClient.close()
    prodClient.close()
# devmongoClient = MongoClient(os.environ["MONGODEV_INSTANCE"])
# devMongodbHandle = devmongoClient["admin"]
# devMongodbHandle.authenticate(os.environ["MONGODEV_UNAME"], os.environ["MONGODEV_PASS"])
def get_ip_address(ifname):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    return socket.inet_ntoa(fcntl.ioctl(
        s.fileno(),
        0x8915,  # SIOCGIFADDR
        struct.pack('256s', ifname[:15])
    )[20:24])

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
conf = SparkConf().setMaster("spark://172.22.69.141:7077").setAppName("MongoTest")
sc = SparkContext(conf=conf)
sc.setLogLevel("INFO")
chromosome_entries = sc.parallelize(chromosome_LB_UB_Map, 8)
chromosome_entries.map(lambda entry: sampleSubDocUpdate("172.22.69.141", (mongoProdHost, mongoProdUser, mongoProdPwd), entry["_id"], entry["minStart"], entry["maxStart"], filesCacheLookup)).collect()
sc.stop()
