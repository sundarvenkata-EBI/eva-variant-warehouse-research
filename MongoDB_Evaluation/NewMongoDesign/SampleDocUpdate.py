# This program encodes the Sample sub-document inside the variant document

from pymongo import MongoClient
import pymongo
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

def sampleSubDocUpdate(devMongoClient, prodMongoHandle, chromosome, lowerBound, upperBound, filesCacheLookup, srcCollHandle_grch37):
    unencoded_resultCollHandle = devMongoClient["eva_testing"]["sample_unenc_small"]
    encoded_resultCollHandle = devMongoClient["eva_testing"]["sample_enc_small"]

    numEncTimes = 0
    cumExecTime = 0
    encoded_resultCollHandle.delete_many({})
    unencoded_resultCollHandle.delete_many({})

    query = {"chr": chromosome, "start": {"$gte": lowerBound, "$lte": upperBound}, "files.samp": {"$exists": "true"}}
    results = list(srcCollHandle_grch37.find(query).limit(100))
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
                    startTime = datetime.datetime.now()
                    sampleDoc[sampleKey] = binencode(sampleIndexSet, numSamp)
                    endTime = datetime.datetime.now()
                    cumExecTime += (endTime-startTime).total_seconds()
                    numEncTimes += 1
            #del sampleDoc["def"]
            #sampleDoc[defaultGenotype] = hexencode(defaultGenotypeSampleSet, numSamp)
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