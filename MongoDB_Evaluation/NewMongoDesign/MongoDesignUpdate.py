#########################
# Update to flip the "def" key into a value in the "files.samp" sub-document in Variant documents
#########################

from collections import OrderedDict

from bson import CodecOptions, SON, json_util
from pymongo import MongoClient
import collections
import sys, json, os, pprint

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

client = MongoClient(os.environ["MONGODEV_INSTANCE"])
db = client["admin"]
db.authenticate(os.environ["MONGODEV_UNAME"], os.environ["MONGODEV_PASS"])

db = client["eva_testing"]
srcCollHandle = db["variants_hsap_87_87"]
destCollHandle = db["variants_hsap_87_87_sample_mod"]

docHandles = srcCollHandle.find().limit(100)
for docHandle in docHandles:
    filesDocIndex = 0
    docChangeFlag = False
    for fileSubDoc in docHandle["files"]:
        if "samp" in fileSubDoc:
            sampleDoc = fileSubDoc["samp"]
            if "def" in sampleDoc:
                sampleDoc[sampleDoc["def"]] = "def"
                sampleDoc.pop("def", None)
                fileSubDoc["samp"] = sampleDoc
                docHandle["files"][filesDocIndex] = fileSubDoc
                docChangeFlag = True
                filesDocIndex += 1
    if docChangeFlag:
        print(u"Inserting document: {0}".format(docHandle["_id"]))
        destCollHandle.insert(docHandle)
                #srcCollHandle.find_and_modify(query={"_id": docHandle["_id"]},update={"$set": {"files.{0}.samp".format(filesDocIndex): sampleDoc}})


docHandles = destCollHandle.find()
filesCollHandle = db["files_100MB"]
docChangeFlag = False

for docHandle in docHandles:
    filesDocIndex = 0
    for fileSubDoc in docHandle["files"]:
        fileDocHandle = filesCollHandle.find({"fid": fileSubDoc["fid"]}).next()
        numericallyIndexedFiles = {v: k for k, v in fileDocHandle["samp"].iteritems()}
        numericSampleIndices = numericallyIndexedFiles.keys()

        sampleArray = []
        rangeDoc = []
        defaultGenotype = None
        if "samp" in fileSubDoc:
            sampleDoc = fileSubDoc["samp"]
            for key in sampleDoc.keys():
                value = sampleDoc[key]
                if value != "def":
                    sampleDoc[key] = convertNumberArrayToRange(sampleDoc[key])
                    fileSubDoc["samp"][key] = sampleDoc[key]
                    sampleArray.extend(value)
                else:
                    defaultGenotype = key
            sampleDoc[defaultGenotype] = convertNumberArrayToRange(set(numericSampleIndices) - set(sampleArray))
            fileSubDoc["samp"][defaultGenotype] = sampleDoc[defaultGenotype]
            docHandle["files"][filesDocIndex] = fileSubDoc
            filesDocIndex += 1
            docChangeFlag = True
    if docChangeFlag:
        print(u"Updating document: {0}".format(docHandle["_id"]))
        destCollHandle.update({"_id": docHandle["_id"]}, docHandle)
        docChangeFlag = False
