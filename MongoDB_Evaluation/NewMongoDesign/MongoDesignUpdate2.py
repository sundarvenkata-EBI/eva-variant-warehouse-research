#########################
# Update to add "phased" and "unphased" categorization to genotypes
#########################
from bson import CodecOptions, SON, json_util
from pymongo import MongoClient
from commonpyutils import guiutils
import collections
import sys, json, os, pprint

client = MongoClient(os.environ["MONGODEV_INSTANCE"])
db = client["admin"]
db.authenticate(os.environ["MONGODEV_UNAME"], os.environ["MONGODEV_PASS"])

db = client["eva_testing"]
srcCollHandle = db["variant_chr21_1_1_sample_mod"]

docHandles = srcCollHandle.find()
for docHandle in docHandles:
    filesDocIndex = 0
    for fileSubDoc in docHandle["files"]:
        if "samp" in fileSubDoc:
            sampleDoc = fileSubDoc["samp"]
            if not "phased" in sampleDoc:
                phasedArray = []
                unphasedArray = []
                for genotype in sampleDoc.keys():
                    if "/" in genotype:
                        unphasedArray.append({"gt": genotype, "si": sampleDoc[genotype]})
                    else:
                        phasedArray.append({"gt": genotype, "si": sampleDoc[genotype]})
                replacementDoc = {"phased": phasedArray, "unphased": unphasedArray}
                fileSubDoc["samp"] = replacementDoc
                docHandle["files"][filesDocIndex] = fileSubDoc
    print(u"Updating document: {0}".format(docHandle["_id"]))
    srcCollHandle.update({"_id": docHandle["_id"]}, docHandle)