# A variation of MongoCSVExport.py that does not involve any ID generation

from collections import OrderedDict
from commonpyutils import guiutils
from bson import CodecOptions, SON, json_util
from pymongo import MongoClient
from pgdb import connect
from multiprocessing import Process, Pipe

import collections, datetime
import sys, json, os, pprint, hashlib

client = MongoClient(os.environ["MONGODEV_INSTANCE"])
mongodbHandle = client["admin"]
mongodbHandle.authenticate(os.environ["MONGODEV_UNAME"], os.environ["MONGODEV_PASS"])

mongodbHandle = client["eva_testing"]
srcCollHandle = mongodbHandle["variants_hsap_87_87_sample_mod"]

postgresHost = guiutils.promptGUIInput("PostgreSQL Host", "PostgreSQL Host")
postgresUser = guiutils.promptGUIInput("PostgreSQL Username", "PostgreSQL Username")
postgresPassword = guiutils.promptGUIInput("PostgreSQL password", "PostgreSQL password", "*")

def getDictValueOrNull(dict, key):
    if key in dict:
        return dict[key]
    return None

def insertDocs(sampleDocs, postgresHost, postgresUser, postgresPassword):
    postgresDBHandle = connect(database='template1', host=postgresHost, user=postgresUser, password=postgresPassword)
    postgresCursor = postgresDBHandle.cursor()
    docIndex = 0
    for sampleDoc in sampleDocs:
        documentId = sampleDoc["_id"]
        print(u"Inserting variant: {0}".format(documentId))
        #hgvIDArray = {}
        for doc in getDictValueOrNull(sampleDoc,"hgvs"):
            #hgvID = hashlib.md5(json.dumps(doc, sort_keys=True).encode("utf-8")).hexdigest()
            #if hgvID not in hgvIDArray:
                #hgvIDArray[hgvID] = hgvID
                postgresCursor.execute ("insert into public_1.hgv values (%s, %s, %s);",(documentId, getDictValueOrNull(doc,"type"), getDictValueOrNull(doc,"name")))
        #hgvIDArray = hgvIDArray.keys()
        #hgvIDArray.sort()
        #hgvGrpID = hashlib.md5("".join(hgvIDArray)).hexdigest()
        #for hgvID in hgvIDArray:
            #postgresCursor.execute("insert into public_1.hgv_grp values (%s,%s);",(hgvGrpID, hgvID))

        #filesIDArray = {}
        sampleIndex = 0
        for doc in getDictValueOrNull(sampleDoc,"files"):
            #filesID = hashlib.md5(json.dumps(doc, sort_keys=True, encoding="latin1").encode("utf-8")).hexdigest()
            sampDoc = getDictValueOrNull(doc, "samp")
            #sampleAttrID = None
            if sampDoc:
                #sampleAttrID = hashlib.md5(json.dumps(sampDoc, sort_keys=True).encode("utf-8")).hexdigest()
                for genotype in sampDoc.keys():
                    for elem in sampDoc[genotype]:
                        if type(elem) is dict:
                            postgresCursor.execute("insert into public_1.variant_sample_attrs values (%s,%s,%s,%s,%s,%s);",
                                                   (documentId, sampleIndex, genotype, elem["s"], elem["e"], None))
                        else:
                            postgresCursor.execute("insert into public_1.variant_sample_attrs values (%s,%s,%s,%s,%s,%s);",
                                                   (documentId, sampleIndex, genotype, None, None, elem))
            #if filesID not in filesIDArray:
                #filesIDArray[filesID] = filesID
                postgresCursor.execute("insert into public_1.src_file values (%s,%s,%s,%s,%s);",
                                       (documentId, sampleIndex, getDictValueOrNull(doc,"fid"), getDictValueOrNull(doc,"sid"),
                                        getDictValueOrNull(doc, "fm")))
            sampleIndex += 1

        annotDoc = getDictValueOrNull(sampleDoc,"annot")
        #ctIDArray = {}
        ctIndex = 0
        for ctDoc in annotDoc["ct"]:
            #ctID = hashlib.md5(json.dumps(ctDoc, sort_keys=True).encode("utf-8")).hexdigest()
            #if ctID not in ctIDArray:
                #ctIDArray[ctID] = ctID
                postgresCursor.execute("insert into public_1.ct values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);",
                                       (documentId, ctIndex, getDictValueOrNull(ctDoc, "gn"), getDictValueOrNull(ctDoc, "ensg"), getDictValueOrNull(ctDoc, "enst"),
                                                getDictValueOrNull(ctDoc, "codon"), getDictValueOrNull(ctDoc, "strand"), getDictValueOrNull(ctDoc, "bt"), getDictValueOrNull(ctDoc, "aaChange"), "{" + ",".join([str(x) for x in getDictValueOrNull(ctDoc, "so")]) + "}"))
                ctIndex += 1

        postgresCursor.execute("insert into public_1.variant values (%s, %s, %s,%s,%s,%s,%s,%s);",
                 (getDictValueOrNull(sampleDoc,"_id"), getDictValueOrNull(sampleDoc,"chr"),
                  getDictValueOrNull(sampleDoc,"start"), getDictValueOrNull(sampleDoc,"end"),
                  getDictValueOrNull(sampleDoc,"type"), getDictValueOrNull(sampleDoc,"len"),
                  getDictValueOrNull(sampleDoc,"ref"), getDictValueOrNull(sampleDoc,"alt")
                  ))
        docIndex += 1

    postgresDBHandle.commit()
    postgresCursor.close()
    postgresDBHandle.close()

sampleDocs = list(srcCollHandle.find().limit(30000))
print("Start Time:" + str(datetime.datetime.now()))
processList = [Process(target=insertDocs, args=(sampleDocs[i:i+3750], postgresHost, postgresUser, postgresPassword)) for i in range(0,len(sampleDocs),3750)]
for process in processList:
    process.start()
for process in processList:
    process.join()
print("End Time:" + str(datetime.datetime.now()))