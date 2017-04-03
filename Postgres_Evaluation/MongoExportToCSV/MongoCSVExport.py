from collections import OrderedDict
from commonpyutils import guiutils
from bson import CodecOptions, SON, json_util
from pymongo import MongoClient
from pgdb import connect

import collections
import sys, json, os, pprint, hashlib

client = MongoClient(os.environ["MONGODEV_INSTANCE"])
mongodbHandle = client["admin"]
mongodbHandle.authenticate(os.environ["MONGODEV_UNAME"], os.environ["MONGODEV_PASS"])

mongodbHandle = client["eva_testing"]
srcCollHandle = mongodbHandle["variant_chr21_1_1_sample_mod"]

sampleDoc = srcCollHandle.find_one()

postgresDBHandle = connect(database='template1',host=guiutils.promptGUIInput("PostgreSQL Host", "PostgreSQL Host"), user=guiutils.promptGUIInput("PostgreSQL Username", "PostgreSQL Username"), password=guiutils.promptGUIInput("PostgreSQL password", "PostgreSQL password", "*"))
postgresCursor = postgresDBHandle.cursor()

def getDictValueOrNull(dict, key):
    if key in dict:
        return dict[key]
    return None

hgvIDArray = {}
for doc in getDictValueOrNull(sampleDoc,"hgvs"):
    hgvID = hashlib.md5(json.dumps(doc, sort_keys=True).encode("utf-8")).hexdigest()
    if hgvID not in hgvIDArray:
        hgvIDArray[hgvID] = hgvID
        postgresCursor.execute ("insert into public.hgv values (%s, %s, %s)",(hgvID, getDictValueOrNull(doc,"type"), getDictValueOrNull(doc,"name")))
hgvIDArray = hgvIDArray.keys()
hgvIDArray.sort()
hgvGrpID = hashlib.md5("".join(hgvIDArray)).hexdigest()
for hgvID in hgvIDArray:
    postgresCursor.execute("insert into public.hgv_grp values (%s,%s)",(hgvGrpID, hgvID))
    

filesIDArray = {}
for doc in getDictValueOrNull(sampleDoc,"files"):
    filesID = hashlib.md5(json.dumps(doc, sort_keys=True, encoding="latin1").encode("utf-8")).hexdigest()
    sampDoc = getDictValueOrNull(doc, "samp")
    sampleAttrID = None
    if sampDoc:
        sampleAttrID = hashlib.md5(json.dumps(sampDoc, sort_keys=True).encode("utf-8")).hexdigest()
        for genotype in sampDoc.keys():
            for elem in sampDoc[genotype]:
                if type(elem) is dict:
                    postgresCursor.execute("insert into public.variant_sample_attrs values (%s,%s,%s,%s,%s)",
                                           (sampleAttrID, genotype, elem["s"], elem["e"], None))
                else:
                    postgresCursor.execute("insert into public.variant_sample_attrs values (%s,%s,%s,%s,%s)",
                                           (sampleAttrID, genotype, None, None, elem))
    if filesID not in filesIDArray:
        filesIDArray[filesID] = filesID
        postgresCursor.execute("insert into public.src_file values (%s,%s,%s,%s,%s)",
                               (filesID, getDictValueOrNull(doc,"fid"), getDictValueOrNull(doc,"sid"),
                                getDictValueOrNull(doc, "fm"), sampleAttrID))


filesIDArray = filesIDArray.keys()
filesIDArray.sort()
filesGrpID = hashlib.md5("".join(filesIDArray)).hexdigest()
for filesID in filesIDArray:
    postgresDBHandle.execute("insert into public.file_grp values (%s,%s)",(filesGrpID, filesID))
    

annotDoc = getDictValueOrNull(sampleDoc,"annot")
ctIDArray = {}
for ctDoc in annotDoc["ct"]:
    ctID = hashlib.md5(json.dumps(ctDoc, sort_keys=True).encode("utf-8")).hexdigest()
    if ctID not in ctIDArray:
        ctIDArray[ctID] = ctID
        postgresCursor.execute("insert into public.ct values (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                               (ctID, getDictValueOrNull(ctDoc, "gn"), getDictValueOrNull(ctDoc, "ensg"), getDictValueOrNull(ctDoc, "enst"),
                                        getDictValueOrNull(ctDoc, "codon"), getDictValueOrNull(ctDoc, "strand"), getDictValueOrNull(ctDoc, "bt"), getDictValueOrNull(ctDoc, "aaChange"), "{" + ",".join([str(x) for x in getDictValueOrNull(ctDoc, "so")]) + "}"))
ctIDArray = ctIDArray.keys()
ctIDArray.sort()
ctGrpID = hashlib.md5("".join(ctIDArray)).hexdigest()
for ctID in ctIDArray:
    postgresCursor.execute("insert into public.ct_grp values (%s, %s)",(ctGrpID, ctID))

postgresCursor.execute("insert into public.variant values (%s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s)",
         (getDictValueOrNull(sampleDoc,"_id"), getDictValueOrNull(sampleDoc,"chr"), getDictValueOrNull(sampleDoc,"start"), getDictValueOrNull(sampleDoc,"end"), getDictValueOrNull(sampleDoc,"type"), getDictValueOrNull(sampleDoc,"len"), getDictValueOrNull(sampleDoc,"ref"), getDictValueOrNull(sampleDoc,"alt")
          , hgvGrpID, "ROW('{0}')".format(ctGrpID), filesGrpID))

postgresDBHandle.commit()