import pandas as pd, sys, functools, numpy as np, operator,os,gzip
from itertools import islice, groupby, count
from sklearn.cross_validation import train_test_split

if sys.argv.__len__() != 3:
    print("Usage: Assess_Genotype_Distribution.py <VCF_FilePath> <ChunkSize>")
    sys.exit(0)

vcfDir = sys.argv[1]
chunkSize = int(sys.argv[2])

targetVCFFileName = os.path.join(vcfDir, "Stratified_Sample.vcf")
try:
    os.remove(targetVCFFileName)
except Exception:
    pass
targetVCFFileHandle = open(targetVCFFileName, "a")

firstPass = True
for vcfFileName in os.listdir(vcfDir):
    if vcfFileName.upper().endswith("VCF.GZ"):
        vcfFileName = os.path.join(vcfDir, vcfFileName)
        # Number of rows to be skipped
        numRowsToSkip = 0
        processedLinesLimit = 5000
        lineChunkSize = 100
        inputFileHandle = gzip.open(vcfFileName)
        groups = groupby(inputFileHandle, key=lambda k, line=count(): next(line) // lineChunkSize)
        foundHeaderRow = False

        for k, group in groups:
            for line in group:
                if firstPass: targetVCFFileHandle.write(line)
                if line.strip().upper().startswith("#CHROM"):
                    foundHeaderRow = True
                    break
                numRowsToSkip += 1
                if numRowsToSkip >= processedLinesLimit:
                    break
            if foundHeaderRow:
                break

        firstPass = False
        inputFileHandle.close()

        reader = pd.read_table(vcfFileName, sep = '\t', skiprows=numRowsToSkip, chunksize=10, iterator=True, low_memory=False, engine='c', compression='gzip')
        genotypeCountDict = {}
        vcfDF = reader.get_chunk(10)
        columnList = vcfDF.columns.tolist()
        sampleColumnList = columnList[columnList.index("FORMAT")+1:]

        reader = pd.read_table(vcfFileName, sep = '\t', skiprows=numRowsToSkip, chunksize=chunkSize, iterator=True, low_memory=False, engine='c', compression='gzip')
        for chunk in reader:
            vcfDF = chunk
            vcfDF[sampleColumnList[0]] = vcfDF[sampleColumnList[0]].map(lambda x: x.split(":")[0])
            vcfDF = vcfDF[vcfDF[sampleColumnList[0]] != "./."]
            vcfDF = vcfDF.reset_index()
            # Generate stratified sample
            featuresToLook = ["#CHROM", "INFO", "FORMAT"]
            train, test = train_test_split(vcfDF[featuresToLook], test_size=0.1,random_state=0, train_size=0)
            vcfDF.iloc[test.index].to_csv(targetVCFFileHandle, header=False, index=False, sep="\t")

targetVCFFileHandle.close()

reader = pd.read_table(targetVCFFileName, sep='\t', skiprows=numRowsToSkip, chunksize=10, iterator=True, low_memory=False,
                       engine='c')
vcfDF = reader.get_chunk(10)
columnList = vcfDF.columns.tolist()
sampleColumnList = columnList[columnList.index("FORMAT") + 1:]

reader = pd.read_table(targetVCFFileName, sep='\t', skiprows=numRowsToSkip, chunksize=chunkSize, iterator=True,low_memory=False, engine='c')
chunkNo = 0
genotypeCountDict = {}
for chunk in reader:
    vcfDF = chunk
    sampleDFValues = pd.melt(vcfDF[sampleColumnList], id_vars=[], var_name="sample")
    sampleDFValues["value"] = sampleDFValues["value"].map(lambda x: x.split(":")[0])
    sampleDFValues = sampleDFValues[sampleDFValues["value"] != "./."]

    resultDict = dict(sampleDFValues["value"].value_counts())
    if not genotypeCountDict:
        genotypeCountDict = resultDict
    else:
        for genotype in resultDict.keys():
            if genotypeCountDict.has_key(genotype):
                genotypeCountDict[genotype] = genotypeCountDict[genotype] + resultDict[genotype]
            else:
                genotypeCountDict[genotype] = resultDict[genotype]
    chunkNo += 1
    print("Processed Chunk: {0}".format(chunkNo))

sorted_genotypeCountTuple = sorted(genotypeCountDict.items(), key=operator.itemgetter(1),reverse=True)
print("Most Dominant Genotype: {0}, Count: {1}".format(sorted_genotypeCountTuple[0][0], sorted_genotypeCountTuple[0][1]))
print("Second most Dominant Genotype: {0}, Count: {1}".format(sorted_genotypeCountTuple[1][0], sorted_genotypeCountTuple[1][1]))
print("Third most Dominant Genotype: {0}, Count: {1}".format(sorted_genotypeCountTuple[2][0], sorted_genotypeCountTuple[2][1]))
print("Genotype counts: \n{0}".format(sorted_genotypeCountTuple))

