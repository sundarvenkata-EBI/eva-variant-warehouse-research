from pyspark import SparkConf, SparkContext
from pyspark import TaskContext
from pyspark.sql import SQLContext
from os.path import expanduser
import os, gzip, fcntl, glob, errno, traceback, subprocess
from cassandra.cluster import Cluster

def gzipfileLineCount(fname):
    count = 0
    with gzip.open(fname,"rb") as f:
        for line in f:
            count += 1
    return count

def insertDefaultGenotypeToCassandra(sampleName, defaultGenotype, numMatchedVariants):
    session.execute(defaultGenotypeInsertPrepStmt.bind([sampleName, defaultGenotype, numMatchedVariants]))


def processVariantMatchFile(uniqueVariantListFileName, numMatchedVariants, matchOutputFileName, sampleName, defaultGenotype):
    insertDefaultGenotypeToCassandra(sampleName, defaultGenotype, numMatchedVariants)
    with gzip.open(uniqueVariantListFileName, "rb") as varListFile:
        with gzip.open(matchOutputFileName, "rb") as varMatchFile:
            if defaultGenotype == "0/0":
                for matchLine in varMatchFile:
                    chromMatch, varPosMatch, ref, alt, genotype, formatMatch, sampleinfoMatch = matchLine.strip().split("\t")
                    varPosMatch = long(varPosMatch)
                    chunk = int(varPosMatch/1000000)
                    for varPosLine in varListFile:
                        varPosLine = varPosLine.strip()
                        chromToFind, varPosToFind  = varPosLine.split("\t")
                        varPosToFind = long(varPosToFind)
                        while varPosToFind > varPosMatch:
                            matchLine = varMatchFile.readline()
                            if not matchLine: return
                            chromMatch, varPosMatch, ref, alt, genotype, formatMatch, sampleinfoMatch = matchLine.strip().split("\t")
                            varPosMatch = long(varPosMatch)
                            chunk = int(varPosMatch / 1000000)
                        if (chromToFind, varPosToFind) == (chromMatch,varPosMatch):
                            if genotype == defaultGenotype or alt != '.': break
                            session.execute(variantInsertPrepStmt.bind(
                                [chromToFind, chunk, varPosToFind, ref, alt, sampleName, formatMatch, sampleinfoMatch]))
                            break
                        else:
                            session.execute(variantInsertPrepStmt.bind(
                                [chromToFind, chunk, varPosToFind, ref, alt, sampleName, "GT", "./."]))
            else:
                for matchLine in varMatchFile:
                    chromMatch, varPosMatch, ref, alt, genotype, formatMatch, sampleinfoMatch = matchLine.strip().split("\t")
                    varPosMatch = long(varPosMatch)
                    chunk = int(varPosMatch / 1000000)
                    for varPosLine in varListFile:
                        varPosLine = varPosLine.strip()
                        chromToFind, varPosToFind  = varPosLine.split("\t")
                        varPosToFind = long(float(varPosToFind))
                        while varPosToFind > varPosMatch:
                            matchLine = varMatchFile.readline()
                            if not matchLine: return
                            chromMatch, varPosMatch, ref, alt, genotype, formatMatch, sampleinfoMatch = matchLine.strip().split("\t")
                            varPosMatch = long(varPosMatch)
                            chunk = int(varPosMatch / 1000000)
                        if (chromToFind, varPosToFind) == (chromMatch,varPosMatch):
                            if genotype != defaultGenotype:
                                session.execute(variantInsertPrepStmt.bind(
                                    [chromToFind, chunk, varPosToFind, ref, alt, sampleName, formatMatch,
                                     sampleinfoMatch]))
                            break


def matchVariantPosInStudyFiles(tokenNumber, numDistinctVariants, sparkRootDir, variantPositionFileName):
    global cluster, session, defaultGenotypeInsertPrepStmt, variantInsertPrepStmt
    cluster = Cluster(["192.168.0.18", "192.168.0.22", "192.168.0.20"])
    session = cluster.connect("variant_ksp")
    defaultGenotypeInsertPrepStmt = session.prepare("INSERT INTO sample_defaults (samplename, default_genotype, num_match_variants) VALUES (?,?,?)")
    variantInsertPrepStmt = session.prepare("INSERT INTO variants (chrom,chunk,start_pos,ref,alt,samplename, sampleinfoformat, sampleinfo) VALUES (?,?,?,?,?,?,?,?)")


    gzipFileHandle = None
    chosenFileToProcess = None
    try:
        baseDir = "/opt/data/mergevcf"
        os.chdir(baseDir)
        fileList = glob.glob("*.snp.vcf.gz")
        numFiles = len(fileList)
        initialFileListIndex = tokenNumber%numFiles
        fileListIndex = initialFileListIndex
        success = False
        while not success:
            try:
                chosenFileToProcess = fileList[fileListIndex]
                gzipFileHandle = gzip.open(chosenFileToProcess, "r")
                fcntl.flock(gzipFileHandle, fcntl.LOCK_EX|fcntl.LOCK_NB)
            except IOError, e:
                if e.errno == errno.EAGAIN:
                    fileListIndex = (fileListIndex + 1)%numFiles
                    if (fileListIndex == initialFileListIndex): return
                    continue
            success = True

        tc = TaskContext()
        variantPositionFilePath = sparkRootDir + os.path.sep + os.listdir(sparkRootDir)[0] + os.path.sep + variantPositionFileName
        matchOutputFileName = chosenFileToProcess.split(".")[0] + "_variantmatch.gz"
        errFileName = chosenFileToProcess.split(".")[0] + "_variantmatch.err"
        sampleNameCmd = subprocess.Popen("/opt/bcftools/bin/bcftools query -l {0}".format(chosenFileToProcess), shell = True, stdout = subprocess.PIPE)
        sampleName, err = sampleNameCmd.communicate()
        if err: return "Could not obtain sample name from the SNP file:{0}".format(chosenFileToProcess)
        sampleName = sampleName.strip()
        bcfVariantMatchCmd = "(/opt/bcftools/bin/bcftools query -f'%CHROM\\t%POS\\t%REF\\t%ALT[\\t%GT]\\t%LINE' -T {0} {2} | cut -f1,2,3,4,5,14,15 | gzip) 1> {1} 2> {3}".format(variantPositionFilePath, matchOutputFileName, chosenFileToProcess, errFileName)
        # result =
        result = os.system(bcfVariantMatchCmd)
        errFileHandle = open(errFileName, "r")
        errlines = errFileHandle.readlines()
        errFileHandle.close()
        if not errlines:
            numMatchedVariants = gzipfileLineCount(matchOutputFileName)
            if (numDistinctVariants*1.0/numMatchedVariants) > 2:
                processVariantMatchFile(variantPositionFilePath, numMatchedVariants, matchOutputFileName, sampleName, "./.")
            else:
                processVariantMatchFile(variantPositionFilePath, numMatchedVariants, matchOutputFileName, sampleName, "0/0")
        else:
            return "Error in processing file:{0}".format(chosenFileToProcess) + os.linesep + os.linesep.join(errlines)
    except Exception, e:
        return "Error in processing file:{0}".format(chosenFileToProcess) + os.linesep + traceback.format_exc()
    finally:
        if gzipFileHandle:
            fcntl.flock(gzipFileHandle, fcntl.LOCK_UN)
        cluster.shutdown()
        session.shutdown()
        return "SUCCESS"



conf = SparkConf().setMaster("spark://192.168.0.26:7077").setAppName("SingleSampleVCFMerge").set("spark.cassandra.connection.host", "192.168.0.18").set("spark.scheduler.listenerbus.eventqueue.size", "100000").set("spark.cassandra.read.timeout_ms", 1200000).set("spark.cassandra.connection.timeout_ms", 1200000)
sc = SparkContext(conf=conf)
sc.setLogLevel("INFO")

sparkRootDir = os.environ["HOME"] + os.path.sep + "spark-2.2.0-bin-hadoop2.7" + os.path.sep + "work" + os.path.sep + sc.applicationId

sql = SQLContext(sc)
variants = sql.read.format("org.apache.spark.sql.cassandra").\
               load(keyspace="variant_ksp", table="variants")
variants.registerTempTable("variantsTable")
resultDF = sql.sql("select chrom,start_pos from variantsTable group by 1,2 order by 1,2")
iterator = resultDF.toLocalIterator()
homeDir = expanduser("~")
variantPositionFileName = homeDir + os.path.sep + "unique_variant_positions.gz"
variantPositionFileHandle = gzip.open(variantPositionFileName, "wb")
for result in iterator:
    discardOutput = variantPositionFileHandle.write(result["chrom"] + "\t" + str(result["start_pos"]) + os.linesep)
variantPositionFileHandle.close()

numericRange = range(0,10000)
numProcessingNodes = 20
numPartitions = 1000
# partitionLength = len(studyIndivFilePrefixes)/numProcessingNodes
studyIndivRDD = sc.parallelize(numericRange, numPartitions)
sc.addFile(variantPositionFileName)

numDistinctVariants = 14915288
processResults = studyIndivRDD.map(lambda entry: matchVariantPosInStudyFiles(entry, numDistinctVariants, sparkRootDir, os.path.basename(variantPositionFileName))).collect()
for result in processResults:
    if result != "SUCCESS":
        print(result)

sc.stop()