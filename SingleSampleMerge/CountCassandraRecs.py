from pyspark import SparkConf, SparkContext
from pyspark import TaskContext
from pyspark.sql import SQLContext
from pyspark import SparkFiles
from os.path import expanduser
import os, gzip, fcntl, glob, errno, traceback
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, BatchType

def gzipfileLineCount(fname):
    count = 0
    with gzip.open(fname,"rb") as f:
        for line in f:
            count += 1
    return count

def getSampleName(fname):
    count = 0
    with gzip.open(fname,"rb") as f:
        line = f.readline().strip()
        return line.split("]")[-1].split(":")[0]


def insertDefaultGenotypeToCassandra(sampleName, defaultGenotype):
    session.execute(defaultGenotypeInsertPrepStmt.bind([sampleName, defaultGenotype]))


def processVariantMatchFile(uniqueVariantListFileName, matchOutputFileName, sampleName, defaultGenotype):
    insertDefaultGenotypeToCassandra(sampleName, defaultGenotype)
    with gzip.open(uniqueVariantListFileName, "rb") as varListFile:
        with gzip.open(matchOutputFileName, "rb") as varMatchFile:
            for line in varMatchFile:
                line = line.strip()
                if line.startswith('#'): break
            if defaultGenotype == "0/0":
                for matchLine in varMatchFile:
                    chromMatch, varPosMatch, ref, alt, genotype, formatMatch, sampleinfoMatch = matchLine.split("\t")
                    varPosMatch = long(varPosMatch)
                    chunk = int(varPosMatch/1000000)
                    for varPosLine in varListFile:
                        varPosLine = varPosLine.strip()
                        chromToFind, varPosToFind  = varPosLine.split("\t")
                        varPosToFind = long(varPosToFind)
                        while varPosToFind > varPosMatch:
                            matchLine = varMatchFile.readline()
                            if not matchLine: return
                            chromMatch, varPosMatch, ref, alt, genotype, formatMatch, sampleinfoMatch = matchLine.split("\t")
                            varPosMatch = long(varPosMatch)
                            chunk = int(varPosMatch / 1000000)
                        if (chromToFind, varPosToFind) == (chromMatch,varPosMatch):
                            if genotype == defaultGenotype: break
                            session.execute(variantInsertPrepStmt.bind(
                                [chromToFind, chunk, varPosToFind, ref, alt, sampleName, formatMatch, sampleinfoMatch]))
                            break
                        else:
                            session.execute(variantInsertPrepStmt.bind(
                                [chromToFind, chunk, varPosToFind, ref, alt, sampleName, "GT", "./."]))
            else:
                for matchLine in varMatchFile:
                    chromMatch, varPosMatch, ref, alt, genotype, formatMatch, sampleinfoMatch = matchLine.split("\t")
                    varPosMatch = long(varPosMatch)
                    chunk = int(varPosMatch / 1000000)
                    for varPosLine in varListFile:
                        varPosLine = varPosLine.strip()
                        chromToFind, varPosToFind  = varPosLine.split("\t")
                        varPosToFind = long(float(varPosToFind))
                        while varPosToFind > varPosMatch:
                            matchLine = varMatchFile.readline()
                            if not matchLine: return
                            chromMatch, varPosMatch, ref, alt, genotype, formatMatch, sampleinfoMatch = matchLine.split("\t")
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
    defaultGenotypeInsertPrepStmt = session.prepare("INSERT INTO sample_defaults (samplename, default_genotype) VALUES (?,?)")
    variantInsertPrepStmt = session.prepare("INSERT INTO variants (chrom,chunk,start_pos,ref,alt,samplename, sampleinfoformat, sampleinfo) VALUES (?,?,?,?,?,?,?,?)")


    gzipFileHandle = None
    chosenFileToProcess = None
    try:
        baseDir = "/opt/data/mergevcf"
        os.chdir(baseDir)
        fileList = glob.glob("*.snp.vcf.gz")
        fileListIndex = tokenNumber%(len(fileList))
        success = False
        while not success:
            try:
                chosenFileToProcess = fileList[fileListIndex]
                gzipFileHandle = gzip.open(chosenFileToProcess, "r")
                fcntl.flock(gzipFileHandle, fcntl.LOCK_EX|fcntl.LOCK_NB)
            except IOError, e:
                if e.errno == errno.EAGAIN:
                    fileListIndex += 1
                    if (fileListIndex == len(fileList)): return
                    continue
            success = True

        tc = TaskContext()
        variantPositionFilePath = sparkRootDir + os.path.sep + os.listdir(sparkRootDir)[0] + os.path.sep + variantPositionFileName
        matchOutputFileName = chosenFileToProcess.split(".")[0] + "_variantmatch.gz"
        # os.system('echo "{0}" >> variantPositionFilePath'.format(variantPositionFilePath))
        bcfVariantMatchCmd = "(/opt/bcftools/bin/bcftools query -f'%CHROM\t%POS[\t%GT]\t%REF\t%ALT\t%LINE\n' -H -T {0} {2} | cut -d$'\\t' -f1,2,3,4,5,14,15 | gzip) 1> {1} 2> {3}".format(variantPositionFilePath, matchOutputFileName, chosenFileToProcess, variantPositionFileName + "_variantmatch.err")
        # result = os.subprocess.Popen(bcfVariantMatchCmd, shell = True, stdout = os.subprocess.PIPE)
        result = os.system(bcfVariantMatchCmd)
        errFileHandle = open(variantPositionFileName + "_variantmatch.err", "r")
        errlines = errFileHandle.readlines()
        errFileHandle.close()
        if not errlines:
            numMatchedVariants = gzipfileLineCount(matchOutputFileName)
            sampleName = getSampleName(matchOutputFileName)
            if not sampleName: return "Could not obtain sample name from the match file:{0}".format(matchOutputFileName)
            if (numDistinctVariants*1.0/numMatchedVariants) > 2:
                pass
                #processVariantMatchFile(variantPositionFilePath, matchOutputFileName, sampleName, "./.")
            else:
                pass
                #processVariantMatchFile(variantPositionFilePath, matchOutputFileName, sampleName, "0/0")
        else:
            return "Error in processing file:{0}".format(chosenFileToProcess) + os.linesep + os.linesep.join(errlines)
    except Exception, e:
        return "Error in processing file:{0}".format(chosenFileToProcess) + os.linesep + traceback.format_exc()
    finally:
        if gzipFileHandle:
            fcntl.flock(gzipFileHandle, fcntl.LOCK_UN)



conf = SparkConf().setMaster("spark://192.168.0.26:7077").setAppName("SingleSampleVCFMerge").set("spark.cassandra.connection.host", "192.168.0.18").set("spark.scheduler.listenerbus.eventqueue.size", "100000").set("spark.cassandra.read.timeout_ms", 1200000).set("spark.cassandra.connection.timeout_ms", 1200000)
sc = SparkContext(conf=conf)
sc.setLogLevel("INFO")

sparkRootDir = os.environ["HOME"] + os.path.sep + "spark-2.2.0-bin-hadoop2.7" + os.path.sep + "work" + os.path.sep + sc.applicationId

sql = SQLContext(sc)
variants = sql.read.format("org.apache.spark.sql.cassandra").\
               load(keyspace="variant_ksp", table="variants")
# print("Total number of variants processed from ALL files:" + str(variants.count()))
# variants.registerTempTable("variantsTable")
# resultDF = sql.sql("select chrom,start_pos from variantsTable group by 1,2 order by 1,2")
# print("Counting number of distinct variants...")
# numDistinctVariants = resultDF.count()
# print("Total number of distinct variants:{0}".format(str(numDistinctVariants)))
# iterator = resultDF.toLocalIterator()
homeDir = expanduser("~")
variantPositionFileName = homeDir + os.path.sep + "unique_variant_positions.gz"
# variantPositionFileHandle = gzip.open(variantPositionFileName, "wb")
# for result in iterator:
#     discardOutput = variantPositionFileHandle.write(result["chrom"] + "\t" + str(result["start_pos"]) + os.linesep)
# variantPositionFileHandle.close()

numericRange = range(0,100)
numProcessingNodes = 20
numPartitions = 100
# partitionLength = len(studyIndivFilePrefixes)/numProcessingNodes
studyIndivRDD = sc.parallelize(numericRange, numPartitions)
sc.addFile(variantPositionFileName)

numDistinctVariants = 14915288
processResults = studyIndivRDD.map(lambda entry: matchVariantPosInStudyFiles(entry, numDistinctVariants, sparkRootDir, os.path.basename(variantPositionFileName))).collect()

sc.stop()