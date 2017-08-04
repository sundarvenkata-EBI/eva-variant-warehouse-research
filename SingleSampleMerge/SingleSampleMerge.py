import ftplib
import os, hashlib
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, BatchType
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("spark://192.168.0.26:7077").setAppName("SingleSampleVCFMerge")
sc = SparkContext(conf=conf)
sc.setLogLevel("INFO")

def writeVariantToCassandra(linesToWrite, sampleName):
        chunkSize = 1000000
        batch = BatchStatement(BatchType.UNLOGGED)
        for line in linesToWrite:
            lineComps = line.split("\t")
            chromosome = lineComps[0]
            position = long(float(lineComps[1]))
            rsID = lineComps[2].strip()
            ref = lineComps[3].strip()
            alt = lineComps[4].strip()
            qual = lineComps[5].strip()
            qualFilter = lineComps[6].strip()
            info = lineComps[7].strip()
            sampleInfoFormat = lineComps[8].strip()
            sampleInfo = lineComps[9].strip()
            chunk = int(position/chunkSize)
            variantID = chromosome + "_" + str(position).zfill(12) + "_" + hashlib.md5(ref + "_" + alt).hexdigest() + sampleName.zfill(12)
            boundStmt = stmt.bind([chromosome, chunk, position, ref, alt, qual, qualFilter, info, sampleInfoFormat, sampleInfo, rsID, variantID, sampleName])
            batch.add(boundStmt)
        session.execute(batch)


def writeHeaderToCassandra(headerLines, sampleName):
    headerPrepStmt = session.prepare("INSERT INTO headers (samplename, header) VALUES (?,?)")
    session.execute(headerPrepStmt.bind([sampleName, headerLines]))


def cassandraInsert(vcfFileName):
    totNumVariants = 0
    vcfFileHandle = open(vcfFileName, 'r')
    headerLines = ""
    sampleName = ""
    lineBatchSize = 50
    linesToWrite = []
    for line in vcfFileHandle:
        line = line.strip()
        if line.startswith("#"):
            headerLines += (line + os.linesep)
            if (line.startswith("#CHROM")):
                sampleName = line.split("\t")[-1]
                writeHeaderToCassandra(headerLines.strip(), sampleName)
                break
    lineBatchIndex = 0
    for line in vcfFileHandle:
        totNumVariants += 1
        line = line.strip()
        linesToWrite.append(line)
        lineBatchIndex += 1
        if lineBatchIndex == lineBatchSize:
            writeVariantToCassandra(linesToWrite, sampleName)
            lineBatchIndex = 0
            linesToWrite = []
    if linesToWrite:
        writeVariantToCassandra(linesToWrite, sampleName)
    vcfFileHandle.close()
    return totNumVariants



def processStudyFiles(ftpSite, studyFilesDir, ftpUserName, studyIndivFilePrefix):
    global cluster, session, stmt
    totNumVariants = 0
    cluster = Cluster(["192.168.0.18", "192.168.0.22", "192.168.0.20"])
    session = cluster.connect("variant_ksp")
    stmt = session.prepare("INSERT INTO variants (chrom,chunk,start_pos,ref,alt,qual,filter,info,sampleinfoformat,sampleinfo,var_id,var_uniq_id,sampleName) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)")
    baseDir = "/opt/data/mergevcf"
    tbiFileName = studyIndivFilePrefix + ".snp.vcf.gz.tbi"
    vcfFileName = studyIndivFilePrefix + ".snp.vcf.gz"
    targettbiFileName = baseDir + os.path.sep + tbiFileName
    targetvcfFileName = baseDir + os.path.sep + vcfFileName
    filteredFileName = "{0}_filtered.snp.vcf".format(studyIndivFilePrefix)
    os.chdir(baseDir)
    if not os.path.isfile(targetvcfFileName):
        ftp = ftplib.FTP(ftpSite, ftpUserName)
        ftp.cwd(studyFilesDir)

        if os.path.isfile(targettbiFileName): return
        if os.path.isfile(targetvcfFileName): return
        targettbiFileHandle = open(targettbiFileName, 'wb')
        targetvcfFileHandle = open(targetvcfFileName, 'wb')

        print("Retrieving file:{0}".format(tbiFileName))
        ftp.retrbinary('RETR %s' % tbiFileName, targettbiFileHandle.write)
        print("Retrieving file:{0}".format(vcfFileName))
        ftp.retrbinary('RETR %s' % vcfFileName, targetvcfFileHandle.write)
        targettbiFileHandle.close()
        targetvcfFileHandle.close()

    #filePrefix = vcfFileName.split(".")[0]
    if not os.path.isfile(baseDir + os.path.sep + filteredFileName):
        filterCommandResult = os.system("""/opt/bcftools/bin/bcftools filter {0}.snp.vcf.gz -e ALT=\\'.\\' -o {0}_filtered.snp.vcf -O v 2> {0}_filtering_err.txt""".format(studyIndivFilePrefix))
    else:
        filterCommandResult = 0
    if filterCommandResult != 0:
        print("Failed to process {}".format(studyIndivFilePrefix))
    else:
        totNumVariants = cassandraInsert(baseDir + os.path.sep + "{0}_filtered.snp.vcf".format(studyIndivFilePrefix))
        os.system("echo {0} > {1}_filtered_variant_count.txt".format(str(totNumVariants), studyIndivFilePrefix))

    session.shutdown()
    cluster.shutdown()
    return "Number of variants from {0}:{1}".format(studyIndivFilePrefix + ".snp.vcf.gz", totNumVariants)



studyFilesDir = "/pub/databases/eva/PRJEB13618/submitted_files"
ftpSite = "ftp.ebi.ac.uk"
ftpUserName = "anonymous"
ftp = ftplib.FTP(ftpSite, ftpUserName)
ftp.cwd(studyFilesDir)
dirContents = ftp.nlst("*.vcf.gz.tbi")
dirContents.sort()
studyIndivFilePrefixes = [x.split(".")[0] for x in dirContents[:100]]

numProcessingNodes = 10
numPartitions = 100
# partitionLength = len(studyIndivFilePrefixes)/numProcessingNodes
studyIndivRDD = sc.parallelize(studyIndivFilePrefixes, numPartitions)
studyIndivRDD.map(lambda entry: processStudyFiles(ftpSite, studyFilesDir, ftpUserName, entry)).collect()
sc.stop()