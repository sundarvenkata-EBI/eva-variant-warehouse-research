# import ftplib
import os, hashlib, sys, glob, socket
import traceback, subprocess

from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, BatchType
from pyspark import SparkConf, SparkContext

def getSampleName(bcfToolsDir, vcfFileName):
    sampleNameCmd = subprocess.Popen(
        "{0}/bin/bcftools query -l {1}".format(bcfToolsDir, vcfFileName), shell=True,
        stdout=subprocess.PIPE)
    sampleName, err = sampleNameCmd.communicate()
    if err: return err
    return sampleName.strip()

def get_ip_address():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    return s.getsockname()[0]

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
            boundStmt = variantInsertStmt.bind([chromosome, chunk, position, ref, alt, qual, qualFilter, info, sampleInfoFormat, sampleInfo, rsID, variantID, sampleName])
            batch.add(boundStmt)
        session.execute(batch, timeout = 1200)


def writeHeaderToCassandra(headerLines, sampleName):
    session.execute(headerInsertStmt.bind([sampleName, headerLines]), timeout=1200)


def cassandraInsert(vcfFileName):
    totNumVariants = None
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


def getErrFileContents(errFileName):
    errFileContents = None
    with open(errFileName, "r") as errFileHandle:
        errFileContents = errFileHandle.readlines()
    if errFileContents: return errFileContents
    return None


def someFunc(sampleInsertLogTableName, studyName, sampleName):
    pass


def getSampleProcessedStatus(sampleInsertLogTableName, studyName, sampleName):
    # someFunc(sampleInsertLogTableName, studyName, sampleName)
    # allrows = session.execute("select insert_flag from {0} where studyname = '{1}' and samplename = '{2}';".format(sampleInsertLogTableName, studyName, sampleName)).current_rows
    allrows = session.execute("select insert_flag from variant_ksp.variants_PRJEB21300 where studyname = 'PRJEB21300' and samplename = 'Proband-1001';")
    alist = []
    for row in allrows:
        a = alist.append(row)
    del allrows
    # firstRow = rows.next()
    # if firstRow: return False
    # if firstRow[0] == 1: return True
    return True

def processStudyFiles(studyName, studyFileName, cassandraNodeIPs, headerTableName, variantTableName, sampleInsertLogTableName, bcfToolsDir):
    sampleName = getSampleName(bcfToolsDir, studyFileName)
    isSampleProcessed = getSampleProcessedStatus(sampleInsertLogTableName, studyName, sampleName)
    if isSampleProcessed: return "Already processed!"
    # totNumVariants = 0
    # filterCommandResult = -1
    # samplePrefix, errFileContents,returnErrMsg, cluster, session = None, None, None, None, None
    # global cluster, session, variantInsertStmt, headerInsertStmt
    # try:
    #     cluster = Cluster(cassandraNodeIPs)
    #     session = cluster.connect("variant_ksp")
    #     variantInsertStmt = session.prepare(
    #         "insert into {0} (chrom,chunk,start_pos,ref,alt,qual,filter,info,sampleinfoformat,sampleinfo,var_id,var_uniq_id,sampleName) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)".format(variantTableName))
    #     headerInsertStmt = session.prepare("INSERT INTO {0} (samplename, header) VALUES (?,?)".format(headerTableName))
    #     sampleCommitInsertStmt = session.prepare("INSERT INTO {0} (samplename, header) VALUES (?,?)".format(headerTableName))
    #
    #     samplePrefix = os.path.basename(studyFileName).split(".")[0]
    #     baseDir = os.path.dirname(studyFileName)
    #     filteredFileName = "{0}_filtered.vcf".format(samplePrefix)
    #     os.chdir(baseDir)
    #
    #     if not os.path.isfile(baseDir + os.path.sep + filteredFileName):
    #         os.system("""{0}/bin/bcftools filter {1} -e ALT=\\'.\\' -o {2}_filtered.vcf -O v 2> {2}_filtering_err.txt""".format(bcfToolsDir,studyFileName, samplePrefix))
    #         errFileContents = getErrFileContents("{0}_filtering_err.txt".format(samplePrefix))
    #         if errFileContents: filterCommandResult = -1
    #     else:
    #         filterCommandResult = 0
    #
    #     if filterCommandResult != 0:
    #          returnErrMsg = "Failed to process {0} due to error: {1}".format(studyFileName, errFileContents)
    #     else:
    #         totNumVariants = cassandraInsert(baseDir + os.path.sep + "{0}_filtered.vcf".format(samplePrefix))
    #         os.system("echo {0} > {1}_filtered_variant_count.txt".format(str(totNumVariants), samplePrefix))
    #
    # except Exception, ex:
    #     returnErrMsg = "Error in processing file:{0}".format(studyFileName) + os.linesep + traceback.format_exc()
    # finally:
    #     if cluster != None and session != None:
    #         try:
    #             session.shutdown()
    #             cluster.shutdown()
    #         except Exception, e:
    #             pass
    #         if not returnErrMsg and totNumVariants != 0:
    #             session.execute("insert into {0} (studyname, samplename, insert_flag, numVariants) values ({1}, {2}, {3}, {4})".format(sampleInsertLogTableName, studyName, samplePrefix, 1, totNumVariants))
    #     if returnErrMsg: return returnErrMsg
    #     return None



# studyFilesDir = "/pub/databases/eva/PRJEB13618/submitted_files"
# ftpSite = "ftp.ebi.ac.uk"
# ftpUserName = "anonymous"
# ftp = ftplib.FTP(ftpSite, ftpUserName)
# ftp.cwd(studyFilesDir)
if len(sys.argv) != 6:
    print("Usage: SingleSampleMerge.py <Study Name> <Full Path to study files> <Cassandra node IP1> <Cassandra node IP2> <BCF Tools Directory>")
    sys.exit(1)

studyName = sys.argv[1]
studyFilesInputDir = sys.argv[2]
cassandraNodeIPs = [sys.argv[3], sys.argv[4]]
bcfToolsDir = sys.argv[5]
os.chdir(studyFilesInputDir)
dirContents = glob.glob("*.vcf.gz")
dirContents.sort()
studyFileNames = dirContents

keyspaceName = "variant_ksp"
variantTableName = keyspaceName + "." + "variants_{0}".format(studyName)
headerTableName = keyspaceName + "." + "headers_{0}".format(studyName)
sampleInsertLogTableName = keyspaceName + "." + "sample_insert_log"
cluster = Cluster(cassandraNodeIPs)
session = cluster.connect()
# Create keyspace, variant, header and sample insertlog tables
session.execute("create keyspace if not exists variant_ksp with replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};")
session.execute("create table if not exists {0} (samplename varchar, header varchar, primary key(samplename));".format(headerTableName))
session.execute("create table if not exists {0} (chrom varchar, chunk int, start_pos bigint, ref varchar, alt varchar, qual varchar, filter varchar, info varchar, sampleinfoformat varchar, sampleinfo  varchar, var_id varchar, var_uniq_id varchar, sampleName varchar,  primary key((chrom, chunk), start_pos, ref, alt, samplename));".format(variantTableName))
session.execute("create table if not exists {0} (studyname varchar, samplename varchar, insert_flag int, num_variants bigint, primary key((studyname, samplename)));".format(sampleInsertLogTableName))
session.shutdown()
cluster.shutdown()

conf = SparkConf().setMaster("spark://{0}:7077".format(get_ip_address())).setAppName("SingleSampleVCFMerge")
sc = SparkContext(conf=conf)
sc.setLogLevel("INFO")

numPartitions = len(studyFileNames)
# partitionLength = len(studyFileNames)/numProcessingNodes
studyIndivRDD = sc.parallelize(studyFileNames, numPartitions)
results = studyIndivRDD.map(lambda entry: processStudyFiles(studyName, studyFilesInputDir + os.path.sep + entry, cassandraNodeIPs, headerTableName, variantTableName, sampleInsertLogTableName, bcfToolsDir)).collect()
for result in results:
    if result:
        print(result)
sc.stop()