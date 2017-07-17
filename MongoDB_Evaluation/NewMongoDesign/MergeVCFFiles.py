import ftplib
import os, glob
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("spark://172.22.69.141:7077").setAppName("MergeVCFFiles")
sc = SparkContext(conf=conf)
sc.setLogLevel("INFO")

def downloadStudyFiles(ftpSite, studyFilesDir, ftpUserName, studyIndivFilePrefixes):
    baseDir = "/opt/data/mergevcf"
    ftp = ftplib.FTP(ftpSite, ftpUserName)
    ftp.cwd(studyFilesDir)
    os.chdir(baseDir)
    # for studyIndivFilePrefix in studyIndivFilePrefixes:
    #     tbiFileName = studyIndivFilePrefix + ".snp.vcf.gz.tbi"
    #     vcfFileName = studyIndivFilePrefix + ".snp.vcf.gz"
    #     targettbiFileName = baseDir + os.path.sep + tbiFileName
    #     targettbiFileHandle = open(targettbiFileName, 'wb')
    #     targetvcfFileName = baseDir + os.path.sep + vcfFileName
    #     targetvcfFileHandle = open(targetvcfFileName, 'wb')
    #
    #     ftp.retrbinary('RETR %s' % tbiFileName, targettbiFileHandle.write)
    #     ftp.retrbinary('RETR %s' % vcfFileName, targetvcfFileHandle.write)
    #     targettbiFileHandle.close()
    #     targetvcfFileHandle.close()

    snpFilesList = glob.glob("*.snp.vcf.gz")
    for snpFileName in snpFilesList:
        filePrefix = snpFileName.split(".")[0]
        os.system("""bcftools filter {0}.snp.vcf.gz -e ALT=\\'.\\' -o {0}_filtered.snp.vcf.gz -O z --threads 7""".format(filePrefix))
        os.system("tabix -p vcf {0}_filtered.snp.vcf.gz".format(filePrefix))


studyFilesDir = "/pub/databases/eva/PRJEB13618/submitted_files"
ftpSite = "ftp.ebi.ac.uk"
ftpUserName = "anonymous"
ftp = ftplib.FTP(ftpSite, ftpUserName)
ftp.cwd(studyFilesDir)
dirContents = ftp.nlst("*.vcf.gz.tbi")
studyIndivFilePrefixes = [x.split(".")[0] for x in dirContents[:20]]

numProcessingNodes = 5
partitionLength = len(studyIndivFilePrefixes)/numProcessingNodes
studyIndivRDD = sc.parallelize(range(0, len(studyIndivFilePrefixes),partitionLength), numProcessingNodes)
studyIndivRDD.map(lambda entry: downloadStudyFiles(ftpSite, studyFilesDir, ftpUserName, studyIndivFilePrefixes[entry: entry + partitionLength])).collect()
sc.stop()