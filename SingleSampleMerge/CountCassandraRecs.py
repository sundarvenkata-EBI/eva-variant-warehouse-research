from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import os



conf = SparkConf().setMaster("spark://192.168.0.26:7077").setAppName("SingleSampleVCFMerge").set("spark.cassandra.connection.host", "192.168.0.18").set("spark.scheduler.listenerbus.eventqueue.size", "100000").set("spark.cassandra.read.timeout_ms", 1200000).set("spark.cassandra.connection.timeout_ms", 1200000)
sc = SparkContext(conf=conf)
sc.setLogLevel("INFO")

sparkRootDir = os.environ["HOME"] + os.path.sep + "spark-2.2.0-bin-hadoop2.7" + os.path.sep + "work" + os.path.sep + sc.applicationId

sql = SQLContext(sc)
variants = sql.read.format("org.apache.spark.sql.cassandra").\
               load(keyspace="variant_ksp", table="variants")
print("*************************************************************************")
print("Total number of variants processed from ALL files:" + str(variants.count()))
print("*************************************************************************")
variants.registerTempTable("variantsTable")
resultDF = sql.sql("select chrom,start_pos from variantsTable group by 1,2")
print("Counting number of distinct variants...")
numDistinctVariants = resultDF.count()
print("*************************************************************************")
print("Total number of distinct variants:{0}".format(str(numDistinctVariants)))
print("*************************************************************************")

sc.stop()