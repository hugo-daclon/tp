import re
import time

from pyspark.sql import SparkSession

pattern = re.compile("^[a-z]+$")

spark = SparkSession.builder.master('local[*]').appName('BigDataTP').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

print("Start")
start_time = time.time()

gdelt = spark.read.option("delimiter", "\\t").csv('dataset/raw')
gdelt.write.parquet('dataset/parquet')

print("End")
print("--- %s seconds ---" % (time.time() - start_time))
