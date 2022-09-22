from pyspark.sql import SparkSession
from pyspark.sql import functions as f

import re
import time

pattern = re.compile("^[a-z]+$")

spark = SparkSession.builder.master('local[*]').appName('BigDataTP').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

print ("Start")
start_time = time.time()

gdelt_parquet = spark.read.parquet('dataset/parquet')
print(gdelt_parquet.printSchema)

gdelt_parquet.createTempView('gdelt')
print(spark.sql("SELECT count(1) FROM gdelt").explain())

# Add your implementation
# 1. Filter the rows where Actor1CountryCode does not have a value
# 2. Group them by Actor1CountryCode
# 3. Sum them by NumMentions
# 4. Order by NumMentions and get the 10 more mentioned
# 5. Write the results to a CSV file

print ("End")
print("--- %s seconds ---" % (time.time() - start_time))
