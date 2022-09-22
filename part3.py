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

gdelt = spark.read.option("delimiter", "\\t").csv('dataset/raw')
print(gdelt.printSchema)

# Add your implementation
# 1. Filter the rows where Actor1CountryCode does not have a value
# 2. Group them by Actor1CountryCode
# 3. Sum them by NumMentions
# 4. Order by NumMentions and get the 10 more mentioned
# 5. Write the results to a CSV file

print ("End")
print("--- %s seconds ---" % (time.time() - start_time))
