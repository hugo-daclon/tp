import re
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

debug = "--debug" in sys.argv or "-d" in sys.argv

pattern = re.compile("^[a-z]+$")

spark = SparkSession.builder.master('local[*]').appName('BigDataTP').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

print("Start")
start_time = time.time()

gdelt_orig = spark.read.option("delimiter", "\\t").csv('dataset/raw')
print(gdelt_orig.printSchema) if debug else None

# Add your implementation
# 1. Filter the rows where Actor1CountryCode does not have a value
gdelt_filtered = gdelt_orig.filter("_c7 is not null and _c7 != ''").withColumn("_c31", col("_c31").cast("int"))
# 2. Group them by Actor1CountryCode
# 3. Sum them by NumMentions
gdelt_grouped = gdelt_filtered.groupBy("_c7").sum("_c31")
# 4. Order by NumMentions and get the 10 more mentioned
gdelt_sorted = gdelt_grouped.sort("sum(_c31)", ascending=False).limit(10)
# 5. Write the results to a CSV file
gdelt_sorted.write.save("output.csv", format="csv", mode="overwrite")
# 6. Show the top 10 results
gdelt_sorted.show()
print("End")
print("--- %s seconds ---" % (time.time() - start_time))
