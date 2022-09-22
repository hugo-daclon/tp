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

text_file = sc.textFile("dataset/wordcount/hamlet.txt")
# print(text_file.collect())

words = text_file.flatMap(lambda line: line.split(" "))
# print(words.collect())

words_filtered = words.map(lambda x: x.lower()).filter(lambda x: pattern.match(x))
# print(words_filtered.collect())

wordCounts = words_filtered.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)
# print(wordCounts.collect())
# wordCounts.saveAsTextFile("output/wordcount/")

wordCounts_sorted = wordCounts.sortBy(lambda a:a[1],ascending=False).take(10)
print(wordCounts_sorted)

print ("End")
print("--- %s seconds ---" % (time.time() - start_time))
