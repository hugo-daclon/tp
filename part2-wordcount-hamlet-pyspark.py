import re
import sys
import time

from pyspark.sql import SparkSession

debug = "--debug" in sys.argv or "-d" in sys.argv
cache = "--cache" in sys.argv or "-c" in sys.argv

pattern = re.compile("^[mM][a-z]+$")

spark = SparkSession.builder.master(
    'local[*]').appName('BigDataTP').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

print("Start")
start_time = time.time()

text_file = sc.textFile("dataset/wordcount/hamlet.txt")
print(text_file.collect()) if debug else None

words = text_file.flatMap(lambda line: line.split(" "))
print(words.collect()) if debug else None

words_filtered = words.map(lambda x: x.lower()).filter(
    lambda x: pattern.match(x))
print(words_filtered.collect()) if debug else None

wordCounts = words_filtered.map(lambda word: (
    word, 1)).reduceByKey(lambda a, b: a + b)
print(wordCounts.collect()) if debug else None
wordCounts.saveAsTextFile("output/wordcount/") if cache else None

wordCounts_sorted = wordCounts.sortBy(lambda a: a[1], ascending=False).take(10)
print(wordCounts_sorted)

print("End")
print("--- %s seconds ---" % (time.time() - start_time))
