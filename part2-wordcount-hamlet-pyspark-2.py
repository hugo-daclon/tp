import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lower, split

spark = SparkSession.builder.master(
    'local[*]').appName('BigDataTP').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

print("Start")
start_time = time.time()

text_file = spark.read.text('dataset/wordcount/hamlet.txt')
words = text_file.select(explode(split(text_file.value, "\\s+")).alias("word"))
words = words.filter("word is not NULL")
words = words.filter("word != ''")
words = words.filter("word like 'm%' or word like 'M%'")
words = words.withColumn("word", lower(col("word")))

word_counts = words.groupBy("word").count()

# print(word_counts.printSchema)

word_counts.orderBy(col("count").desc()).show(10)

print("End")
print("--- %s seconds ---" % (time.time() - start_time))
