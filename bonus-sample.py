from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.master(
    "local[*]").appName("BigDataTP").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

print("Start")

print("Reading CSV file")
csv_file = spark.read.option("header", True).option("delimiter", ";").csv(
    "dataset/bonus/data.csv")

print("Samples commands")
csv_file.show(10)
csv_file.printSchema()
csv_file.select(col("Station"), col("Paramètre")).limit(8).show(truncate=False)
print(csv_file.count())

print("End")
