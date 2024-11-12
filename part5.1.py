import re
import sys
import time

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, mean, min, stddev, sum
from scipy import stats

debug = "--debug" in sys.argv or "-d" in sys.argv

pattern = re.compile("^[a-z]+$")

spark = SparkSession.builder.master('local[*]').appName('BigDataTP').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

print("Start")
start_time = time.time()

gdelt_parquet = spark.read.parquet('dataset/parquet')
print(gdelt_parquet.printSchema) if debug else None

gdelt_parquet.createTempView('gdelt')
print(spark.sql("SELECT count(1) FROM gdelt").explain())

# Add your implementation
gdelt_filtered = gdelt_parquet \
    .withColumn("_c34", col("_c34").cast("float"))
gdelt_grouped = gdelt_filtered.groupBy("_c10", "_c11").agg(
    mean("_c34").alias("avgTone"),
    stddev("_c34").alias("stddevTone"),
    min("_c34").alias("minTone"),
    max("_c34").alias("maxTone"),
    sum("_c31").alias("NumMentions")
)
gdelt_sorted = gdelt_grouped.orderBy(["NumMentions"], ascending=[0])
gdelt_sorted.write.save("output.csv", format="csv", mode="overwrite")
gdelt_sorted.show()

print("End")
print("--- %s seconds ---" % (time.time() - start_time))


# Convert to Pandas DataFrame for visualization
visual_data = gdelt_grouped.toPandas()

# Plotting average tone by _c10 and _c11
plt.figure(figsize=(16, 8))
sns.barplot(data=visual_data, x="_c10", y="avgTone", hue="_c11", ci=None)
plt.title("Average Tone by Religion and Subcategory")
plt.xlabel("Religion (_c10)")
plt.ylabel("Average Tone (avgTone)")
plt.xticks(rotation=45)
plt.legend(title="_c11")
plt.show()

# Collect unique _c10/_c11 pairs
groups = gdelt_filtered \
    .select("_c10", "_c11", "_c34",).toPandas() \
    .groupby(['_c10', '_c11'])['_c34'] \
    .apply(list).to_dict()

# Prepare data for ANOVA
anova_data = [np.array(value) for value in groups.values()]

# Perform ANOVA
f_statistic, p_value = stats.f_oneway(*anova_data)

# Output results
print(f"F-statistic: {f_statistic}, P-value: {p_value}")

# as of now:
# F-statistic: 13.292375580872188, P-value: 3.992895614666755e-33
# a very low P-value means that at least on religion has a significantly different average from the others
# This tells us that there is indeed some religion-based bias in the event's tones.
