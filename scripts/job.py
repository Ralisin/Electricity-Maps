from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("JobFromNiFi").getOrCreate()

data = [("Alice", 30), ("Bob", 25), ("Carol", 27)]
df = spark.createDataFrame(data, ["name", "age"])
df.show()

spark.stop()

print("Modified from container")

print("Modified from HOST")