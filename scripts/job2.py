from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("JobFromNiFi").getOrCreate()

input_path = "hdfs://master:54310/nifi/data"

df = spark.read.parquet(input_path + "/*.csv.parquet")

print(df.inputFiles())
print(len(df.inputFiles()))

df.show()

spark.stop()