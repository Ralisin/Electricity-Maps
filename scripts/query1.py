from collections import defaultdict
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, min as spark_min, max as spark_max, avg
import argparse
import re
import sys
import time

from Utils  import *

# CSV header structure:
#  0 - Datetime (UTC),
#  1 - Country,
#  2 - Zone name,
#  3 - Zone id,
#  4 - Carbon intensity gCO₂eq/kWh (direct),
#  5 - Carbon intensity gCO₂eq/kWh (Life cycle),
#  6 - Carbon-free energy percentage (CFE%),
#  7 - Renewable energy percentage (RE%),
#  8 - Data source,
#  9 - Data estimated,
# 10 - Data estimation method

# Input path on HDFS
BASE_INPUT_PATH = "hdfs://master:54310/nifi/"
DATA_PATH = BASE_INPUT_PATH + "data/"
CSV_PATH = DATA_PATH + "csv/"
PARQUET_PATH = DATA_PATH + "parquet/"
HOURLY_CSV_PATH = CSV_PATH + "hourly/"
HOURLY_PARQUET_PATH = PARQUET_PATH + "hourly/"
RESULT_PATH = BASE_INPUT_PATH + "result/"
RESULT_QUERY1_PATH = RESULT_PATH + "query1/"

# InfluxDB
INFLUXDB_URL = "http://influxdb:8086"
BUCKET = "Q1"
ORG = "ralisin"
TOKEN = "my-super-secret-token"

def query1_rdd(sc, file_paths, save=False):
    # Get a single rdd for all files
    rdd = combine_into_single_rdd(sc, file_paths)

    # Ensure necessary values are not null
    rdd = rdd.filter(
        lambda fields:
            len(fields) > 6 and
            fields[0] and
            fields[3] and
            fields[4] and
            fields[6]
    )

    map_by_year_rdd = rdd.map(lambda fields: (
        (fields[0][:4], fields[3]),             # (year from datetime, zone id)
        (float(fields[4]), float(fields[6]))    # (carbon intensity direct, carbon free percentage)
    ))

    # Map every value into: (carbon_sum, carbon_min, carbon_max, cfe_sum, cfe_min, cfe_max, count)
    #                            0           1           2          3        4        5       6
    mapped = map_by_year_rdd.mapValues(lambda v: (v[0], v[0], v[0], v[1], v[1], v[1], 1))

    # Reduce by key to get the stats for each zone
    def reduce_stats(a, b):
        return (
            a[0] + b[0],        # sum carbon
            min(a[1], b[1]),    # min carbon
            max(a[2], b[2]),    # max carbon
            a[3] + b[3],        # sum cfe
            min(a[4], b[4]),    # min cfe
            max(a[5], b[5]),    # max cfe
            a[6] + b[6]         # count
        )

    reduced = mapped.reduceByKey(reduce_stats).sortByKey()

    if save:
        write_reduced_to_influxdb_q1(
            reduced,
            INFLUXDB_URL,
            TOKEN,
            ORG,
            BUCKET
        )

    csv_rdd = reduced.map(lambda kv: (
        kv[0][0] + "," +                    # year
        kv[0][1] + "," +                    # zone_id
        f"{kv[1][0] / kv[1][6]:.6f}," +     # carbon_mean = carbon_sum / count
        f"{kv[1][1]:.6f}," +                # carbon_min
        f"{kv[1][2]:.6f}," +                # carbon_max
        f"{kv[1][3] / kv[1][6]:.6f}," +     # cfe_mean = cfe_sum / count
        f"{kv[1][4]:.6f}," +                # cfe_min
        f"{kv[1][5]:.6f},"                  # cfe_max
    ))

    return csv_rdd


def query1_df(spark, file_paths, file_type="csv"):
    if file_type == "csv":
        df = spark.read.option("header", True).csv(file_paths)
    elif file_type == "parquet":
        df = spark.read.option("header", True).parquet(*file_paths)
        df = normalize_column_names(df)
    else:
        return None

    df = df.filter(
        col("Datetime (UTC)").isNotNull() &
        col("Zone name").isNotNull() &
        col("Carbon intensity gCO₂eq/kWh (direct)").isNotNull() &
        col("Carbon-free energy percentage (CFE%)").isNotNull()
    )

    df = df.withColumn("year", year(col("Datetime (UTC)")))

    grouped = df.groupBy("year", "Zone name").agg(
        avg("Carbon intensity gCO₂eq/kWh (direct)").alias("carbon_mean"),
        spark_min("Carbon intensity gCO₂eq/kWh (direct)").alias("carbon_min"),
        spark_max("Carbon intensity gCO₂eq/kWh (direct)").alias("carbon_max"),
        avg("Carbon-free energy percentage (CFE%)").alias("cfe_mean"),
        spark_min("Carbon-free energy percentage (CFE%)").alias("cfe_min"),
        spark_max("Carbon-free energy percentage (CFE%)").alias("cfe_max")
    ).orderBy("year", "Zone name")

    csv_rdd = grouped.rdd.map(lambda row: (
        f"{row['year']}," +
        f"{row['Zone name']}," +
        f"{row['carbon_mean']:.6f}," +
        f"{float(row['carbon_min']):.6f}," +
        f"{float(row['carbon_max']):.6f}," +
        f"{float(row['cfe_mean']):.6f}," +
        f"{float(row['cfe_min']):.6f}," +
        f"{float(row['cfe_max']):.6f},"
    ))

    return csv_rdd

def main(mode, file_format, save=False):
    if mode == "rdd" and file_format != "csv":
        print("Errore: la modalità RDD supporta solo il formato CSV.", file=sys.stderr)
        sys.exit(1)

    # Start Spark Session
    spark = SparkSession.builder.appName("Query1").getOrCreate()
    sc = spark.sparkContext

    hadoop_conf = sc._jsc.hadoopConfiguration()
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)

    if file_format == "csv":
        path = sc._jvm.org.apache.hadoop.fs.Path(HOURLY_CSV_PATH)
    else:
        path = sc._jvm.org.apache.hadoop.fs.Path(HOURLY_PARQUET_PATH)

    files = fs.listStatus(path)
    file_paths = [f.getPath().toString() for f in files if "result" not in f.getPath().toString()]

    pattern = re.compile(r".*/([A-Z]{2})_.*\.(csv|parquet)$")
    files_by_country = defaultdict(list)

    for f in file_paths:
        match = pattern.match(f)
        if match:
            country = match.group(1)
        else:
            country = "unknown"
        files_by_country[country].append(f)

    for country, flist in files_by_country.items():
        rdd = None

        if mode == "df":
            start = time.time()
            rdd = query1_df(spark, flist, file_type=file_format)
            end = time.time()
            print(f"[query1_df_{file_format}] {country} - Tempo: {end - start:.4f} s")
            for row in rdd.take(10):
                print(row)
            print("------------------------")

        elif mode == "rdd":
            start = time.time()
            rdd = query1_rdd(sc, flist, save)
            end = time.time()
            print(f"[query1_rdd_{file_format}] {country} - Tempo: {end - start:.4f} s")
            for row in rdd.take(10):
                print(row)
            print("------------------------")

        if rdd is not None:
            header = "date,zone_id,carbon_mean,carbon_min,carbon_max,cfe_mean,cfe_min,cfe_max"
            save_rdd(rdd, RESULT_QUERY1_PATH + f'query1_{country}.csv', header=header, sc=sc)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["rdd", "df"], required=True, help="Tipo di elaborazione: rdd o df")
    parser.add_argument("--format", choices=["csv", "parquet"], required=True, help="Formato dei file: csv o parquet")
    parser.add_argument("--save", action="store_true", help="Se presente, salva i risultati su InfluxDB")
    args = parser.parse_args()

    start  = time.time()
    main(args.mode, args.format, args.save)
    end  = time.time()
    print(f"[main_query1] Tempo trascorso: {end - start:.4f} secondi")
