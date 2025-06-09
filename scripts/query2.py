from collections import defaultdict
from datetime import datetime
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, date_format, avg
import argparse
import re
import sys
import time

from Utils import *

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

REPETITIONS = 10

# Input path on HDFS
BASE_INPUT_PATH = "hdfs://master:54310/nifi/"
DATA_PATH = BASE_INPUT_PATH + "data/"
CSV_PATH = DATA_PATH + "csv/"
PARQUET_PATH = DATA_PATH + "parquet/"
HOURLY_CSV_PATH = CSV_PATH + "hourly/"
HOURLY_PARQUET_PATH = PARQUET_PATH + "hourly/"
RESULT_PATH = BASE_INPUT_PATH + "result/"
RESULT_QUERY2_PATH = RESULT_PATH + "query2/"

# InfluxDB
INFLUXDB_URL = "http://influxdb:8086"
BUCKET = "Q2"
ORG = "ralisin"
TOKEN = "my-super-secret-token"

def query2_rdd(sc, rdd, save=False):
    # Ensure necessary values are not null
    rdd = rdd.filter(
        lambda fields:
        len(fields) > 6 and
        fields[0] and
        fields[4] and
        fields[5] and
        fields[6] and
        fields[7]
    )

    # Convert the date in column 0 from "yyyy-MM-dd HH:mm:ss" to "yyyy_MM"
    def transform_date(fields):
        try:
            dt = datetime.strptime(fields[0], "%Y-%m-%d %H:%M:%S")
            fields[0] = dt.strftime("%Y_%m")
        except Exception:
            # If parsing fails, keep the original value or handle as needed
            pass
        return fields

    # rdd with date as yyyy_MM
    rdd = rdd.map(transform_date)

    map_by_year_and_month_rdd = rdd.map(lambda fields: (
        (fields[0]),  # (year from datetime, zone id)
        (float(fields[4]), float(fields[6]))  # (carbon intensity direct, carbon free percentage)
    ))

    # Map every value into: (carbon_sum, cfe_sum, count)
    #                            0          1       2
    mapped = map_by_year_and_month_rdd.mapValues(lambda v: (v[0], v[1], 1))

    # Reduce by key to get the stats for each month
    def reduce_stats(a, b):
        return (
            a[0] + b[0],  # sum carbon
            a[1] + b[1],  # sum cfe
            a[2] + b[2]  # count
        )

    reduced = mapped.reduceByKey(reduce_stats).sortByKey()

    if save:
        write_reduced_to_influxdb_q2(
            reduced,
            INFLUXDB_URL,
            TOKEN,
            ORG,
            BUCKET
        )

    top5_ci_asc = (
        reduced
        .map(lambda kv: (kv[0], kv[1][0] / kv[1][2], kv[1][1] / kv[1][2]))  # (year, carbon_mean, cfe_mean)
        .sortBy(lambda x: x[1], ascending=True)
        .take(5)
    )

    top5_ci_desc = (
        reduced
        .map(lambda kv: (kv[0], kv[1][0] / kv[1][2], kv[1][1] / kv[1][2]))  # (year, carbon_mean, cfe_mean)
        .sortBy(lambda x: x[1], ascending=False)
        .take(5)
    )

    top5_cf_asc = (
        reduced
        .map(lambda kv: (kv[0], kv[1][0] / kv[1][2], kv[1][1] / kv[1][2]))  # (year, carbon_mean, cfe_mean)
        .sortBy(lambda x: x[2], ascending=True)
        .take(5)
    )

    top5_cf_desc = (
        reduced
        .map(lambda kv: (kv[0], kv[1][0] / kv[1][2], kv[1][1] / kv[1][2]))  # (year, carbon_mean, cfe_mean)
        .sortBy(lambda x: x[2], ascending=False)
        .take(5)
    )

    def format_line(t):
        # t = (date, carbon_mean, cfe_mean)
        return f"{t[0]},{t[1]:.6f},{t[2]:.6f}"

    all_lines = []
    all_lines += list(map(format_line, top5_ci_desc))
    all_lines += list(map(format_line, top5_ci_asc))
    all_lines += list(map(format_line, top5_cf_desc))
    all_lines += list(map(format_line, top5_cf_asc))

    rdd_rank = sc.parallelize(all_lines).coalesce(1)

    csv_rdd = reduced.map(lambda kv: (
            f"{kv[0]}," +                       # year
            f"{kv[1][0] / kv[1][2]:.6f}," +     # carbon_mean = carbon_sum / count
            f"{kv[1][1] / kv[1][2]:.6f}"        # cfe_mean = cfe_sum / count
    ))

    return rdd_rank, csv_rdd

def query2_df(df):
    if df is None:
        return None

    df = df.filter(
        (col("Datetime (UTC)").isNotNull()) &
        (col("Carbon intensity gCO₂eq/kWh (direct)").isNotNull()) &
        (col("Carbon-free energy percentage (CFE%)").isNotNull())
    )

    df = df.withColumn("date", date_format(to_timestamp("Datetime (UTC)", "yyyy-MM-dd HH:mm:ss"), "yyyy_MM"))

    df = df.select(
        "date",
        col("Carbon intensity gCO₂eq/kWh (direct)").alias("carbon_intensity"),
        col("Carbon-free energy percentage (CFE%)").alias("cfe")
    )

    monthly_avg = df.groupBy("date").agg(
        avg("carbon_intensity").alias("carbon_mean"),
        avg("cfe").alias("cfe_mean")
    ).orderBy("date")

    top5_ci_asc = monthly_avg.orderBy("carbon_mean").limit(5)
    top5_ci_desc = monthly_avg.orderBy(col("carbon_mean").desc()).limit(5)
    top5_cfe_asc = monthly_avg.orderBy("cfe_mean").limit(5)
    top5_cfe_desc = monthly_avg.orderBy(col("cfe_mean").desc()).limit(5)

    from functools import reduce
    top_union = reduce(lambda df1, df2: df1.union(df2), [
        top5_ci_desc, top5_ci_asc, top5_cfe_desc, top5_cfe_asc
    ])

    rdd_rank = top_union.rdd.map(lambda row: f"{row['date']},{row['carbon_mean']:.6f},{row['cfe_mean']:.6f}")
    csv_rdd = monthly_avg.rdd.map(lambda row: f"{row['date']},{row['carbon_mean']:.6f},{row['cfe_mean']:.6f}")

    return rdd_rank, csv_rdd

def main(mode, file_format, save=False):
    if mode == "rdd" and file_format != "csv":
        print("Errore: la modalità RDD supporta solo il formato CSV.", file=sys.stderr)
        sys.exit(1)

    # Start Spark Session
    spark = SparkSession.builder.appName("Query2").getOrCreate()
    sc = spark.sparkContext

    hadoop_conf = sc._jsc.hadoopConfiguration()
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)

    base_path = HOURLY_CSV_PATH if file_format == "csv" else HOURLY_PARQUET_PATH
    path = sc._jvm.org.apache.hadoop.fs.Path(base_path)
    files = fs.listStatus(path)

    # List of file names get dynamically from hsfs
    file_paths = [fileStatus.getPath().toString() for fileStatus in files]

    files_by_country = defaultdict(list)

    # RegEx to extract country code
    pattern = re.compile(r".*/([A-Z]{2})_.*\.(csv|parquet)$")

    for f in file_paths:
        country_code = pattern.match(f)
        if country_code and country_code.group(1) == "IT":
            country = country_code.group(1)
            files_by_country[country].append(f)

    for country, flist in files_by_country.items():
        if country != "IT":
            continue

        rdd_rank = None
        csv_rdd = None

        time_list = []

        if mode == "rdd":
            rdd = combine_into_single_rdd(sc, file_paths)

            for rep in range(REPETITIONS):
                start = time.time()
                rdd_rank, csv_rdd = query2_rdd(sc, rdd, save)
                end = time.time()

                print(f"[query2_rdd_{file_format}] {country} - Tempo: {end - start:.4f} s")

                time_list.append(end - start)
            print(f"[query2_rdd_{file_format}] {country} - Tempo medio (esclusa la prima esecuzione): {sum(time_list[1:]) / len(time_list[1:])}")
        elif mode == "df":
            if file_format == "csv":
                df = spark.read.option("header", True).csv(file_paths)
            elif file_format == "parquet":
                df = spark.read.option("header", True).parquet(*file_paths)
                df = normalize_column_names(df)

            for rep in range(REPETITIONS):
                start = time.time()
                rdd_rank, csv_rdd = query2_df(df)
                end = time.time()

                print(f"[query2_df_{file_format}] {country} - Tempo: {end - start:.4f} s")

                time_list.append(end - start)
            print(f"[query2_df_{file_format}] {country} - Tempo medio (esclusa la prima esecuzione): {sum(time_list[1:]) / len(time_list[1:])}")

        if rdd_rank is None or csv_rdd is None:
            print(f"{mode}, {country}, {file_format} - rdd_rank or csv_rdd are None")
            continue

        header = "date,carbon_intensity,cfe"
        output_rdd_full = save_rdd(csv_rdd, RESULT_QUERY2_PATH + f'query2_{country}_full.csv', header=header, sc=sc)
        output_rdd_rk = save_rdd(rdd_rank, RESULT_QUERY2_PATH + f'query2_{country}_rk.csv', header=header, sc=sc)

        for row in output_rdd_rk.take(10):
            print(row)
        print("-------")

        for row in output_rdd_full.take(10):
            print(row)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Esegui Query2 con RDD o DataFrame e formato CSV/Parquet")
    parser.add_argument("--mode", choices=["rdd", "df"], required=True, help="Modalità di esecuzione: 'rdd' o 'df'")
    parser.add_argument("--format", choices=["csv", "parquet"], default="csv", help="Formato dei file (solo per df)")
    parser.add_argument("--save", action="store_true", help="Se presente, salva i risultati su InfluxDB")
    args = parser.parse_args()

    start = time.time()
    main(args.mode, args.format, args.save)
    end = time.time()
    print(f"[main_query1] Tempo trascorso: {end - start:.4f} secondi")