from collections import defaultdict
from datetime import datetime
from pyspark import SparkContext
import re

from Utils import combine_into_single_rdd, save_rdd

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
HOURLY_CSV_PATH = CSV_PATH + "hourly/"
RESULT_PATH = BASE_INPUT_PATH + "result/"
RESULT_QUERY2_PATH = RESULT_PATH + "query2/"

def query2_rdd(sc, file_paths):
    rdd = combine_into_single_rdd(sc, file_paths)

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

    rdd_rank = sc.parallelize(all_lines)

    csv_rdd = reduced.map(lambda kv: (
            f"{kv[0]}," +                       # year
            f"{kv[1][0] / kv[1][2]:.6f}," +     # carbon_mean = carbon_sum / count
            f"{kv[1][1] / kv[1][2]:.6f}"        # cfe_mean = cfe_sum / count
    ))

    return rdd_rank, csv_rdd

def main():
    # Start Spark Session
    sc = SparkContext(appName="Query2")

    hadoop_conf = sc._jsc.hadoopConfiguration()
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)

    path = sc._jvm.org.apache.hadoop.fs.Path(HOURLY_CSV_PATH)
    files = fs.listStatus(path)

    # List of file names get dynamically from hsfs
    file_paths = [fileStatus.getPath().toString() for fileStatus in files]

    files_by_country = defaultdict(list)

    # RegEx to extract country code
    pattern = re.compile(r".*/([A-Z]{2})_.*\.csv$")

    for f in file_paths:
        country_code = pattern.match(f)
        if country_code and country_code.group(1) == "IT":
            country = country_code.group(1)
            files_by_country[country].append(f)

    for country, flist in files_by_country.items():
        if country != "IT":
            continue

        (rdd_rank, csv_rdd) = query2_rdd(sc, flist)

        header = "date,carbon_intensity,cfe"
        output_rdd_rk = save_rdd(rdd_rank, RESULT_QUERY2_PATH + f'query2_{country}_rk.csv', header=header, sc=sc)
        output_rdd_full = save_rdd(csv_rdd, RESULT_QUERY2_PATH + f'query2_{country}_full.csv', header=header, sc=sc)

        for row in output_rdd_rk.take(10):
            print(row)
        print("-------")

        for row in output_rdd_full.take(10):
            print(row)


if __name__ == "__main__":
    main()
