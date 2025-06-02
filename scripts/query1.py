from collections import defaultdict
from pyspark import SparkContext
import datetime
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
RESULT_QUERY1_PATH = RESULT_PATH + "query1/"

def query1_rdd(sc, file_paths):
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

def main():
    # Start Spark Session
    sc = SparkContext(appName="Query1")

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
        if country_code:
            country = country_code.group(1)
            files_by_country[country].append(f)
        else:
            if "result" in f:
                continue
            # If no match is found
            files_by_country["unknown"].append(f)

    for country, flist in files_by_country.items():
        csv_rdd = query1_rdd(sc, flist)

        header = "date,zone_id,carbon_mean,carbon_min,carbon_max,cfe_mean,cfe_min,cfe_max"
        output_rdd = save_rdd(csv_rdd, RESULT_QUERY1_PATH + f'query1_{country}.csv', header=header, sc=sc)

        for row in output_rdd.take(10):
            print(row)

if __name__ == "__main__":
    main()
