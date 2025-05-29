from collections import defaultdict
from pyspark import SparkContext
import datetime
import re

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
BASE_INPUT_PATH = "hdfs://master:54310/nifi/data/"
CSV_PATH = BASE_INPUT_PATH + "csv/"
HOURLY_CSV_PATH = CSV_PATH + "hourly/"
RESULT_CSV_PATH = BASE_INPUT_PATH + "result/"

def country_stats(sc, file_paths):
    list_rdd = []

    # Remove the header from each file
    for file in file_paths:
        rdd = sc.textFile(file)
        header = rdd.first()
        list_rdd.append(rdd.filter(lambda x: x != header))

    # Combine all files in a single RDD
    f_rdd = sc.union(list_rdd)

    # Map every row
    f_rdd = f_rdd.map(lambda x: x.split(","))

    # Ensure necessary values are not null
    f_rdd = f_rdd.filter(lambda fields: len(fields) > 6 and fields[4] and fields[6])

    map_by_year_rdd = f_rdd.map(lambda fields: (
        (fields[0][:4], fields[3]),             # (year from datetime, zone id)
        (float(fields[4]), float(fields[6]))    # (carbon intensity direct, carbon free percentage)
    ))

    # Map every value into: (carbon_mean, carbon_min, carbon_max, cfe_mean, cfe_min, cfe_max, count)
    #                            0            1           2           3        4        5       6
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
        str(kv[1][0] / kv[1][6]) + "," +    # carbon_mean = carbon_sum / count
        str(kv[1][1]) + "," +               # carbon_min
        str(kv[1][2]) + "," +               # carbon_max
        str(kv[1][3] / kv[1][6]) + "," +    # cfe_mean = cfe_sum / count
        str(kv[1][4]) + "," +               # cfe_min
        str(kv[1][5])                       # cfe_max
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
            # If no match is found
            files_by_country["unknown"].append(f)

    for country, flist in files_by_country.items():
        csv_rdd = country_stats(sc, flist)

        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = RESULT_CSV_PATH + f'query1_{country}_{timestamp}.csv'

        header = sc.parallelize(["date,zone_id,carbon_mean,carbon_min,carbon_max,cfe_mean,cfe_min,cfe_max"])

        output_rdd = header.union(csv_rdd)
        output_rdd.coalesce(1).saveAsTextFile(output_path)

        for row in output_rdd.take(10):
            print(row)


# Invochi main() solo se lo script è eseguito direttamente
if __name__ == "__main__":
    main()
