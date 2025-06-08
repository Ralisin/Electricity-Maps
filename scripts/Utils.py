from datetime import datetime
from influxdb_client import BucketRetentionRules, InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS, WritePrecision


def combine_into_single_rdd(sc, file_paths):
    """
    Load multiple text files into a single Spark RDD, removing the header from each file.

    Parameters:
    -----------
    sc : pyspark.SparkContext
        The Spark context used to create RDDs.
    file_paths : list of str
        List of file paths to be loaded.

    Returns:
    --------
    pyspark.RDD
        An RDD containing all rows from all files, excluding the headers,
        where each row is represented as a list of strings split by commas.

    Description:
    ------------
    For each file in the list, the function:
      - reads the file into an RDD,
      - extracts the first line as the header,
      - filters out all lines equal to the header,
    then combines all RDDs into a single RDD and maps each line by splitting it at commas.

    Example usage:
    --------------
    >>> sc = SparkContext()
    >>> files = ["file1.csv", "file2.csv"]
    >>> rdd = combine_into_single_rdd(sc, files)
    """

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

    return f_rdd

def save_rdd(rdd, path, header=None, sc=None):
    """
    Saves an RDD as a single CSV file, optionally with a header.

    :param rdd: The RDD to be saved.
    :param path: Final output path, including the desired filename (e.g., "/path/to/output.csv").
    :param header: (Optional) A CSV-formatted string to be used as header.
    :param sc: The active SparkContext, required for filesystem operations.

    :return: The (possibly modified) RDD that was saved.
    """
    if sc is None:
        raise ValueError("SparkContext (sc) must be provided")

    tmp_path = path + "_tmp"

    # Prepend the header if provided
    if header is not None:
        header_rdd = sc.parallelize([header])
        rdd = header_rdd.union(rdd)

    # Save the RDD to a temporary directory with a single output file
    rdd.coalesce(1).saveAsTextFile(tmp_path)

    # Use the Hadoop FileSystem API to move the file and clean up
    hadoop_conf = sc._jsc.hadoopConfiguration()
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    temp_path = sc._jvm.org.apache.hadoop.fs.Path(tmp_path)
    final_path = sc._jvm.org.apache.hadoop.fs.Path(path)

    # Delete existing output file if present
    if fs.exists(final_path):
        fs.delete(final_path, False)

    # Look for the generated part file and rename it to the desired final path
    for status in fs.listStatus(temp_path):
        name = status.getPath().getName()
        if name.startswith("part-"):
            fs.rename(status.getPath(), final_path)
            break

    # Delete the temporary directory
    fs.delete(temp_path, True)

    return rdd

def normalize_column_names(df):
    """
    Renames columns in the given DataFrame to a standardized format for consistency across data sources.

    This function is particularly useful when switching between different input formats (e.g., CSV and Parquet)
    that may have differing column naming conventions. It renames columns with underscores and simplified names
    (typically found in Parquet files) to the original column names used in CSV files.

    Parameters:
        df (pyspark.sql.DataFrame): The input DataFrame whose columns need to be normalized.

    Returns:
        pyspark.sql.DataFrame: A DataFrame with renamed columns, matching the expected standard names.

    Columns renamed (if present):
        - "Datetime__UTC_" -> "Datetime (UTC)"
        - "Zone_name" -> "Zone name"
        - "Carbon_intensity_gCO_eq_kWh__direct_" -> "Carbon intensity gCO₂eq/kWh (direct)"
        - "Carbon_free_energy_percentage__CFE__" -> "Carbon-free energy percentage (CFE%)"
        """
    rename_map = {
        "Datetime__UTC_": "Datetime (UTC)",
        "Zone_name": "Zone name",
        "Carbon_intensity_gCO_eq_kWh__direct_": "Carbon intensity gCO₂eq/kWh (direct)",
        "Carbon_free_energy_percentage__CFE__": "Carbon-free energy percentage (CFE%)"
    }

    for old_name, new_name in rename_map.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)

    return df

def create_bucket_if_not_exists(influx_url, token, org, bucket_name, retention_days=0):
    """
    Crea un bucket InfluxDB se non esiste.

    Args:
        influx_url (str): URL di InfluxDB (es. "http://localhost:8086")
        token (str): token di autenticazione
        org (str): organizzazione InfluxDB
        bucket_name (str): nome del bucket da creare
        retention_days (int, optional): giorni di retention dati (default 30). 0 = retention illimitata.

    Returns:
        bucket: oggetto bucket creato o esistente
    """
    client = InfluxDBClient(url=influx_url, token=token, org=org)
    buckets_api = client.buckets_api()

    # Controlla se il bucket esiste
    existing_buckets = buckets_api.find_buckets().buckets
    bucket = next((b for b in existing_buckets if b.name == bucket_name), None)

    if bucket is None:
        print(f"Bucket '{bucket_name}' non trovato. Creo nuovo bucket...")
        retention_seconds = retention_days * 24 * 60 * 60 if retention_days > 0 else 0
        retention_rule = BucketRetentionRules(type="expire",
                                              every_seconds=retention_seconds) if retention_seconds > 0 else None
        bucket = buckets_api.create_bucket(bucket_name=bucket_name, org=org, retention_rules=retention_rule)
        print(f"Bucket '{bucket_name}' creato con retention {retention_days} giorni.")
    else:
        print(f"Bucket '{bucket_name}' già esistente.")

    client.close()
    return bucket

def write_reduced_to_influxdb_q1(reduced_rdd, influx_url, token, org, bucket):
    create_bucket_if_not_exists(influx_url, token, org, bucket)

    client = InfluxDBClient(url=influx_url, token=token, org=org)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    def to_influx_point(kv):
        (year, zone_id), (carbon_sum, carbon_min, carbon_max, cfe_sum, cfe_min, cfe_max, count) = kv
        timestamp = datetime(int(year) - 1, 1, 1)
        # timestamp = datetime.strptime(year, "%Y")

        carbon_mean = carbon_sum / count
        cfe_mean = cfe_sum / count

        return Point("carbon_data") \
            .tag("zone_id", zone_id) \
            .field("carbon_mean", carbon_mean) \
            .field("carbon_min", carbon_min) \
            .field("carbon_max", carbon_max) \
            .field("cfe_mean", cfe_mean) \
            .field("cfe_min", cfe_min) \
            .field("cfe_max", cfe_max) \
            .time(timestamp, WritePrecision.NS)

    points = reduced_rdd.map(to_influx_point).collect()
    write_api.write(bucket=bucket, org=org, record=points)

    client.close()

def write_reduced_to_influxdb_q2(reduced_rdd, influx_url, token, org, bucket):
    create_bucket_if_not_exists(influx_url, token, org, bucket)

    client = InfluxDBClient(url=influx_url, token=token, org=org)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    def to_influx_point(kv):
        date_str, (carbon_sum, cfe_sum, count) = kv
        try:
            # Parsing "2022_12" → datetime(2022, 12, 1)
            timestamp = datetime.strptime(date_str, "%Y_%m")
        except ValueError:
            # In caso di errore, salta o imposta data di default
            timestamp = datetime(1970, 1, 1)

        carbon_mean = carbon_sum / count
        cfe_mean = cfe_sum / count

        return Point("monthly_carbon_stats") \
            .field("carbon_mean", carbon_mean) \
            .field("cfe_mean", cfe_mean) \
            .time(timestamp, WritePrecision.NS)

    points = reduced_rdd.map(to_influx_point).collect()
    write_api.write(bucket=bucket, org=org, record=points)