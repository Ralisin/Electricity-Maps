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
