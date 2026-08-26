from pyspark.sql import SparkSession


path = "hdfs://hdfs-namenode:8020/data/gds/_spark_smoke"
spark = SparkSession.builder.appName("gds-spark-hdfs-smoke").getOrCreate()

try:
    source = spark.createDataFrame([(1,), (2,), (3,)], ["value"])
    spark_count = source.count()
    print(f"spark_count={spark_count}")
    if spark_count != 3:
        raise RuntimeError(f"expected Spark count 3, got {spark_count}")

    source.write.mode("overwrite").parquet(path)
    hdfs_count = spark.read.parquet(path).count()
    print(f"hdfs_count={hdfs_count}")
    if hdfs_count != 3:
        raise RuntimeError(f"expected HDFS count 3, got {hdfs_count}")
finally:
    jvm = spark.sparkContext._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    smoke_path = jvm.org.apache.hadoop.fs.Path(path)
    smoke_path.getFileSystem(hadoop_conf).delete(smoke_path, True)
    spark.stop()

