# Spark and HDFS Runtime Contract

This directory pins the infrastructure used by the Spark-to-HDFS phase. The
versions are shared by Compose, submit scripts, local PySpark tests, and the
runbook; preview releases and floating `latest` tags are prohibited.

## Selected compatibility set

| Component | Pin | Rationale |
|---|---|---|
| Apache Spark / PySpark | 4.1.3 | Stable 4.1 maintenance release with Python 3.13 support |
| Scala binary | 2.13 | Spark 4 official binary line |
| Java | 17 | Supported Spark runtime and LTS baseline shared with Hadoop runner |
| Spark Kafka connector | `spark-sql-kafka-0-10_2.13:4.1.3` | Must match Spark and Scala exactly |
| Apache Hadoop | 3.5.0 | Current official Hadoop convenience image and HDFS runtime |

The Spark image includes Python and a Hadoop 3 client. The Hadoop image runs
the NameNode and DataNode services. Both images are Apache Software Foundation
images with exact tags.

## Authoritative references

- Spark releases and supported runtimes: <https://spark.apache.org/downloads/>
- PySpark 4.1.3 package and Python support: <https://pypi.org/project/pyspark/4.1.3/>
- Spark Docker image tags: <https://hub.docker.com/r/apache/spark/tags>
- Structured Streaming Kafka dependency coordinates:
  <https://spark.apache.org/docs/latest/streaming/structured-streaming-kafka-integration.html>
- Hadoop Docker documentation:
  <https://hadoop.apache.org/docs/r3.5.0/hadoop-project-dist/hadoop-common/HadoopDocker.html>
- Hadoop Docker image tags: <https://hub.docker.com/r/apache/hadoop/tags>

Run the prerequisite check from WSL before pulling images:

```bash
bash scripts/check-bigdata-prerequisites.sh --smoke
```

The full mode is intentionally stricter:

```bash
bash scripts/check-bigdata-prerequisites.sh
```
