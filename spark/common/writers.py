from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from spark.common.config import settings


def configure_s3(spark: SparkSession, endpoint: str, access_key: str, secret_key: str) -> None:
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", endpoint)
    hadoop_conf.set("fs.s3a.access.key", access_key)
    hadoop_conf.set("fs.s3a.secret.key", secret_key)
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    hadoop_conf.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )


def delta_writer(
    df: DataFrame,
    checkpoint_location: str,
    output_path: str,
    mode: str = "append",
    partition_by: list[str] | None = None,
):
    writer = (
        df.writeStream.format("delta")
        .outputMode(mode)
        .option("checkpointLocation", checkpoint_location)
        .trigger(processingTime=settings.trigger_interval)
    )
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    return writer.start(output_path)
