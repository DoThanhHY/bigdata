"""Khởi tạo SparkSession thống nhất cho batch, streaming và test."""

from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession

SPARK_VERSION = "4.0.4"
KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{SPARK_VERSION}"


def get_spark(app: str, kafka: bool = False, master: str | None = None) -> SparkSession:
    # executor (Python UDF, foreachPartition) phải dùng cùng interpreter/venv với driver
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    b = (
        SparkSession.builder.appName(app)
        .master(master or os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "16"))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.ui.showConsoleProgress", "false")
    )
    if kafka:
        b = b.config("spark.jars.packages", KAFKA_PACKAGE)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
