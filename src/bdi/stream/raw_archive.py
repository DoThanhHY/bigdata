"""Lưu nguyên văn mọi bản tin Kafka xuống parquet (bronze).

Đây là "băng ghi hình" của luồng live — cần cho:
  - E3: phát lại có nhịp với nhiều watermark (bdi.ingest.replay --from-archive)
  - E5: bản ghi phía stream để ghép mờ với batch
  - C3: mô phỏng lại bản tin nào bị loại vì trễ (LATE_BEYOND_WATERMARK)

    python -m bdi.stream.raw_archive                       # topic live
"""

from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from bdi.config import path, settings
from bdi.spark import get_spark


def main() -> None:
    cfg = settings()
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", default=cfg["kafka"]["topic_live"])
    ap.add_argument("--starting-offsets", default="earliest")
    a = ap.parse_args()

    spark = get_spark("raw_archive", kafka=True)
    src = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", cfg["kafka"]["bootstrap"])
           .option("subscribe", a.topic)
           .option("startingOffsets", a.starting_offsets)
           .option("failOnDataLoss", "false")
           .load())
    out = src.select(
        F.col("key").cast("string").alias("symbol"),
        F.col("value").cast("string").alias("value"),
        F.get_json_object(F.col("value").cast("string"), "$.recv_ts_us").cast("long").alias("recv_ts_us"),
        F.col("timestamp").alias("kafka_ts"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
    ).withColumn("date", F.to_date("kafka_ts"))

    name = a.topic.replace(".", "_")
    q = (out.writeStream.format("parquet")
         .option("path", str(path("lake") / "bronze" / "raw_aggtrades"))
         .option("checkpointLocation", str(path("checkpoints") / f"raw_archive_{name}"))
         .partitionBy("date")
         .trigger(processingTime="30 seconds")
         .start())
    q.awaitTermination()


if __name__ == "__main__":
    main()
