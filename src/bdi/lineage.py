"""C6 — Truy vết nguồn gốc từng cây nến.

Mỗi nến mang: source, file_checksum, ingested_at, watermark_config, pipeline_version.
  - batch:    file_checksum = sha256 của file zip đã tải (đã đối chiếu .CHECKSUM)
  - official: như batch, nhưng là file klines
  - stream:   file_checksum = "kafka:<topic>" ; watermark_config = "5s" ...
"""

from __future__ import annotations

import hashlib
from pathlib import Path

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from bdi.config import settings

LINEAGE_COLS = ["source", "file_checksum", "ingested_at", "watermark_config", "pipeline_version"]


def sha256_file(p: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        while b := f.read(chunk):
            h.update(b)
    return h.hexdigest()


def with_lineage(
    df: DataFrame,
    source: str,
    file_checksum: str | Column,
    watermark_config: str | None = None,
) -> DataFrame:
    chk = F.lit(file_checksum) if isinstance(file_checksum, str) else file_checksum
    return (
        df.withColumn("source", F.lit(source))
        .withColumn("file_checksum", chk)
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("watermark_config", F.lit(watermark_config).cast("string"))
        .withColumn("pipeline_version", F.lit(settings()["pipeline_version"]))
    )
