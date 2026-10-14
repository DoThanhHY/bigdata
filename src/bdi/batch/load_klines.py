"""Nạp klines 1m chính thức (official) -> parquet + Postgres (source='official').

    python -m bdi.batch.load_klines
    python -m bdi.batch.load_klines --symbols BTCUSDT --periods 2026-10-14
"""

from __future__ import annotations

import argparse
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from bdi.batch.build_batch import select_files
from bdi.config import path
from bdi.lineage import with_lineage
from bdi.schema import KLINES_CSV_SCHEMA, official_from_klines_csv
from bdi.sink import upsert_candles
from bdi.spark import get_spark


def run(symbols=None, periods=None, write_db=True) -> DataFrame:
    spark = get_spark("load_klines")
    parts = []
    for e in select_files("klines", symbols, periods):
        raw = spark.read.csv(e["csv"], schema=KLINES_CSV_SCHEMA, header=False)
        k = official_from_klines_csv(raw, e["symbol"])
        # Các cột không có trong klines: để NULL cho khớp lược đồ bảng candles
        k = (k.withColumn("n_agg", F.lit(None).cast("long"))
             .withColumn("first_agg_id", F.lit(None).cast("long"))
             .withColumn("last_agg_id", F.lit(None).cast("long")))
        parts.append(with_lineage(k, "official", e["sha256"]).withColumn("period", F.lit(e["period"])))
    out = reduce(DataFrame.unionByName, parts)
    dest = str(path("lake") / "gold" / "candles_official")
    (out.write.mode("overwrite").partitionBy("symbol", "period")
     .option("partitionOverwriteMode", "dynamic").parquet(dest))
    if write_db:
        upsert_candles(spark.read.parquet(dest).drop("period"))
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", nargs="*")
    ap.add_argument("--periods", nargs="*")
    ap.add_argument("--no-db", action="store_true")
    a = ap.parse_args()
    run(a.symbols, a.periods, not a.no_db)


if __name__ == "__main__":
    main()
