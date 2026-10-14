"""Tầng batch: CSV aggTrades -> (silver trades parquet) -> nến -> parquet + Postgres.

Dùng CHUNG build_candles với tầng streaming (C2).

    python -m bdi.batch.build_batch                         # mọi file aggTrades trong manifest
    python -m bdi.batch.build_batch --symbols BTCUSDT --periods 2022-11
    python -m bdi.batch.build_batch --naive-time --source batch_naive --no-db   # E1
"""

from __future__ import annotations

import argparse
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from bdi.candles import build_candles
from bdi.config import path
from bdi.ingest.download import load_manifest
from bdi.lineage import with_lineage
from bdi.schema import AGGTRADES_CSV_SCHEMA, canonical_from_aggtrades_csv
from bdi.sink import upsert_candles
from bdi.spark import get_spark


def select_files(kind: str, symbols: list[str] | None, periods: list[str] | None) -> list[dict]:
    files = [e for e in load_manifest().values() if e["kind"] == kind]
    if symbols:
        files = [e for e in files if e["symbol"] in symbols]
    if periods:
        files = [e for e in files if e["period"] in periods]
    if not files:
        raise SystemExit(f"Không có file {kind} nào trong manifest — chạy bdi.ingest.download trước.")
    return sorted(files, key=lambda e: (e["symbol"], e["period"]))


def read_trades(spark, entry: dict, naive_time: bool = False) -> DataFrame:
    raw = spark.read.csv(entry["csv"], schema=AGGTRADES_CSV_SCHEMA, header=False)
    return canonical_from_aggtrades_csv(raw, entry["symbol"], naive_time=naive_time)


def run(symbols=None, periods=None, naive_time=False, source="batch", write_db=True,
        write_silver=True) -> DataFrame:
    spark = get_spark("build_batch")
    lake = path("lake")
    parts = []
    silver = str(lake / "silver" / "trades")
    for e in select_files("aggTrades", symbols, periods):
        trades = read_trades(spark, e, naive_time)
        if write_silver and not naive_time:
            # CSV -> parquet một lần; E5/E6 và các lần chạy sau đọc parquet (nhanh hơn nhiều)
            (trades.withColumn("period", F.lit(e["period"]))
             .write.mode("overwrite").partitionBy("symbol", "period")
             .option("partitionOverwriteMode", "dynamic").parquet(silver))
            trades = (spark.read.parquet(silver)
                      .where((F.col("symbol") == e["symbol"]) & (F.col("period") == e["period"]))
                      .drop("period"))
        candles = with_lineage(build_candles(trades), source, e["sha256"])
        parts.append(candles.withColumn("period", F.lit(e["period"])))
        print(f"[batch] {e['symbol']} {e['period']}")

    out = reduce(DataFrame.unionByName, parts)
    (out.write.mode("overwrite").partitionBy("symbol", "period")
     .option("partitionOverwriteMode", "dynamic")
     .parquet(str(lake / "gold" / f"candles_{source}")))
    if write_db:
        upsert_candles(spark.read.parquet(str(lake / "gold" / f"candles_{source}")).drop("period"))
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", nargs="*")
    ap.add_argument("--periods", nargs="*", help="YYYY-MM hoặc YYYY-MM-DD")
    ap.add_argument("--naive-time", action="store_true", help="E1: coi mọi timestamp là ms")
    ap.add_argument("--source", default="batch")
    ap.add_argument("--no-db", action="store_true")
    a = ap.parse_args()
    run(a.symbols, a.periods, a.naive_time, a.source, not a.no_db)


if __name__ == "__main__":
    main()
