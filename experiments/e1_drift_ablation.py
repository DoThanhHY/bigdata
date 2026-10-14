"""E1 — Ablation schema drift: naive (mọi timestamp là ms) vs drift-aware.

Tiền điều kiện: đã tải aggTrades + klines (bdi.ingest.download) cho ít nhất
1 tháng 2022 và 1 tháng 2026.

    python experiments/e1_drift_ablation.py                       # mọi file trong manifest
    python experiments/e1_drift_ablation.py --symbols BTCUSDT --periods 2022-11 2026-08

Đầu ra: results/e1/ablation.csv  (dòng = symbol × kỳ × phiên bản)
  pct_bad_ts     : % bản ghi có mốc vô lý (> năm 3000 hoặc < 2017)
  pct_candle_ok  : % nến official có nến tương ứng khớp tuyệt đối OHLCV + trades
"""

from __future__ import annotations

import argparse

import _common  # noqa: F401  (thiết lập sys.path)
import pandas as pd
from pyspark.sql import functions as F

from bdi.batch.build_batch import read_trades, select_files
from bdi.candles import build_candles
from bdi.schema import KLINES_CSV_SCHEMA, official_from_klines_csv
from bdi.spark import get_spark
from bdi.timeutil import YEAR_3000_US

YEAR_2017_US = 1_483_228_800 * 10**6
FIELDS = ["open", "high", "low", "close", "volume", "trades"]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", nargs="*")
    ap.add_argument("--periods", nargs="*")
    a = ap.parse_args()

    spark = get_spark("e1_drift_ablation")
    klines = {(e["symbol"], e["period"]): e for e in select_files("klines", a.symbols, a.periods)}
    rows = []
    for e in select_files("aggTrades", a.symbols, a.periods):
        k = klines.get((e["symbol"], e["period"]))
        official = None
        if k:
            official = official_from_klines_csv(
                spark.read.csv(k["csv"], schema=KLINES_CSV_SCHEMA), k["symbol"]).cache()
            n_off = official.count()
        for version, naive in (("naive", True), ("drift_aware", False)):
            t = read_trades(spark, e, naive_time=naive)
            stats = t.agg(
                F.count("*").alias("n"),
                F.sum(((F.col("ts_us") > YEAR_3000_US) | (F.col("ts_us") < YEAR_2017_US)).cast("int")).alias("bad"),
            ).first()
            row = {"symbol": e["symbol"], "period": e["period"], "version": version,
                   "n_records": stats["n"], "pct_bad_ts": 100 * stats["bad"] / stats["n"]}
            if official is not None:
                c = build_candles(t)
                j = official.alias("o").join(c.alias("c"), ["symbol", "minute"], "left")
                ok = F.lit(True)
                for f in FIELDS:
                    ok = ok & F.coalesce(F.col(f"o.{f}") == F.col(f"c.{f}"), F.lit(False))
                n_ok = j.where(ok).count()
                row.update({"n_official": n_off, "pct_candle_ok": 100 * n_ok / n_off})
            rows.append(row)
            print(row)

    df = pd.DataFrame(rows)
    out = _common.out_dir("e1") / "ablation.csv"
    df.to_csv(out, index=False)
    print(df.to_string(index=False))
    print(f"[ok] {out}")


if __name__ == "__main__":
    main()
