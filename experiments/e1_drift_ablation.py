"""E1 — Ablation schema drift: naive (mọi timestamp là ms) vs drift-aware.

Tiền điều kiện: đã tải aggTrades + klines (bdi.ingest.download) cho ít nhất
1 tháng 2022 và 1 tháng 2026.

    # bản đầy đủ: 5 cặp × 4 tháng × 2 phiên bản = 40 dòng
    python experiments/e1_drift_ablation.py --symbols BTCUSDT ETHUSDT BNBUSDT SOLUSDT XRPUSDT \
        --periods 2022-11 2026-06 2026-07 2026-08
    python experiments/e1_drift_ablation.py --symbols SOLUSDT --periods 2026-08    # chạy thử
    python experiments/e1_drift_ablation.py --plot-only                            # chỉ vẽ lại hình

Luôn truyền --periods: thiếu nó thì script lấy MỌI file aggTrades trong manifest,
kể cả file theo ngày (13/10, ngày D của C5) trên máy dữ liệu.

Đầu ra: results/e1/ablation.csv  (dòng = symbol × kỳ × phiên bản)
  pct_bad_ts       : % bản ghi có mốc vô lý (> năm 3000, < 2017, hoặc không suy được đơn vị)
  pct_candle_ok    : % nến official có nến tương ứng khớp tuyệt đối OHLCV + trades
  raw_digits       : số chữ số của timestamp thô trong file (13 = ms, 16 = µs) — hồ sơ nguồn
  first_ts, last_ts: mốc đầu/cuối của file theo bản drift-aware (bản naive để trống)
và results/e1/ablation.png (cột nhóm theo kỳ, gộp mọi cặp có trong CSV) cho slide 6.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

import _common
import matplotlib.pyplot as plt
import pandas as pd

YEAR_2017_US = 1_483_228_800 * 10**6
FIELDS = ["open", "high", "low", "close", "volume", "trades"]
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _iso(us: int | None) -> str | None:
    return None if us is None else (EPOCH + timedelta(microseconds=us)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def run(symbols: list[str] | None, periods: list[str] | None) -> pd.DataFrame:
    # Import Spark ở đây để --plot-only chạy được trên máy không có Java/pyspark
    from pyspark.sql import functions as F

    from bdi.batch.build_batch import read_trades, select_files
    from bdi.candles import build_candles
    from bdi.schema import KLINES_CSV_SCHEMA, official_from_klines_csv
    from bdi.spark import get_spark
    from bdi.timeutil import YEAR_3000_US

    spark = get_spark("e1_drift_ablation")
    klines = {(e["symbol"], e["period"]): e for e in select_files("klines", symbols, periods)}
    rows = []
    for e in select_files("aggTrades", symbols, periods):
        k = klines.get((e["symbol"], e["period"]))
        official = None
        if k:
            official = official_from_klines_csv(
                spark.read.csv(k["csv"], schema=KLINES_CSV_SCHEMA), k["symbol"]).cache()
            n_off = official.count()
        raw_digits = None
        for version, naive in (("naive", True), ("drift_aware", False)):
            t = read_trades(spark, e, naive_time=naive)
            ts = F.col("ts_us")
            # NULL = to_micros_col không suy được đơn vị (giá trị < 10^9): cũng là mốc vô lý
            bad = (ts > YEAR_3000_US) | (ts < YEAR_2017_US) | ts.isNull()
            stats = t.agg(
                F.count("*").alias("n"),
                F.sum(bad.cast("int")).alias("bad"),
                F.min(ts).alias("min_ts"),
                F.max(ts).alias("max_ts"),
            ).first()
            if naive and stats["max_ts"] is not None:
                # bản naive nhân mọi giá trị với 1000, nên chia lại là ra timestamp thô
                raw_digits = len(str(abs(stats["max_ts"]) // 1000))
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
            row.update({"raw_digits": raw_digits,
                        "first_ts": None if naive else _iso(stats["min_ts"]),
                        "last_ts": None if naive else _iso(stats["max_ts"])})
            rows.append(row)
            print(row)
    return pd.DataFrame(rows)


def plot(df: pd.DataFrame) -> None:
    """Cột nhóm: % nến đúng theo kỳ, naive vs drift-aware, gộp mọi cặp có trong CSV."""
    d = df.dropna(subset=["pct_candle_ok"]).copy()
    d["n_ok"] = d["pct_candle_ok"] * d["n_official"] / 100
    g = (d.groupby(["period", "version"])
         .agg(n_ok=("n_ok", "sum"), n_off=("n_official", "sum"))
         .reset_index())
    g["pct"] = 100 * g["n_ok"] / g["n_off"]
    periods = sorted(g["period"].unique())
    series = [("naive", "Naive (luôn coi là ms)", _common.SERIES[1]),
              ("drift_aware", "Drift-aware (suy theo độ lớn)", _common.SERIES[0])]

    fig, ax = plt.subplots(figsize=(2.2 + 1.45 * len(periods), 3.9))
    w = 0.34
    for k, (version, label, color) in enumerate(series):
        for i, p in enumerate(periods):
            v = g.loc[(g["period"] == p) & (g["version"] == version), "pct"]
            if v.empty:
                continue
            v, x = float(v.iloc[0]), i + (k - 0.5) * (w + 0.03)
            ax.bar(x, v, width=w, color=color, label=label if i == 0 else None, zorder=2)
            if v < 0.5:  # cột 0% không có chiều cao: vẽ vạch ở đáy để vẫn thấy
                ax.hlines(0, x - w / 2, x + w / 2, color=color, linewidth=3, zorder=3)
            ax.text(x, v + 2, f"{v:.0f}%", ha="center", va="bottom", color=_common.TEXT_2, fontsize=9)
    ax.set_xticks(range(len(periods)), [f"{p}\n{'ms' if p < '2025' else 'µs'}" for p in periods])
    ax.set_ylim(0, 115)
    ax.set_yticks([0, 25, 50, 75, 100], [f"{v}%" for v in (0, 25, 50, 75, 100)])
    ax.set_ylabel("% nến khớp tuyệt đối official")
    ax.grid(axis="x", visible=False)
    ax.set_axisbelow(True)
    n_off = int(g.loc[g["version"] == "drift_aware", "n_off"].sum())
    syms = sorted(d["symbol"].unique())
    ax.set_title("E1 · Bỏ qua schema drift: mất toàn bộ nến từ 2025", loc="left", fontsize=11, pad=22)
    n_txt = f"{n_off:,}".replace(",", " ")
    ax.text(0, 1.025, f"{len(syms)} cặp ({', '.join(s.replace('USDT', '') for s in syms)}) · {n_txt} nến official",
            transform=ax.transAxes, color=_common.TEXT_2, fontsize=8.5, va="bottom")
    ax.legend(frameon=False, loc="upper center", bbox_to_anchor=(0.5, -0.2), ncol=2, fontsize=9)
    _common.save(fig, "e1", "ablation")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", nargs="*")
    ap.add_argument("--periods", nargs="*")
    ap.add_argument("--plot-only", action="store_true", help="chỉ vẽ lại hình từ results/e1/ablation.csv")
    a = ap.parse_args()

    out = _common.out_dir("e1") / "ablation.csv"
    if a.plot_only:
        plot(pd.read_csv(out))
        return
    df = run(a.symbols, a.periods)
    df.to_csv(out, index=False)
    print(df.to_string(index=False))
    print(f"[ok] {out}")
    plot(df)


if __name__ == "__main__":
    main()
