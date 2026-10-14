"""E5 — Record linkage mờ stream ↔ batch, chấm điểm bằng agg_id.

Phía stream: bản tin raw archive; thời gian dùng recv_ts_us (lúc NHẬN, có nhiễu mạng)
— nếu dùng trường T (trade time) thì Δt luôn = 0 vì cùng một nguồn, bài toán trở
nên tầm thường. Phía batch: aggTrades file theo ngày (silver parquet).

Tiền điều kiện:
    python -m bdi.ingest.download --kind aggTrades --days 2026-10-14
    python -m bdi.batch.build_batch --periods 2026-10-14 --no-db

    python experiments/e5_linkage.py --date 2026-10-14 --start 2026-10-14T09:00 --minutes 30
"""

from __future__ import annotations

import argparse
import time
from decimal import Decimal

import _common
import matplotlib.pyplot as plt
import pandas as pd
import pyarrow.dataset as ds

from bdi.config import path
from bdi.linkage.fuzzy import STRATEGIES, link, prepare, score
from bdi.reconcile.compare import load_raw

DELTAS_MS = [10, 50, 100, 500, 1000]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--start", required=True)
    ap.add_argument("--minutes", type=int, default=30)
    ap.add_argument("--symbols", nargs="*", default=["BTCUSDT", "ETHUSDT"])
    ap.add_argument("--stream-time", choices=["recv", "event"], default="recv")
    ap.add_argument("--price-bucket", type=float, default=1.0)
    a = ap.parse_args()

    t0 = pd.Timestamp(a.start, tz="UTC")
    t1 = t0 + pd.Timedelta(minutes=a.minutes)

    raw = load_raw(t0.isoformat(), t1.isoformat())
    raw = raw[raw["symbol"].isin(a.symbols)].drop_duplicates(["symbol", "agg_id"])
    stream = raw.assign(
        ts_us=raw["recv_ts_us"] if a.stream_time == "recv" else raw["ts_us"],
        price=raw["p"].map(Decimal),
        qty=raw["q"].map(Decimal),
    )
    s = prepare(stream, "s")
    b = prepare(_load_batch(t0, t1, a.symbols), "b")
    print(f"[e5] stream={len(s):,} batch={len(b):,}")

    rows = []
    for d_ms in DELTAS_MS:
        for strat in STRATEGIES:
            for method in ("greedy", "ordered"):
                if method == "ordered" and strat != "second":
                    continue  # ordered không phụ thuộc blocking; chỉ chạy một lần
                tic = time.perf_counter()
                matches, n_cmp = link(s, b, d_ms * 1000, strat, a.price_bucket, method=method)
                sc = score(matches, s, b)
                rows.append({"delta_ms": d_ms, "strategy": strat, "method": method, "pairs_compared": n_cmp,
                             "seconds": time.perf_counter() - tic, **sc})
                print(rows[-1])
    df = pd.DataFrame(rows)
    df.to_csv(_common.out_dir("e5") / "linkage.csv", index=False)

    # Hình 1: Precision–Recall theo δ (blocking 'second'; mọi blocking cho cùng kết quả ghép)
    fig, ax = plt.subplots(figsize=(6, 4.8))
    for i, (method, label) in enumerate((("greedy", "tham lam theo |Δt|"), ("ordered", "giữ thứ tự (2 con trỏ)"))):
        d = df[(df["strategy"] == "second") & (df["method"] == method)].sort_values("delta_ms")
        ax.plot(d["recall"], d["precision"], marker="o", color=_common.SERIES[i], label=label)
        for r in d.itertuples():
            ax.annotate(f"{r.delta_ms}ms", (r.recall, r.precision), textcoords="offset points",
                        xytext=(4, 6) if r.delta_ms == 1000 else (4, -12), color=_common.TEXT_2, fontsize=8)
    ax.legend(frameon=False, loc="center right")
    ax.set_xlabel("Recall")
    ax.set_ylabel("Precision")
    ax.set_title("E5 · Precision–Recall theo dung sai thời gian δ")
    _common.save(fig, "e5", "pr_curve")

    # Hình 2: chi phí (số cặp so sánh) theo chiến lược blocking, thang log
    fig, ax = plt.subplots(figsize=(6.5, 4))
    for i, strat in enumerate(STRATEGIES):
        d = df[(df["strategy"] == strat) & (df["method"] == "greedy")].sort_values("delta_ms")
        ax.plot(d["delta_ms"], d["pairs_compared"], marker="o", color=_common.SERIES[i], label=strat)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("δ (ms)")
    ax.set_ylabel("số cặp ứng viên phải so sánh")
    ax.set_title("E5 · Chi phí theo chiến lược blocking")
    ax.legend(frameon=False)
    _common.save(fig, "e5", "blocking_cost")


def _load_batch(t0, t1, symbols) -> pd.DataFrame:
    """Phía batch từ silver trades (file theo ngày)."""
    t = ds.dataset(path("lake") / "silver" / "trades", format="parquet", partitioning="hive").to_table(
        filter=ds.field("symbol").isin(symbols)
        & (ds.field("ts_us") >= int(t0.value // 1000)) & (ds.field("ts_us") < int(t1.value // 1000)),
        columns=["symbol", "agg_id", "price", "qty", "ts_us"])
    return t.to_pandas()


if __name__ == "__main__":
    main()
