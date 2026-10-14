"""C5 — Chính sách chọn nguồn: completeness / timeliness / consistency + time-to-convergence.

Tiền điều kiện:
  - stream job chạy với --output-mode update --source stream:update (ghi candle_versions)
  - official + batch của cùng ngày đã nạp

    python experiments/c5_source_policy.py --start 2026-10-14 --end 2026-10-15
"""

from __future__ import annotations

import argparse

import _common
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from bdi.metrics import emission_latency, pct, source_quality, time_to_convergence
from bdi.reconcile.compare import compare, load_candles


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--sources", nargs="+", default=["stream", "stream:update", "batch"])
    a = ap.parse_args()

    official = load_candles("official", start=a.start, end=a.end)
    rows = []
    for src in a.sources:
        c = load_candles(src, start=a.start, end=a.end)
        if c.empty:
            print(f"[skip] {src}: không có dữ liệu")
            continue
        m = compare(c, official, "outer")
        # batch được ghi T+1 nên "độ kịp thời" là thời điểm ghi so với cuối phút
        rows.append({"source": src, **source_quality(m, emission_latency(c))})
    q = pd.DataFrame(rows)
    q.to_csv(_common.out_dir("c5") / "source_quality.csv", index=False)
    print(q.to_string(index=False))

    ttc = time_to_convergence("stream:update")
    ttc = ttc[(ttc["minute"] >= pd.Timestamp(a.start, tz="UTC")) & (ttc["minute"] < pd.Timestamp(a.end, tz="UTC"))]
    if ttc.empty:
        print("[c5] chưa có candle_versions — chạy stream job với --output-mode update")
        return
    ttc.to_csv(_common.out_dir("c5") / "ttc.csv.gz", index=False)
    conv = ttc["ttc_s"].dropna()
    print(f"[c5] hội tụ {100 * len(conv) / len(ttc):.2f}% nến; TTC p50={pct(conv, 50):.1f}s p95={pct(conv, 95):.1f}s")

    fig, ax = plt.subplots(figsize=(6.5, 4))
    xs = np.sort(conv.to_numpy())
    ax.plot(xs, 100 * np.arange(1, len(xs) + 1) / len(ttc), color=_common.SERIES[0])
    ax.set_xlabel("thời gian từ cuối phút tới khi nến stream trùng official (s)")
    ax.set_ylabel("% nến đã hội tụ (tích lũy)")
    ax.set_title("C5 · Time-to-convergence của nến streaming")
    _common.save(fig, "c5", "ttc_cdf")


if __name__ == "__main__":
    main()
