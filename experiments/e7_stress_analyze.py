"""E7 stress test — throughput và độ trễ cho mỗi lần replay ở tốc độ k×.

    python experiments/e7_stress_analyze.py --run-id e7_x100     # 1 lần chạy
    python experiments/e7_stress_analyze.py --summary            # gộp tất cả + vẽ hình

Độ trễ quy về đồng hồ thật bằng metadata replay (bdi.metrics.emission_latency).
Throughput = max(processedRowsPerSecond) trong results/progress/<run_id>.jsonl.
"""

from __future__ import annotations

import argparse
import json

import _common
import matplotlib.pyplot as plt
import pandas as pd

from bdi.config import path
from bdi.metrics import emission_latency, pct
from bdi.reconcile.compare import load_candles


def one(run_id: str) -> dict:
    meta = json.loads((path("results") / "replay" / f"{run_id}.json").read_text())
    c = load_candles(f"stream:{run_id}")
    lat = emission_latency(c, run_id)
    prog = pd.read_json(path("results") / "progress" / f"{run_id}.jsonl", lines=True)
    busy = prog[prog["num_input_rows"] > 0]
    res = {
        "run_id": run_id, "speed": meta["speed"], "messages": meta["messages"],
        "offered_msg_per_s": meta["messages"] / max((meta["replay_end_us"] - meta["replay_start_us"]) / 1e6, 1e-9),
        "max_processed_rows_per_s": float(busy["processed_rows_per_second"].max()) if len(busy) else None,
        "latency_p50_s": pct(lat, 50), "latency_p95_s": pct(lat, 95),
    }
    (_common.out_dir("e7") / f"stress_{run_id}.json").write_text(json.dumps(res, indent=2))
    print(res)
    return res


def summary() -> None:
    rows = [json.loads(p.read_text()) for p in sorted(_common.out_dir("e7").glob("stress_e7_x*.json"))]
    df = pd.DataFrame(rows).sort_values("speed")
    df.to_csv(_common.out_dir("e7") / "stress.csv", index=False)
    print(df.to_string(index=False))
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(6.5, 6), sharex=True)
    ax1.plot(df["speed"], df["offered_msg_per_s"], marker="o", color=_common.SERIES[0], label="tải đưa vào")
    ax1.plot(df["speed"], df["max_processed_rows_per_s"], marker="s", color=_common.SERIES[1], label="xử lý tối đa")
    ax1.set_ylabel("bản tin / giây")
    ax1.set_yscale("log")
    ax1.legend(frameon=False)
    ax1.set_title("E7 · Stress test velocity (replay FTX)")
    ax2.plot(df["speed"], df["latency_p95_s"], marker="o", color=_common.SERIES[0])
    ax2.axhline(10, color=_common.TEXT_2, linestyle=":", linewidth=1)
    ax2.annotate("ngưỡng 10s", (df["speed"].min(), 10), textcoords="offset points", xytext=(4, 4),
                 color=_common.TEXT_2, fontsize=9)
    ax2.set_ylabel("độ trễ p95 (s)")
    ax2.set_xlabel("tốc độ replay (×)")
    ax2.set_xscale("log")
    _common.save(fig, "e7", "stress")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id")
    ap.add_argument("--summary", action="store_true")
    a = ap.parse_args()
    if a.run_id:
        one(a.run_id)
    if a.summary:
        summary()


if __name__ == "__main__":
    main()
