"""E3 — Phân tích trade-off watermark: % khớp official vs độ trễ p50/p95.

    python experiments/e3_analyze.py

Đọc mọi cấu hình đã chạy từ results/e3/configs/*.json (e3_watermark_sweep.sh tạo ra):
mỗi cấu hình = (watermark, trigger, jitter). Các cấu hình cùng (trigger, jitter) tạo
thành một series trên hình.

Tiền điều kiện: official của khoảng replay đã nạp (load_klines hoặc rest_klines).

Hình: 2 panel CHUNG trục x (watermark) — trên: % khớp, dưới: độ trễ p95.
(Không dùng 2 trục y trên cùng một khung: hai đại lượng khác thang đo.)
"""

from __future__ import annotations

import json

import _common
import matplotlib.pyplot as plt
import pandas as pd

from bdi.config import path
from bdi.metrics import emission_latency, pct
from bdi.reconcile.compare import compare, load_candles


def analyze(cfg: dict) -> dict:
    rid = cfg["run_id"]
    meta = json.loads((path("results") / "replay" / f"{rid}.json").read_text())
    lo = pd.to_datetime(meta["origin_us"], unit="us", utc=True).ceil("min")
    span_us = (meta["replay_end_us"] - meta["replay_start_us"]) * meta["speed"]
    # bỏ phút đầu dở dang và phút cuối: phút cuối chỉ "đóng" khi có bản tin sau nó
    # >= W giây, mà replay dừng ở END => không đủ dữ liệu để đẩy watermark qua
    hi = pd.to_datetime(meta["origin_us"] + span_us, unit="us", utc=True).floor("min") - pd.Timedelta(minutes=1)
    stream = load_candles(cfg["source"], start=lo.isoformat(), end=hi.isoformat())
    official = load_candles("official", start=lo.isoformat(), end=hi.isoformat())
    m = compare(stream, official, "right")
    lat = emission_latency(stream, rid)
    return {
        **{k: cfg[k] for k in ("watermark_s", "trigger_s", "jitter_pct", "jitter_max_s")},
        "n_official": len(official), "n_stream": len(stream),
        "pct_match": 100 * m["all_match"].mean(),
        "pct_match_trades": 100 * m["match_trades"].mean(),
        "latency_p50_s": pct(lat, 50), "latency_p95_s": pct(lat, 95),
        "late_rows_dropped": _late_dropped(rid),
    }


def series_name(trigger_s, jitter_pct, jitter_max_s) -> str:
    t = f"trigger {trigger_s}s" if trigger_s else "trigger 0 (liên tục)"
    return t if not jitter_pct else f"{t} · {jitter_pct:g}% bản tin trễ ≤ {jitter_max_s:g}s"


def main() -> None:
    cfgs = [json.loads(p.read_text()) for p in sorted((path("results") / "e3" / "configs").glob("*.json"))]
    df = pd.DataFrame([analyze(c) for c in cfgs]).sort_values(
        ["jitter_pct", "trigger_s", "watermark_s"], ascending=[True, False, True])
    df.to_csv(_common.out_dir("e3") / "tradeoff.csv", index=False)
    print(df.to_string(index=False))

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(7, 7), sharex=True)
    keys = ["trigger_s", "jitter_pct", "jitter_max_s"]
    for i, (k, d) in enumerate(df.groupby(keys, sort=False)):
        name = series_name(*k)
        ax1.plot(d["watermark_s"], d["pct_match"], marker="o", color=_common.SERIES[i], label=name)
        ax2.plot(d["watermark_s"], d["latency_p95_s"], marker="o", color=_common.SERIES[i], label=name)
    ax1.set_ylabel("% nến khớp official")
    ax1.set_title("E3 · Watermark: độ đúng vs độ trễ")
    ax1.legend(frameon=False, fontsize=8)
    ax2.set_ylabel("độ trễ ghi nến p95 (s)")
    ax2.set_xlabel("watermark (s)")
    _common.save(fig, "e3", "tradeoff")


def _late_dropped(run_id: str) -> int | None:
    p = path("results") / "progress" / f"{run_id}.jsonl"
    if not p.exists():
        return None
    with open(p) as f:
        return sum(json.loads(line).get("late_rows_dropped") or 0 for line in f)


if __name__ == "__main__":
    main()
