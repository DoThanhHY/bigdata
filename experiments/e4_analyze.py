"""E4 — Phân tích fault injection: thời gian phục hồi, nến thiếu/trùng, timeline.

Tiền điều kiện: official của (các) ngày chạy chaos đã nạp:
    python -m bdi.ingest.download --kind klines --days 2026-10-14
    python -m bdi.batch.load_klines --periods 2026-10-14

    python experiments/e4_analyze.py [--source stream]

Với mỗi sự cố (cặp inject/recover trong chaos_events), xét cửa sổ
[inject − 5 phút, recover + 10 phút]:
  recovery_s      : từ lúc recover đến khi có nến stream MỚI đầu tiên được ghi
  missing_minutes : phút official có mà stream không có
  extra_trades    : phút stream có trades > official (dấu hiệu trùng)
  wrong_minutes   : phút có cả hai nhưng lệch
"""

from __future__ import annotations

import argparse

import _common
import matplotlib.pyplot as plt
import pandas as pd

from bdi.reconcile.compare import compare, load_candles
from bdi.sink import query


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default="stream")
    a = ap.parse_args()

    ev = pd.DataFrame(query("SELECT id, scenario, phase, at FROM chaos_events ORDER BY at"),
                      columns=["id", "scenario", "phase", "at"])
    ev["at"] = pd.to_datetime(ev["at"], utc=True)
    gaps = pd.DataFrame(query("SELECT symbol, disconnected_at, reconnected_at, backfilled, reason FROM ws_gaps"),
                        columns=["symbol", "disconnected_at", "reconnected_at", "backfilled", "reason"])

    incidents = []
    pending = {}
    for r in ev.itertuples():
        if r.phase == "inject":
            pending[r.scenario] = r.at
        elif r.phase == "recover" and r.scenario in pending and r.scenario != "batch_rerun":
            incidents.append((r.scenario, pending.pop(r.scenario), r.at))

    rows = []
    for scen, t_in, t_rec in incidents:
        lo, hi = t_in - pd.Timedelta(minutes=5), t_rec + pd.Timedelta(minutes=10)
        s = load_candles(a.source, start=lo.isoformat(), end=hi.isoformat())
        o = load_candles("official", start=lo.isoformat(), end=hi.isoformat())
        m = compare(s, o, "outer")
        em = pd.to_datetime(s["emitted_at"], utc=True)
        after = em[em > t_rec]
        g = gaps[(pd.to_datetime(gaps["disconnected_at"], utc=True) >= lo)
                 & (pd.to_datetime(gaps["disconnected_at"], utc=True) <= hi)]
        rows.append({
            "scenario": scen, "inject": t_in, "recover": t_rec,
            "down_s": (t_rec - t_in).total_seconds(),
            "recovery_s": (after.min() - t_rec).total_seconds() if len(after) else None,
            "official_minutes": int(m["trades_r"].notna().sum()),
            "missing_minutes": int((m["trades_l"].isna() & m["trades_r"].notna()).sum()),
            "extra_trades_minutes": int((m["trades_l"] > m["trades_r"]).sum()),
            "wrong_minutes": int((m["trades_l"].notna() & m["trades_r"].notna() & ~m["all_match"]).sum()),
            "backfilled_records": int(g["backfilled"].sum()) if len(g) else 0,
        })
    df = pd.DataFrame(rows)
    df.to_csv(_common.out_dir("e4") / "incidents.csv", index=False)
    print(df.to_string(index=False))

    # Timeline: số trade/phút stream vs official cho BTCUSDT, tô vùng sự cố
    for i, (scen, t_in, t_rec) in enumerate(incidents):
        lo, hi = t_in - pd.Timedelta(minutes=5), t_rec + pd.Timedelta(minutes=10)
        s = load_candles(a.source, ["BTCUSDT"], lo.isoformat(), hi.isoformat()).sort_values("minute")
        o = load_candles("official", ["BTCUSDT"], lo.isoformat(), hi.isoformat()).sort_values("minute")
        fig, ax = plt.subplots(figsize=(8, 3.2))
        ax.plot(o["minute"], o["trades"], color=_common.SERIES[0], label="official")
        ax.plot(s["minute"], s["trades"], color=_common.SERIES[1], linestyle="--", label=a.source)
        ax.axvspan(t_in, t_rec, color=_common.GRID, alpha=0.9, label="sự cố")
        ax.set_ylabel("số trade / phút (BTCUSDT)")
        ax.set_title(f"E4 · {scen}")
        ax.legend(frameon=False, loc="upper left")
        _common.save(fig, "e4", f"timeline_{i:02d}_{scen}")


if __name__ == "__main__":
    main()
