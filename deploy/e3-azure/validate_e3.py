from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import numpy as np


ROOT = Path(__file__).resolve().parents[2]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-replay-lag", type=float, default=0.5)
    ap.add_argument(
        "--max-batch-utilization",
        type=float,
        default=1.0,
        help="p95 thoi gian micro-batch / trigger; >1 nghia la Spark thuong xuyen tut nhip",
    )
    a = ap.parse_args()

    cfg_dir = ROOT / "results" / "e3" / "configs"
    progress_dir = ROOT / "results" / "progress"
    log_dir = ROOT / "results" / "e3" / "logs"
    configs = [json.loads(p.read_text(encoding="utf-8")) for p in sorted(cfg_dir.glob("e3_wm*.json"))]
    if not configs:
        raise SystemExit("[invalid] khong co config E3")

    problems: list[str] = []
    print("run_id,batches,batch_p95_ms,late_dropped")
    for cfg in configs:
        rid = cfg["run_id"]
        p = progress_dir / f"{rid}.jsonl"
        if not p.exists():
            problems.append(f"{rid}: thieu progress")
            continue
        rows = [json.loads(line) for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]
        if not rows:
            problems.append(f"{rid}: progress rong")
            continue
        durations = [r.get("batch_duration_ms") or 0 for r in rows]
        late = sum(r.get("late_rows_dropped") or 0 for r in rows)
        p95_ms = float(np.percentile(durations, 95))
        print(f"{rid},{len(rows)},{p95_ms:.1f},{late}")
        trigger_ms = float(cfg["trigger_s"]) * 1000
        if trigger_ms and p95_ms > trigger_ms * a.max_batch_utilization:
            problems.append(
                f"{rid}: batch p95 {p95_ms:.1f}ms > "
                f"{a.max_batch_utilization:.2f} x trigger {trigger_ms:.0f}ms; Spark tut nhip"
            )

    lag_re = re.compile(r"([0-9]+(?:\.[0-9]+)?)s\s*$")
    lags: list[float] = []
    for p in log_dir.glob("e3_round*_replay.log"):
        for line in p.read_text(encoding="utf-8", errors="replace").splitlines():
            if "[replay]" not in line or "mục tiêu" not in line:
                continue
            match = lag_re.search(line)
            if match:
                lags.append(float(match.group(1)))
    if not lags:
        problems.append("khong doc duoc replay lag tu log")
    else:
        print(f"max_replay_lag_s={max(lags):.3f}")
        if max(lags) > a.max_replay_lag:
            problems.append(
                f"replay lag {max(lags):.3f}s > nguong {a.max_replay_lag:.3f}s; "
                "ket qua do tre E3 khong dang tin"
            )

    if problems:
        raise SystemExit("[invalid]\n- " + "\n- ".join(problems))
    print("[valid] du progress va replay giu nhip")


if __name__ == "__main__":
    main()
