"""E1 — Kiểm kết quả theo tiêu chí đạt (chạy TRƯỚC khi commit kết quả mới).

    python experiments/e1_check.py

So results/e1/ablation.csv với bản đang commit (git show HEAD:results/e1/ablation.csv),
in từng tiêu chí ĐẠT/CHƯA và trả mã lỗi 1 nếu có tiêu chí chưa đạt.
"""

from __future__ import annotations

import io
import subprocess
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
TOTAL_RECORDS = 418_156_831  # tổng aggTrades 5 cặp × 4 tháng (docs/02-roadmap.md)
N_OFFICIAL = {"2022-11": 43_200, "2026-06": 43_200, "2026-07": 44_640, "2026-08": 44_640}
NUM_COLS = ["n_records", "pct_bad_ts", "n_official", "pct_candle_ok"]


def expected(row: pd.Series, col: str) -> float:
    broken = row["version"] == "naive" and row["period"] >= "2025"
    if col == "pct_candle_ok":
        return 0.0 if broken else 100.0
    return 100.0 if broken else 0.0  # pct_bad_ts


def main() -> int:
    new = pd.read_csv(ROOT / "results" / "e1" / "ablation.csv")
    old_csv = subprocess.check_output(["git", "-C", str(ROOT), "show", "HEAD:results/e1/ablation.csv"], text=True)
    old = pd.read_csv(io.StringIO(old_csv))

    checks = [("Đúng 40 dòng (5 cặp × 4 tháng × 2 phiên bản)", len(new) == 40, f"{len(new)} dòng")]

    total = int(new.loc[new["version"] == "drift_aware", "n_records"].sum())
    checks.append(("Tổng n_records của dòng drift-aware = 418 156 831", total == TOTAL_RECORDS,
                   f"{total:,}".replace(",", " ")))

    wrong_off = [f"{r.symbol} {r.period}" for r in new.itertuples()
                 if r.period in N_OFFICIAL and r.n_official != N_OFFICIAL[r.period]]
    checks.append(("n_official đủ phút của tháng", not wrong_off, ", ".join(wrong_off) or "mọi dòng đúng"))

    for col in ("pct_candle_ok", "pct_bad_ts"):
        bad = [f"{r['symbol']} {r['period']} {r['version']}={r[col]}" for _, r in new.iterrows()
               if r[col] != expected(r, col)]
        checks.append((f"{col} đúng kỳ vọng (naive hỏng 2026, còn lại đúng)", not bad, ", ".join(bad) or "mọi dòng đúng"))

    keys = ["symbol", "period", "version"]
    m = old.merge(new, on=keys, suffixes=("_cu", "_moi"))
    diff = [f"{r.symbol} {r.period} {r.version}" for r in m.itertuples()
            if any(getattr(r, f"{c}_cu") != getattr(r, f"{c}_moi") for c in NUM_COLS)]
    checks.append(("Các dòng của lần chạy 13/10 trùng từng số", len(m) == len(old) and not diff,
                   f"{len(m)}/{len(old)} dòng cũ có trong file mới" + (f"; lệch: {', '.join(diff)}" if diff else "")))

    for name, ok, detail in checks:
        print(f"[{'ĐẠT ' if ok else 'CHƯA'}] {name}: {detail}")
    return 0 if all(ok for _, ok, _ in checks) else 1


if __name__ == "__main__":
    sys.exit(main())
