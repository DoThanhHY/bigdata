"""E1 — In các bảng markdown từ results/e1/ablation.csv để dán vào báo cáo và slide.

    python experiments/e1_tables.py

Bảng 1 · E1 gộp theo kỳ × phiên bản (báo cáo C1, slide 6).
Bảng 2 · Hồ sơ nguồn theo file: số dòng, dung lượng zip, số chữ số timestamp, khoảng thời gian
         (báo cáo C1, chương dữ liệu, slide 3). Dung lượng zip đọc từ manifest đã tải.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bdi.ingest.download import load_manifest  # noqa: E402


def n(x: float) -> str:
    return f"{x:,.0f}".replace(",", " ")


def main() -> None:
    d = pd.read_csv(ROOT / "results" / "e1" / "ablation.csv")
    d["n_ok"] = d["pct_candle_ok"] * d["n_official"] / 100
    d["n_bad"] = d["pct_bad_ts"] * d["n_records"] / 100

    g = (d.groupby(["period", "version"])
         .agg(rec=("n_records", "sum"), bad=("n_bad", "sum"), ok=("n_ok", "sum"),
              off=("n_official", "sum"), cap=("symbol", "nunique"))
         .reset_index())
    g = g.assign(o=g["version"].map({"naive": 0, "drift_aware": 1})).sort_values(["period", "o"])
    print("Bảng 1 · E1 gộp theo kỳ × phiên bản\n")
    print("| Kỳ | Đơn vị | Phiên bản | Số cặp | Bản ghi | % mốc vô lý | % nến đúng |")
    print("|---|---|---|---|---|---|---|")
    for r in g.itertuples():
        unit = "ms" if r.period < "2025" else "µs"
        name = "naive" if r.version == "naive" else "drift-aware"
        print(f"| {r.period} | {unit} | {name} | {r.cap} | {n(r.rec)} | "
              f"{100 * r.bad / r.rec:.0f}% | {100 * r.ok / r.off:.0f}% |")

    zips = {(e["symbol"], e["period"]): Path(e["zip"]) for e in load_manifest().values() if e["kind"] == "aggTrades"}
    p = d[d["version"] == "drift_aware"].sort_values(["period", "symbol"])
    print("\nBảng 2 · Hồ sơ nguồn (aggTrades)\n")
    print("| Cặp | Kỳ | Bản ghi | Zip (MB) | Chữ số timestamp | Đơn vị suy ra | Mốc đầu | Mốc cuối |")
    print("|---|---|---|---|---|---|---|---|")
    for r in p.itertuples():
        z = zips.get((r.symbol, r.period))
        mb = f"{z.stat().st_size / 1e6:,.0f}".replace(",", " ") if z and z.exists() else "?"
        unit = {13: "ms", 16: "µs"}.get(int(r.raw_digits), "?") if pd.notna(r.raw_digits) else "?"
        print(f"| {r.symbol} | {r.period} | {n(r.n_records)} | {mb} | {r.raw_digits:.0f} | {unit} | "
              f"{str(r.first_ts)[:19]} | {str(r.last_ts)[:19]} |")
    print(f"\nTổng bản ghi (drift-aware): {n(p['n_records'].sum())}")


if __name__ == "__main__":
    main()
