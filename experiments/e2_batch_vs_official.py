"""E2 — Đối soát batch vs official: heatmap % khớp theo cặp × tháng + phân bố nguyên nhân.

Tiền điều kiện:
    python -m bdi.batch.load_klines
    python -m bdi.batch.build_batch
    python -m bdi.reconcile.compare --left batch --right official --run-id e2

    python experiments/e2_batch_vs_official.py --run-id e2
"""

from __future__ import annotations

import argparse

import _common
import matplotlib.pyplot as plt
import pandas as pd

from bdi.config import path

FIELDS = ["match_open", "match_high", "match_low", "match_close", "match_volume", "match_trades", "all_match"]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", default="e2")
    a = ap.parse_args()

    rdir = path("results") / "reconcile"
    s = pd.read_csv(rdir / f"{a.run_id}_summary.csv")
    causes = pd.read_csv(rdir / f"{a.run_id}_causes.csv")

    # Bảng chi tiết từng trường (đưa thẳng vào báo cáo)
    s.to_csv(_common.out_dir("e2") / "match_by_field.csv", index=False)

    # Heatmap % nến khớp tuyệt đối mọi trường
    pv = s.pivot(index="symbol", columns="month", values="all_match")
    fig, ax = plt.subplots(figsize=(1.6 + 1.3 * pv.shape[1], 0.8 + 0.55 * pv.shape[0]))
    im = ax.imshow(pv.values, cmap=_common.SEQ_CMAP, vmin=max(0, pv.values.min() - 1), vmax=100, aspect="auto")
    ax.set_xticks(range(pv.shape[1]), pv.columns)
    ax.set_yticks(range(pv.shape[0]), pv.index)
    ax.grid(False)
    for i in range(pv.shape[0]):
        for j in range(pv.shape[1]):
            v = pv.values[i, j]
            ax.text(j, i, f"{v:.2f}%", ha="center", va="center",
                    color="white" if im.norm(v) > 0.5 else _common.TEXT, fontsize=9)
    ax.set_title("E2 · % nến batch khớp tuyệt đối official (OHLCV + trades)")
    fig.colorbar(im, ax=ax, label="% khớp")
    _common.save(fig, "e2", "heatmap_all_match")

    if not causes.empty:
        fig, ax = plt.subplots(figsize=(6, 0.6 + 0.4 * len(causes)))
        c = causes.sort_values("n")
        ax.barh(c["cause"], c["n"], color=_common.SERIES[0], height=0.6)
        for y, n in enumerate(c["n"]):
            ax.text(n, y, f" {n:,}", va="center", color=_common.TEXT_2)
        ax.set_xlabel("số nến lệch")
        ax.set_title("E2 · Nguyên nhân lệch (taxonomy C3)")
        _common.save(fig, "e2", "causes")
    print(s.to_string(index=False))


if __name__ == "__main__":
    main()
