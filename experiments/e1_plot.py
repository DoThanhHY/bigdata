"""E1 — Vẽ hình ablation từ results/e1/ablation.csv (không cần Spark).

    python experiments/e1_plot.py

Đầu ra: results/e1/ablation.png
  Hai panel chung trục x (kỳ dữ liệu): % nến khớp official và % mốc thời gian vô lý,
  mỗi panel so naive vs drift-aware, gộp 5 cặp (trung bình có trọng số theo số nến/bản ghi).
"""

from __future__ import annotations

import _common
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

VERSIONS = [("naive", "Naive (coi mọi giá trị là ms)", _common.SERIES[1]),
            ("drift_aware", "Drift-aware (suy đơn vị theo độ lớn)", _common.SERIES[0])]


def main() -> None:
    d = _common.out_dir("e1")
    df = pd.read_csv(d / "ablation.csv")
    df["n_ok"] = df["pct_candle_ok"] * df["n_official"] / 100
    df["n_bad"] = df["pct_bad_ts"] * df["n_records"] / 100
    g = df.groupby(["period", "version"]).sum(numeric_only=True)
    g["pct_candle_ok"] = 100 * g["n_ok"] / g["n_official"]
    g["pct_bad_ts"] = 100 * g["n_bad"] / g["n_records"]
    periods = sorted(df["period"].unique())
    n_sym = df["symbol"].nunique()

    fig, axes = plt.subplots(1, 2, figsize=(10, 3.8), sharex=True)
    x = np.arange(len(periods))
    w = 0.38
    for ax, col, title in ((axes[0], "pct_candle_ok", "% nến khớp tuyệt đối official"),
                           (axes[1], "pct_bad_ts", "% bản ghi có mốc thời gian vô lý")):
        for i, (v, label, color) in enumerate(VERSIONS):
            ys = [g.loc[(p, v), col] for p in periods]
            bars = ax.bar(x + (i - 0.5) * w, ys, w, label=label, color=color)
            ax.bar_label(bars, labels=[f"{y:.0f}%" for y in ys], fontsize=8, padding=2,
                         color=_common.TEXT_2)
        ax.set_title(title)
        ax.set_ylim(0, 112)
        ax.set_xticks(x, periods)
        ax.grid(axis="x", visible=False)
        ax.set_axisbelow(True)
    h, l = axes[0].get_legend_handles_labels()
    fig.legend(h, l, loc="lower center", ncol=2, frameon=False, fontsize=9)
    fig.suptitle(f"E1 · Ablation schema drift · {n_sym} cặp · CSV 2022 dùng ms, CSV 2026 dùng µs",
                 fontsize=11)
    fig.tight_layout(rect=(0, 0.07, 1, 1))
    out = d / "ablation.png"
    fig.savefig(out, dpi=160)
    plt.close(fig)
    print(f"[ok] {out}")


if __name__ == "__main__":
    main()
