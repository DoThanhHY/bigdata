"""Sơ đồ kiến trúc cho slide 4 (không cần Spark/DB).

    python experiments/arch_diagram.py   ->  results/arch/architecture.png

Khối build_candles() tô cam: nơi hai tầng batch và streaming dùng chung một hàm (C2).
"""

from __future__ import annotations

import _common
import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

BLUE, ORANGE, GREEN = _common.SERIES[0], _common.SERIES[1], _common.SERIES[2]
GREY = _common.TEXT_2


def box(ax, x, y, w, h, text, color=GREY, fill=None, bold=False, fs=9):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.02,rounding_size=0.12",
                                linewidth=1.6, edgecolor=color, facecolor=fill or _common.SURFACE))
    ax.text(x + w / 2, y + h / 2, text, ha="center", va="center", fontsize=fs,
            color="white" if fill and fill != _common.SURFACE else _common.TEXT,
            fontweight="bold" if bold else "normal")
    return (x, y, w, h)


def arrow(ax, a, b, color=GREY, label=None, start="r", end="l", rad=0.0):
    def pt(bx, side):
        x, y, w, h = bx
        return {"r": (x + w, y + h / 2), "l": (x, y + h / 2), "t": (x + w / 2, y + h),
                "b": (x + w / 2, y)}[side]
    p, q = pt(a, start), pt(b, end)
    ax.add_patch(FancyArrowPatch(p, q, arrowstyle="-|>", mutation_scale=12, linewidth=1.4,
                                 color=color, connectionstyle=f"arc3,rad={rad}"))
    if label:
        ax.text((p[0] + q[0]) / 2, (p[1] + q[1]) / 2 + 0.12, label, ha="center", fontsize=7.5,
                color=GREY)


def main() -> None:
    fig, ax = plt.subplots(figsize=(14, 6.2))
    ax.set_xlim(0, 14)
    ax.set_ylim(-0.45, 6.2)
    ax.axis("off")
    ax.grid(False)

    # nhãn tầng
    ax.text(0.1, 5.85, "TẦNG BATCH (T+1)", fontsize=10, color=GREY, fontweight="bold")
    ax.text(0.1, 1.95, "TẦNG STREAMING (real-time)", fontsize=10, color=BLUE, fontweight="bold")

    # batch
    b_src = box(ax, 0.1, 4.6, 2.0, 0.9, "data.binance.vision\naggTrades + klines CSV")
    b_dl = box(ax, 2.6, 4.6, 1.8, 0.9, "download\nkiểm sha256")
    b_slv = box(ax, 4.9, 4.6, 1.9, 0.9, "canonical C1\nsilver parquet")
    b_off = box(ax, 4.9, 3.4, 1.9, 0.8, "klines → official")

    # stream
    s_src = box(ax, 0.1, 0.6, 2.0, 0.9, "Binance WebSocket\n<sym>@aggTrade", color=BLUE)
    s_prod = box(ax, 2.6, 0.6, 1.8, 0.9, "ws_producer\n+ REST backfill", color=BLUE)
    s_k = box(ax, 4.9, 0.6, 1.9, 0.9, "Kafka\n(retention ∞)", color=BLUE)
    s_sp = box(ax, 7.3, 0.6, 2.0, 0.9, "Spark Streaming\nwatermark + dedup", color=BLUE)
    s_arc = box(ax, 4.9, 0.0, 1.9, 0.45, "bronze archive → replay", color=BLUE, fs=7.5)

    # hàm chung
    core = box(ax, 7.3, 2.65, 2.0, 1.0, "build_candles()\nmột hàm · hai tầng (C2)",
               color=ORANGE, fill=ORANGE, bold=True)

    # sink + downstream
    pg = box(ax, 10.0, 2.55, 1.9, 1.2, "Postgres candles\n(symbol, minute, source)\nupsert + lineage (C6)",
             fs=8)
    rec = box(ax, 12.2, 3.8, 1.7, 0.9, "Đối soát 3 nguồn\n+ nhãn lỗi (C3)", color=GREEN, fs=8)
    c5 = box(ax, 12.2, 2.65, 1.7, 0.9, "Chọn nguồn\nTTC (C5)", color=GREEN, fs=8)
    gf = box(ax, 12.2, 1.5, 1.7, 0.9, "Grafana\nlive + kết quả", color=GREEN, fs=8)

    arrow(ax, b_src, b_dl)
    arrow(ax, b_dl, b_slv)
    arrow(ax, b_slv, core, start="r", end="t", rad=-0.25)
    arrow(ax, b_dl, b_off, start="b", end="l", rad=0.25)
    arrow(ax, b_off, pg, start="r", end="t", rad=-0.35)
    ax.text(9.65, 4.55, "source=official", ha="center", fontsize=7.5, color=GREY)
    arrow(ax, s_src, s_prod, color=BLUE)
    arrow(ax, s_prod, s_k, color=BLUE)
    ax.text(4.65, 1.6, "key=symbol", ha="center", fontsize=7.5, color=GREY)
    arrow(ax, s_k, s_sp, color=BLUE)
    arrow(ax, s_k, s_arc, color=BLUE, start="b", end="t")
    arrow(ax, s_sp, core, color=BLUE, start="t", end="b")
    arrow(ax, core, pg, color=ORANGE)
    ax.text(9.65, 3.25, "batch | stream", ha="center", fontsize=7.5, color=GREY)
    for t in (rec, c5, gf):
        arrow(ax, pg, t, color=GREEN)

    ax.text(7, -0.2, "PySpark 4.0 · Kafka 3.9 KRaft · Postgres 16 · Grafana 11 · Docker Compose",
            ha="center", va="top", fontsize=8.5, color=GREY)
    fig.tight_layout()
    out = _common.out_dir("arch") / "architecture.png"
    fig.savefig(out, dpi=160, bbox_inches="tight")
    plt.close(fig)
    print(f"[ok] {out}")


if __name__ == "__main__":
    main()
