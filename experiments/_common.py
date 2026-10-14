"""Tiện ích chung cho các script thí nghiệm: thư mục kết quả + style hình thống nhất.

Quy ước hình (áp cho cả báo cáo):
  - Bảng màu phân loại cố định theo thứ tự, không xoay vòng (SERIES).
  - KHÔNG dùng biểu đồ 2 trục y: hai đại lượng khác thang đo -> 2 panel chung trục x.
  - Lưới/trục nhạt, nét mảnh 2px, chữ màu trung tính (không tô theo màu series).
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import matplotlib  # noqa: E402

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from bdi.config import path  # noqa: E402

SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
TEXT, TEXT_2, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"
SEQ_CMAP = "Blues"

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "axes.edgecolor": GRID, "axes.labelcolor": TEXT_2, "axes.titlecolor": TEXT,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8,
    "axes.spines.top": False, "axes.spines.right": False,
    "xtick.color": TEXT_2, "ytick.color": TEXT_2, "text.color": TEXT,
    "lines.linewidth": 2, "lines.markersize": 6, "font.size": 10,
    "axes.prop_cycle": matplotlib.cycler(color=SERIES),
})


def out_dir(exp: str) -> Path:
    d = path("results") / exp
    d.mkdir(parents=True, exist_ok=True)
    return d


def save(fig, exp: str, name: str) -> Path:
    p = out_dir(exp) / f"{name}.png"
    fig.tight_layout()
    fig.savefig(p, dpi=160)
    plt.close(fig)
    print(f"[fig] {p}")
    return p
