"""C3 — Phân loại nguyên nhân sai lệch của một cây nến so với nến chính thức.

Thứ tự ưu tiên luật (luật đầu tiên khớp thắng):
  1. EMPTY_MINUTE           một phía thiếu nến, phía kia 0 giao dịch (hoặc cũng thiếu)
  1b. CAPTURE_START         phút đầu tiên của luồng thu thập (bắt đầu giữa phút) — thiếu trade
  2. DISCONNECT_GAP         phút rơi vào khoảng mất kết nối WS (ws_gaps) và thiếu trade
  3. DUPLICATE              thừa trade (trades_left > trades_right) / có agg_id trùng
  4. LATE_BEYOND_WATERMARK  thiếu trade và có bản tin của phút đó bị coi là trễ
  5. NUMERIC_PRECISION      trades + O/H/L/C khớp, chỉ volume lệch
  6. UNKNOWN
"""

from __future__ import annotations

import pandas as pd

CAUSES = [
    "CAPTURE_START", "LATE_BEYOND_WATERMARK", "DISCONNECT_GAP", "DUPLICATE",
    "EMPTY_MINUTE", "NUMERIC_PRECISION", "UNKNOWN",
]


def _in_gaps(m: pd.DataFrame, gaps: pd.DataFrame | None) -> pd.Series:
    """Phút [minute, minute+1m) có giao với khoảng [disconnected_at, reconnected_at] không."""
    out = pd.Series(False, index=m.index)
    if gaps is None or gaps.empty:
        return out
    end = m["minute"] + pd.Timedelta(minutes=1)
    for g in gaps.itertuples(index=False):
        hit = (m["symbol"] == g.symbol) & (m["minute"] <= g.reconnected_at) & (end > g.disconnected_at)
        out |= hit
    return out


def _first_minute(m: pd.DataFrame) -> pd.Series:
    """Phút sớm nhất mà nguồn kiểm tra có nến, theo từng symbol."""
    first = m[m["trades_l"].notna()].groupby("symbol")["minute"].transform("min")
    return (m["minute"] == first.reindex(m.index)).fillna(False)


def _lookup(m: pd.DataFrame, counts: pd.DataFrame | None, col: str) -> pd.Series:
    if counts is None or counts.empty:
        return pd.Series(0, index=m.index)
    j = m[["symbol", "minute"]].merge(counts, on=["symbol", "minute"], how="left")
    return j[col].fillna(0).set_axis(m.index)


def label(m: pd.DataFrame, gaps: pd.DataFrame | None = None, late: pd.DataFrame | None = None,
          dups: pd.DataFrame | None = None) -> pd.Series:
    """m: kết quả compare.compare(); cột *_l (nguồn kiểm tra), *_r (official).

    late: DataFrame(symbol, minute, late_count)   — từ compare.simulate_late()
    dups: DataFrame(symbol, minute, dup_count)    — từ compare.count_duplicates()
    """
    cause = pd.Series(pd.NA, index=m.index, dtype="object")
    miss_l, miss_r = m["trades_l"].isna(), m["trades_r"].isna()
    tl, tr = m["trades_l"].fillna(0), m["trades_r"].fillna(0)
    ohlc_ok = m[["match_open", "match_high", "match_low", "match_close"]].all(axis=1)

    rules = [
        ("EMPTY_MINUTE", (miss_l & (tr == 0)) | (miss_r & (tl == 0))),
        ("CAPTURE_START", _first_minute(m) & (tl < tr)),
        ("DISCONNECT_GAP", _in_gaps(m, gaps) & (tl < tr)),
        ("DUPLICATE", (tl > tr) | ((_lookup(m, dups, "dup_count") > 0) & ~m["match_trades"])),
        ("LATE_BEYOND_WATERMARK", (tl < tr) & (_lookup(m, late, "late_count") > 0)),
        ("NUMERIC_PRECISION", ohlc_ok & m["match_trades"] & ~m["match_volume"]),
    ]
    bad = ~m["all_match"]
    for name, cond in rules:
        cause = cause.mask(bad & cause.isna() & cond.fillna(False), name)
    return cause.mask(bad & cause.isna(), "UNKNOWN")
