"""Chỉ số dùng chung cho E3 (độ trễ) và C5 (time-to-convergence, chất lượng nguồn)."""

from __future__ import annotations

import json

import numpy as np
import pandas as pd
import psycopg

from bdi.config import path, pg_dsn

MINUTE = pd.Timedelta(minutes=1)
EPOCH = pd.Timestamp(0, tz="UTC")


def to_us(s: pd.Series) -> pd.Series:
    """datetime (UTC) -> micro-giây, KHÔNG phụ thuộc độ phân giải (pandas 3 mặc định 'us', pandas 2 'ns')."""
    return (pd.to_datetime(s, utc=True) - EPOCH) // pd.Timedelta(microseconds=1)


def emission_latency(candles: pd.DataFrame, replay_run_id: str | None = None) -> pd.Series:
    """Độ trễ (giây) từ lúc phút kết thúc đến lúc nến được ghi vào DB.

    Với dữ liệu phát lại (replay), mốc "phút kết thúc" được quy đổi sang đồng hồ
    thật theo results/replay/<run_id>.json:
        wall(x) = replay_start + (x − origin) / speed
    """
    end_us = to_us(candles["minute"] + MINUTE)
    emitted_us = to_us(candles["emitted_at"])
    if replay_run_id:
        meta = json.loads((path("results") / "replay" / f"{replay_run_id}.json").read_text())
        end_us = meta["replay_start_us"] + (end_us - meta["origin_us"]) / meta["speed"]
    return (emitted_us - end_us) / 1e6


def pct(s: pd.Series, q: float) -> float:
    s = s.dropna()
    return float(np.percentile(s, q)) if len(s) else float("nan")


def time_to_convergence(source: str, official: str = "official", dsn: str | None = None) -> pd.DataFrame:
    """C5: với mỗi nến, thời điểm phiên bản streaming trùng hẳn với official.

    Nến "hội tụ" tại phiên bản đầu tiên khớp official mà mọi phiên bản sau cũng khớp.
    ttc_s = emitted_at(phiên bản đó) − (minute + 1 phút). NaN = không bao giờ hội tụ.
    """
    sql = """
    WITH v AS (
      SELECT v.symbol, v.minute, v.batch_id, v.emitted_at,
             (v.open = o.open AND v.high = o.high AND v.low = o.low AND v.close = o.close
              AND v.volume = o.volume AND v.trades = o.trades) AS ok
      FROM candle_versions v
      JOIN candles o ON o.symbol = v.symbol AND o.minute = v.minute AND o.source = %s
      WHERE v.source = %s
    ), tail AS (
      SELECT *, bool_and(ok) OVER (PARTITION BY symbol, minute ORDER BY batch_id DESC
                                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ok_after
      FROM v
    )
    SELECT symbol, minute,
           min(emitted_at) FILTER (WHERE ok_after) AS converged_at,
           count(*) AS n_versions
    FROM tail GROUP BY symbol, minute
    """
    with psycopg.connect(dsn or pg_dsn()) as conn:
        cur = conn.execute(sql, (official, source))
        df = pd.DataFrame(cur.fetchall(), columns=["symbol", "minute", "converged_at", "n_versions"])
    df["minute"] = pd.to_datetime(df["minute"], utc=True)
    df["converged_at"] = pd.to_datetime(df["converged_at"], utc=True)
    df["ttc_s"] = (df["converged_at"] - (df["minute"] + MINUTE)).dt.total_seconds()
    return df


def source_quality(cmp: pd.DataFrame, latency_s: pd.Series | None = None) -> dict:
    """C5: completeness / timeliness / consistency của một nguồn so với official.

    cmp: output của reconcile.compare.compare(left=nguồn, right=official, how='outer').
    """
    has_r = cmp["trades_r"].notna()
    return {
        "completeness_pct": 100 * float((cmp["trades_l"].notna() & has_r).sum() / max(has_r.sum(), 1)),
        "consistency_pct": 100 * float(cmp.loc[has_r, "all_match"].mean()),
        "timeliness_p50_s": pct(latency_s, 50) if latency_s is not None else None,
        "timeliness_p95_s": pct(latency_s, 95) if latency_s is not None else None,
    }
