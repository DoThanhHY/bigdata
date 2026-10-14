"""Lấy klines 1m chính thức qua REST /api/v3/klines -> Postgres (source='official').

File klines theo ngày trên data.binance.vision chỉ có sau T+1. Module này cho phép
đối soát stream vs official NGAY trong ngày (demo, Grafana, kiểm tra nhanh).
Khi file ngày được công bố, load_klines ghi đè cùng khóa với cùng giá trị.

    python -m bdi.ingest.rest_klines --minutes 120
    python -m bdi.ingest.rest_klines --start 2026-10-14T08:00 --end 2026-10-14T10:00
    python -m bdi.ingest.rest_klines --loop --warmup-days 3   # service: kéo 3 ngày, rồi cập nhật mỗi phút
"""

from __future__ import annotations

import argparse
import logging
import time
from datetime import datetime, timedelta, timezone

import requests

from bdi.config import settings
from bdi.lineage import LINEAGE_COLS
from bdi.sink import CANDLE_DB_COLS, upsert_rows
from bdi.timeutil import to_micros


def fetch(symbol: str, start: datetime, end: datetime) -> list[tuple]:
    base = settings()["binance"]["rest_url"]
    rows, cur = [], int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    now = datetime.now(timezone.utc)
    while cur < end_ms:
        r = requests.get(f"{base}/api/v3/klines", params={
            "symbol": symbol, "interval": "1m", "startTime": cur, "endTime": end_ms - 1, "limit": 1000}, timeout=30)
        r.raise_for_status()
        data = r.json()
        if not data:
            break
        for k in data:
            minute = datetime.fromtimestamp(to_micros(k[0]) / 1e6, timezone.utc)
            if minute + timedelta(minutes=1) > now:
                continue  # nến chưa đóng
            rows.append((symbol, minute, k[1], k[2], k[3], k[4], k[5], k[7], k[8], None, None, None,
                         "official", "rest:/api/v3/klines", now, None, settings()["pipeline_version"]))
        cur = data[-1][0] + 60_000
    return rows


def main() -> None:
    cfg = settings()
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", nargs="+", default=cfg["symbols"])
    ap.add_argument("--minutes", type=int, default=120, help="lấy N phút gần nhất")
    ap.add_argument("--start")
    ap.add_argument("--end")
    ap.add_argument("--loop", action="store_true", help="chạy mãi: mỗi phút lấy N phút gần nhất")
    ap.add_argument("--warmup-days", type=float, default=0, help="với --loop: lần đầu kéo lùi N ngày")
    a = ap.parse_args()
    if a.loop:
        return loop(a.symbols, a.minutes, a.warmup_days)
    end = datetime.fromisoformat(a.end).replace(tzinfo=timezone.utc) if a.end else datetime.now(timezone.utc)
    start = (datetime.fromisoformat(a.start).replace(tzinfo=timezone.utc) if a.start
             else end - timedelta(minutes=a.minutes))
    assert CANDLE_DB_COLS[-len(LINEAGE_COLS):] == LINEAGE_COLS
    for sym in a.symbols:
        rows = fetch(sym, start, end)
        upsert_rows("candles", CANDLE_DB_COLS, ["symbol", "minute", "source"], rows)
        print(f"[ok] {sym}: {len(rows)} nến official (REST)")


def loop(symbols: list[str], minutes: int, warmup_days: float) -> None:
    """Service: giữ nến official luôn mới cho Grafana và cho detector trên luồng live."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s rest_klines: %(message)s")
    log = logging.getLogger("rest_klines")
    back = timedelta(days=warmup_days) if warmup_days else timedelta(minutes=minutes)
    while True:
        end = datetime.now(timezone.utc)
        n = 0
        for sym in symbols:
            try:
                rows = fetch(sym, end - back, end)
                n += upsert_rows("candles", CANDLE_DB_COLS, ["symbol", "minute", "source"], rows)
            except Exception as e:  # noqa: BLE001 — service không được chết vì 1 lỗi mạng/DB
                log.warning("%s: %s", sym, e)
        log.info("ghi %d nến official (%s)", n, back)
        back = timedelta(minutes=minutes)
        time.sleep(60 - datetime.now(timezone.utc).second + 5)  # chạy ngay sau mỗi phút đóng


if __name__ == "__main__":
    main()
