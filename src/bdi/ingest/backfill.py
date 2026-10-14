"""E4 — Lấp khoảng trống bằng REST: GET /api/v3/aggTrades?fromId=<aggId cuối + 1>.

Trả về bản ghi cùng format với payload WS (thêm "s" và "e") để downstream không
phải phân biệt nguồn — chỉ khác trường origin="rest_backfill".
"""

from __future__ import annotations

import time
from typing import Iterator

import requests

from bdi.config import settings


def fetch_range(symbol: str, from_id: int, to_id: int | None = None, limit: int = 1000,
                session: requests.Session | None = None) -> Iterator[dict]:
    """Sinh aggTrade từ from_id đến to_id (bao gồm). to_id=None => tới mới nhất."""
    s = session or requests.Session()
    base = settings()["binance"]["rest_url"]
    cur = from_id
    while to_id is None or cur <= to_id:
        r = s.get(f"{base}/api/v3/aggTrades",
                  params={"symbol": symbol, "fromId": cur, "limit": limit}, timeout=30)
        if r.status_code in (418, 429):  # bị rate-limit: chờ theo Retry-After
            time.sleep(int(r.headers.get("Retry-After", "5")))
            continue
        r.raise_for_status()
        rows = r.json()
        if not rows:
            return
        for row in rows:
            if to_id is not None and row["a"] > to_id:
                return
            yield {"e": "aggTrade", "s": symbol, **row}
        cur = rows[-1]["a"] + 1
        if len(rows) < limit:
            return
