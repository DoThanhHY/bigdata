"""Bộ nhận WebSocket Binance -> Kafka, có tự nối lại + backfill REST.

Xử lý các bẫy trong plan:
  D5: lưu aggId cuối cùng đã nhận (bảng ingest_state) => backfill được sau khi mất kết nối.
  D6: ping/pong (thư viện websockets tự trả pong cho ping 20s của Binance) +
      chủ động nối lại trước mốc 24h Binance tự ngắt.

Ngữ nghĩa giao nhận: producer là at-least-once (sau crash có thể gửi lại một đoạn
ngắn đã gửi); downstream loại trùng theo (symbol, agg_id). Producer Kafka bật
idempotence để không nhân bản do retry mạng.

Chạy:
    python -m bdi.ingest.ws_producer                 # endpoint chính
    python -m bdi.ingest.ws_producer --fallback      # wss://data-stream.binance.vision
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import time
from datetime import datetime, timezone

import psycopg
from confluent_kafka import Producer
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from bdi.config import pg_dsn, settings
from bdi.ingest.backfill import fetch_range
from bdi.timeutil import to_micros

log = logging.getLogger("ws_producer")

STATE_FLUSH_SECONDS = 2.0


def now_us() -> int:
    return time.time_ns() // 1_000


class StateStore:
    """aggId cuối cùng đã giao thành công cho Kafka, theo symbol."""

    def __init__(self, dsn: str):
        self.dsn = dsn
        self.conn = psycopg.connect(dsn, autocommit=True)

    def _retry(self, fn):
        """Postgres rớt kết nối (restart, quá tải) không được làm chết producer: nối lại rồi thử lại."""
        for attempt in range(5):
            try:
                return fn()
            except psycopg.OperationalError as e:
                log.warning("Postgres lỗi (%s), nối lại lần %d", e, attempt + 1)
                time.sleep(min(2 ** attempt, 15))
                try:
                    self.conn = psycopg.connect(self.dsn, autocommit=True)
                except psycopg.OperationalError:
                    pass
        return fn()

    def load(self) -> dict[str, tuple[int, datetime]]:
        rows = self._retry(lambda: self.conn.execute(
            "SELECT symbol, last_agg_id, updated_at FROM ingest_state").fetchall())
        return {s: (a, u) for s, a, u in rows}

    def save(self, last: dict[str, int], last_ts: dict[str, int]) -> None:
        self._retry(lambda: self._save(last, last_ts))

    def _save(self, last: dict[str, int], last_ts: dict[str, int]) -> None:
        with self.conn.cursor() as cur:
            cur.executemany(
                """INSERT INTO ingest_state (symbol, last_agg_id, last_ts_us, updated_at)
                   VALUES (%s, %s, %s, now())
                   ON CONFLICT (symbol) DO UPDATE SET last_agg_id = EXCLUDED.last_agg_id,
                     last_ts_us = EXCLUDED.last_ts_us, updated_at = now()""",
                [(s, a, last_ts.get(s)) for s, a in last.items()],
            )

    def log_gap(self, symbol: str, disconnected_at: datetime, last_agg_id: int, resumed_agg_id: int,
                backfilled: int, reason: str) -> None:
        self._retry(lambda: self.conn.execute(
            """INSERT INTO ws_gaps (symbol, disconnected_at, reconnected_at, last_agg_id, resumed_agg_id,
                                    backfilled, reason)
               VALUES (%s, %s, now(), %s, %s, %s, %s)""",
            (symbol, disconnected_at, last_agg_id, resumed_agg_id, backfilled, reason),
        ))


class Ingestor:
    def __init__(self, symbols: list[str], fallback: bool, topic: str):
        cfg = settings()
        self.symbols = [s.upper() for s in symbols]
        base = cfg["binance"]["ws_fallback_url" if fallback else "ws_url"]
        self.url = f"{base}?streams=" + "/".join(f"{s.lower()}@aggTrade" for s in self.symbols)
        self.max_session = cfg["binance"]["ws_max_session_seconds"]
        self.topic = topic
        self.producer = Producer({
            "bootstrap.servers": cfg["kafka"]["bootstrap"],
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 20,
            "compression.type": "lz4",
        })
        self.state = StateStore(pg_dsn())
        persisted = self.state.load()
        self.last: dict[str, int] = {s: a for s, (a, _) in persisted.items() if s in self.symbols}
        self.last_ts: dict[str, int] = {}
        # Lúc khởi động coi như vừa mất kết nối từ lần cập nhật state cuối (VD: container bị kill)
        self.disconnected_at: dict[str, datetime] = {s: u for s, (_, u) in persisted.items()}
        self.disconnect_reason = "process_restart"
        self._last_flush = 0.0
        self._stop = False

    # ---------- Kafka ----------
    def _send(self, msg: dict, origin: str) -> None:
        sym = msg["s"]
        msg = {**msg, "recv_ts_us": now_us(), "origin": origin}
        value = json.dumps(msg).encode()
        while True:
            try:
                self.producer.produce(self.topic, key=sym.encode(), value=value)
                break
            except BufferError:  # hàng đợi nội bộ đầy (VD backfill lớn): chờ Kafka nhận bớt
                self.producer.poll(0.2)
        self.producer.poll(0)
        self.last[sym] = msg["a"]
        self.last_ts[sym] = to_micros(msg["T"])

    def _maybe_flush_state(self, force: bool = False) -> None:
        if force or time.monotonic() - self._last_flush > STATE_FLUSH_SECONDS:
            self.producer.flush(10)          # chỉ lưu state SAU khi Kafka đã ack
            if self.last:
                self.state.save(self.last, self.last_ts)
            self._last_flush = time.monotonic()

    # ---------- Backfill ----------
    def _backfill_all(self) -> None:
        """Lấp khoảng trống cho MỌI symbol, tới bản ghi mới nhất, TRƯỚC khi đẩy tin live.

        Watermark của Spark là toàn cục (mọi symbol). Nếu đẩy tin live của symbol A
        trước khi backfill xong symbol B, watermark tiến lên và dữ liệu backfill
        của B bị loại vì trễ. Backfill hết trước => dữ liệu cũ đến trước dữ liệu mới.
        """
        for sym in self.symbols:
            last = self.last.get(sym)
            if last is None:
                continue  # lần chạy đầu tiên: chưa có mốc để backfill
            t0 = time.monotonic()
            n = 0
            for row in fetch_range(sym, last + 1):
                self._send(row, "rest_backfill")
                n += 1
            self.producer.flush(30)
            if n:
                log.warning("backfill %s: aggId %d..%d (%d bản ghi) trong %.1fs",
                            sym, last + 1, self.last[sym], n, time.monotonic() - t0)
                self.state.log_gap(sym, self.disconnected_at.get(sym, datetime.now(timezone.utc)),
                                   last, last + 1, n, self.disconnect_reason)
        self._maybe_flush_state(force=True)

    # ---------- WS loop ----------
    def _handle(self, raw: str) -> None:
        data = json.loads(raw).get("data")
        if not data or data.get("e") != "aggTrade":
            return
        sym = data["s"]
        if data["a"] <= self.last.get(sym, -1):
            return  # đã gửi (qua backfill hoặc trước crash)
        self._send(data, "ws")
        self._maybe_flush_state()

    async def run(self) -> None:
        backoff = 1
        while not self._stop:
            started = time.monotonic()
            try:
                log.info("kết nối %s", self.url)
                async with connect(self.url, ping_interval=20, ping_timeout=60, max_size=2**22) as ws:
                    backoff = 1
                    # chạy ở thread riêng để vòng lặp asyncio vẫn trả pong trong lúc backfill;
                    # tin live đến trong lúc này được websockets đệm lại, aggId cũ sẽ bị bỏ qua
                    await asyncio.to_thread(self._backfill_all)
                    while not self._stop:
                        remaining = self.max_session - (time.monotonic() - started)
                        if remaining <= 0:
                            self.disconnect_reason = "proactive_24h"
                            log.info("chủ động nối lại trước mốc 24h")
                            break
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=min(remaining, 30))
                        except asyncio.TimeoutError:
                            if remaining <= 30:
                                continue  # hết phiên: vòng lặp sau sẽ nối lại chủ động
                            # 30s không có tin nào => coi như kết nối chết
                            self.disconnect_reason = "recv_timeout"
                            break
                        self._handle(raw)
            except (ConnectionClosed, OSError) as e:
                self.disconnect_reason = f"{type(e).__name__}"
                log.warning("mất kết nối: %s", e)
            # đánh dấu thời điểm ngắt cho mọi symbol; lần đầu nhận lại sẽ backfill
            self._maybe_flush_state(force=True)
            now = datetime.now(timezone.utc)
            self.disconnected_at = {s: now for s in self.symbols}
            if not self._stop:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
        self._maybe_flush_state(force=True)

    def stop(self, *_):
        self._stop = True


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    cfg = settings()
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", nargs="+", default=cfg["symbols"])
    ap.add_argument("--fallback", action="store_true", help="dùng wss://data-stream.binance.vision")
    ap.add_argument("--topic", default=cfg["kafka"]["topic_live"])
    a = ap.parse_args()

    ing = Ingestor(a.symbols, a.fallback, a.topic)
    loop = asyncio.new_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, ing.stop)
    loop.run_until_complete(ing.run())


if __name__ == "__main__":
    main()
