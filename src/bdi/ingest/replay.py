"""Phát lại dữ liệu vào Kafka với tốc độ k× (E3, E7, stress test).

Hai nguồn:
  --from-archive  : parquet raw đã lưu từ luồng live (bdi.stream.raw_archive).
                    Giữ NGUYÊN thứ tự đến và khoảng cách thời gian đến (recv_ts_us)
                    => tái hiện đúng độ trễ/đảo thứ tự thật -> dùng cho E3.
  --from-csv      : CSV aggTrades (VD 11/2022 cho FTX) — tạo nhịp theo event time.

Vì sao không "đặt offset về đầu rồi chạy hết tốc lực"? Khi đọc lại cả topic với tốc
độ tối đa, mọi bản tin rơi vào vài micro-batch rất lớn; watermark chỉ tiến giữa các
micro-batch nên GẦN NHƯ KHÔNG bản tin nào bị loại là trễ => đường cong E3 sai.
Phải phát lại có nhịp (speed=1) để watermark hành xử như thật.

Mỗi lần chạy ghi results/replay/<run_id>.json gồm mốc thời gian để quy đổi độ trễ:
    wall_us(x) = replay_start_us + (x - origin_us) / speed

Ví dụ:
    python -m bdi.ingest.replay --from-archive --date 2026-10-14 --speed 1 --run-id e3_wm5
    python -m bdi.ingest.replay --from-csv data/raw/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2022-11.csv \
        --symbol BTCUSDT --speed 100 --start 2022-11-08 --end 2022-11-10 --run-id ftx_100x
"""

from __future__ import annotations

import argparse
import heapq
import json
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import numpy as np
import pandas as pd
import pyarrow.dataset as ds
from confluent_kafka import Producer

from bdi.config import path, settings
from bdi.schema import AGGTRADES_CSV_COLS

TICK_US = 10_000  # gom bản tin trong 10ms rồi mới sleep một lần


def _iso_to_us(s: str) -> int:
    return int(datetime.fromisoformat(s).replace(tzinfo=timezone.utc).timestamp() * 1_000_000)


def iter_archive(date: str | None, start: str | None, end: str | None) -> Iterator[tuple[int, dict]]:
    """(mốc_nhịp_us, payload) theo thứ tự đến thực tế."""
    root = path("lake") / "bronze" / "raw_aggtrades"
    flt = None
    if date:
        flt = ds.field("date") == date
    table = ds.dataset(root, format="parquet", partitioning="hive").to_table(
        filter=flt, columns=["value", "recv_ts_us", "kafka_partition", "kafka_offset"])
    df = table.to_pandas().sort_values(["recv_ts_us", "kafka_partition", "kafka_offset"])
    if start:
        df = df[df.recv_ts_us >= _iso_to_us(start)]
    if end:
        df = df[df.recv_ts_us < _iso_to_us(end)]
    for recv, value in zip(df.recv_ts_us.to_numpy(), df.value.to_numpy()):
        yield int(recv), json.loads(value)


def iter_csv(csv: Path, symbol: str, start: str | None, end: str | None) -> Iterator[tuple[int, dict]]:
    """Đọc CSV theo khối; lọc khoảng thời gian bằng numpy (bỏ qua hàng chục triệu dòng trong vài giây)."""
    lo = _iso_to_us(start) if start else None
    hi = _iso_to_us(end) if end else None
    for chunk in pd.read_csv(csv, header=None, names=AGGTRADES_CSV_COLS, dtype=str, chunksize=1_000_000):
        raw_t = chunk["transact_time"].astype("int64").to_numpy()
        ts = np.where(raw_t >= 10**14, raw_t, raw_t * 1000)  # cùng quy tắc với timeutil.to_micros (ms/us)
        keep = np.ones(len(ts), dtype=bool)
        if lo is not None:
            keep &= ts >= lo
        if hi is not None:
            keep &= ts < hi
        if hi is not None and ts[0] >= hi:
            return
        if not keep.any():
            continue
        c = chunk[keep]
        for t, rt, a, p, q, f, l, m in zip(ts[keep], raw_t[keep], c["agg_id"], c["price"], c["qty"],
                                           c["first_trade_id"], c["last_trade_id"], c["is_buyer_maker"]):
            yield int(t), {"e": "aggTrade", "s": symbol, "a": int(a), "p": p, "q": q, "f": int(f), "l": int(l),
                           "T": int(rt), "E": int(rt), "m": m.lower() == "true"}


def jitter(events: Iterator[tuple[int, dict]], pct: float, max_s: float, seed: int = 42
           ) -> Iterator[tuple[int, dict]]:
    """Tiêm độ trễ có kiểm soát (E3): pct% bản tin bị đẩy lùi thời điểm phát U(0, max_s) giây.

    Mô phỏng mạng xấu: thứ tự event time giữ nguyên trong payload, chỉ thời điểm ĐẾN
    bị lùi lại => các bản tin này đến sau bản tin mới hơn (out-of-order thật).
    Trường "jitter_us" đánh dấu bản tin bị làm trễ để phân tích sau.
    """
    rng = random.Random(seed)
    heap: list[tuple[int, int, dict]] = []
    max_us = int(max_s * 1_000_000)
    for seq, (t, msg) in enumerate(events):
        d = int(rng.random() * max_us) if rng.random() < pct / 100 else 0
        heapq.heappush(heap, (t + d, seq, {**msg, "jitter_us": d} if d else msg))
        while heap and heap[0][0] <= t:
            t_out, _, m = heapq.heappop(heap)
            yield t_out, m
    while heap:
        t_out, _, m = heapq.heappop(heap)
        yield t_out, m


def _produce(p: Producer, topic: str, msg: dict) -> None:
    """Gửi có backpressure: hàng đợi nội bộ đầy (BufferError) thì chờ Kafka nhận bớt rồi gửi lại."""
    value = json.dumps(msg).encode()
    while True:
        try:
            p.produce(topic, key=msg["s"].encode(), value=value)
            p.poll(0)
            return
        except BufferError:
            p.poll(0.2)


def replay(events: Iterator[tuple[int, dict]], topic: str, speed: float, run_id: str,
           extra_meta: dict | None = None) -> dict:
    cfg = settings()
    p = Producer({"bootstrap.servers": cfg["kafka"]["bootstrap"], "linger.ms": 5,
                  "enable.idempotence": True, "acks": "all", "compression.type": "lz4"})
    origin_us = None
    start_wall_us = None
    n = 0
    t_report = time.monotonic()
    for t_us, msg in events:
        if origin_us is None:
            origin_us = t_us
            start_wall_us = time.time_ns() // 1_000
        target = start_wall_us + (t_us - origin_us) / speed
        delay = (target - time.time_ns() // 1_000) / 1e6
        if delay > TICK_US / 1e6:
            time.sleep(delay)
        msg = {**msg, "origin": "replay", "recv_ts_us": time.time_ns() // 1_000}
        _produce(p, topic, msg)
        n += 1
        if time.monotonic() - t_report > 10:
            lag = (time.time_ns() // 1_000 - target) / 1e6
            print(f"[replay] {n:,} bản tin, trễ so với nhịp mục tiêu {lag:.2f}s")
            t_report = time.monotonic()
    p.flush()
    meta = {"run_id": run_id, "topic": topic, "speed": speed, "messages": n,
            "origin_us": origin_us, "replay_start_us": start_wall_us,
            "replay_end_us": time.time_ns() // 1_000, **(extra_meta or {})}
    out = path("results") / "replay" / f"{run_id}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(meta, indent=2))
    print(f"[replay] xong {n:,} bản tin -> {out}")
    return meta


def main() -> None:
    cfg = settings()
    ap = argparse.ArgumentParser()
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--from-archive", action="store_true")
    src.add_argument("--from-csv", type=Path)
    ap.add_argument("--symbol", help="bắt buộc với --from-csv")
    ap.add_argument("--date", help="partition date=YYYY-MM-DD trong archive")
    ap.add_argument("--start")
    ap.add_argument("--end")
    ap.add_argument("--speed", type=float, default=1.0)
    ap.add_argument("--topic", default=cfg["kafka"]["topic_replay"])
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--jitter-pct", type=float, default=0, help="E3: %% bản tin bị làm trễ")
    ap.add_argument("--jitter-max", type=float, default=0, help="E3: độ trễ tối đa (giây)")
    a = ap.parse_args()

    if a.from_archive:
        events = iter_archive(a.date, a.start, a.end)
    else:
        if not a.symbol:
            ap.error("--symbol bắt buộc với --from-csv")
        events = iter_csv(a.from_csv, a.symbol, a.start, a.end)
    if a.jitter_pct > 0:
        events = jitter(events, a.jitter_pct, a.jitter_max)
    replay(events, a.topic, a.speed, a.run_id, {"jitter_pct": a.jitter_pct, "jitter_max_s": a.jitter_max})


if __name__ == "__main__":
    main()
