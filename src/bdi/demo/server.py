"""Demo Console — một trang web cho buổi bảo vệ: live 3 nguồn + nút gây sự cố + tự phục hồi.

    make demo                     # = python -m bdi.demo.server --port 8088
    mở http://localhost:8088

Chạy trên HOST (không trong container) vì cần gọi `docker compose` và `chaos.sh`.
Chỉ dùng stdlib + psycopg (không CDN), để vẫn chạy khi mạng hội trường yếu.

API (JSON):
  GET  /api/status                 container + độ tươi từng nguồn
  GET  /api/candles?symbol=&minutes=   nến stream/official/batch để vẽ
  GET  /api/reconcile?minutes=     stream vs official từng phút, kèm nhãn nguyên nhân sơ bộ
  GET  /api/events                 ws_gaps + chaos_events gần nhất (timeline sự cố)
  GET  /api/chaos                  kịch bản đang chạy + log
  POST /api/chaos/<scenario>       chạy experiments/e4_chaos/chaos.sh <scenario>
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import threading
import time
from datetime import datetime, timezone
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import psycopg

from bdi.config import pg_dsn

ROOT = Path(__file__).resolve().parents[3]
STATIC = Path(__file__).with_name("static")
SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]
SCENARIOS = {"ws_netcut", "ws_kill", "spark_kill", "kafka_restart"}
SERVICES = ["kafka", "postgres", "grafana", "ws-producer", "raw-archive", "stream-candles",
            "official-poller"]
FIELDS = ["open", "high", "low", "close", "volume", "trades"]

_chaos = {"scenario": None, "started": None, "ended": None, "rc": None, "log": []}
_chaos_lock = threading.Lock()


def _json_default(o):
    if isinstance(o, Decimal):
        return float(o)
    if isinstance(o, datetime):
        return o.astimezone(timezone.utc).isoformat()
    raise TypeError(type(o))


def q(sql: str, params: tuple = ()) -> list[dict]:
    with psycopg.connect(pg_dsn(), autocommit=True) as c, c.cursor() as cur:
        cur.execute(sql, params)
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


# ---------------------------------------------------------------- status

def containers() -> list[dict]:
    try:
        out = subprocess.run(["docker", "compose", "ps", "-a", "--format", "json"], cwd=ROOT,
                             capture_output=True, text=True, timeout=10).stdout
    except Exception as e:  # docker chưa bật
        return [{"service": s, "state": "unknown", "status": str(e)[:60]} for s in SERVICES]
    seen = {}
    for line in out.splitlines():
        try:
            d = json.loads(line)
        except json.JSONDecodeError:
            continue
        seen[d["Service"]] = {"service": d["Service"], "state": d["State"], "status": d["Status"]}
    return [seen.get(s, {"service": s, "state": "missing", "status": "chưa tạo"}) for s in SERVICES]


def status() -> dict:
    # latency_s: nến mới nhất được ghi sau bao lâu kể từ lúc hết phút (độ trễ thật của pipeline)
    fresh = q("""
        SELECT DISTINCT ON (source) source, minute AS last_minute, emitted_at AS last_emit,
               extract(epoch FROM emitted_at - (minute + interval '1 minute'))::float AS latency_s
        FROM candles WHERE source IN ('stream', 'official') AND minute > now() - interval '1 day'
        ORDER BY source, minute DESC, emitted_at DESC""")
    return {"now": datetime.now(timezone.utc), "containers": containers(), "freshness": fresh}


# ---------------------------------------------------------------- data

def candles(symbol: str, minutes: int) -> dict:
    rows = q("""
        SELECT source, minute, open, high, low, close, volume, trades
        FROM candles
        WHERE symbol = %s AND source IN ('stream', 'official')
          AND minute > now() - make_interval(mins => %s)
        ORDER BY minute""", (symbol, minutes))
    out: dict[str, list] = {"stream": [], "official": []}
    for r in rows:
        out[r.pop("source")].append(r)
    return out


def _gap_windows(minutes: int) -> list[tuple]:
    rows = q("""
        SELECT symbol, disconnected_at, coalesce(reconnected_at, now()) AS reconnected_at
        FROM ws_gaps WHERE coalesce(reconnected_at, now()) > now() - make_interval(mins => %s)
          AND reason <> 'paused_by_user'""", (minutes + 5,))
    return [(r["symbol"], r["disconnected_at"], r["reconnected_at"]) for r in rows]


def reconcile(minutes: int) -> dict:
    """So khớp tuyệt đối stream vs official trên các phút official đã đóng.

    Nhãn sơ bộ (rút gọn từ bdi.reconcile.taxonomy, không cần Spark):
      MATCH · MISSING_IN_STREAM · DISCONNECT_GAP (lệch/thiếu trong cửa sổ ws_gaps) · MISMATCH
    """
    rows = q("""
        SELECT o.symbol, o.minute,
               s.minute IS NOT NULL AS has_stream,
               o.open = s.open AS open, o.high = s.high AS high, o.low = s.low AS low,
               o.close = s.close AS close, o.volume = s.volume AS volume, o.trades = s.trades AS trades
        FROM candles o
        LEFT JOIN candles s ON s.symbol = o.symbol AND s.minute = o.minute AND s.source = 'stream'
        WHERE o.source = 'official'
          AND o.minute > now() - make_interval(mins => %s)
          AND o.minute < (SELECT coalesce(max(minute), now()) FROM candles WHERE source = 'stream')
          -- bỏ các phút trước lần bật lại sau khi tạm dừng chủ ý (phút đầu tiên chỉ bắt được một phần)
          -- (phút stream đầu tiên = CAPTURE_START, chỉ có một phần dữ liệu)
          AND o.minute > coalesce((SELECT min(c.minute) FROM candles c WHERE c.source = 'stream'
                                   AND c.minute >= (SELECT date_trunc('minute', max(reconnected_at))
                                                    FROM ws_gaps WHERE reason = 'paused_by_user')),
                                  '-infinity')
        ORDER BY o.minute DESC, o.symbol""", (minutes,))
    gaps = _gap_windows(minutes)
    per_sym = {s: {"n": 0, "match": 0} for s in SYMBOLS}
    bad, cells = [], []
    for r in rows:
        sym = r["symbol"]
        if sym not in per_sym:
            continue
        per_sym[sym]["n"] += 1
        ok = bool(r["has_stream"] and all(r[f] for f in FIELDS))
        cells.append([sym, int(r["minute"].timestamp() * 1000), ok])
        if ok:
            per_sym[sym]["match"] += 1
            continue
        in_gap = any(g[0] == sym and g[1] <= r["minute"] <= g[2] for g in gaps)
        label = ("DISCONNECT_GAP" if in_gap else
                 "MISSING_IN_STREAM" if not r["has_stream"] else "MISMATCH")
        bad.append({"symbol": sym, "minute": r["minute"], "label": label,
                    "fields": [f for f in FIELDS if r["has_stream"] and not r[f]]})
    n = sum(v["n"] for v in per_sym.values())
    m = sum(v["match"] for v in per_sym.values())
    return {"total": n, "match": m, "per_symbol": per_sym, "mismatches": bad[:50], "cells": cells}


def _session_start() -> datetime:
    """Mốc bật lại pipeline gần nhất (sau tạm dừng chủ ý); không có thì lấy 24 giờ trước."""
    r = q("SELECT max(reconnected_at) AS t FROM ws_gaps WHERE reason = 'paused_by_user'")
    return r[0]["t"] or q("SELECT now() - interval '1 day' AS t")[0]["t"]


def events() -> list[dict]:
    """Sự cố từ lần bật lại pipeline gần nhất (không lẫn sự cố của các đợt chạy trước)."""
    since = _session_start()
    gaps = q("""
        SELECT 'ws_gap' AS kind, symbol, disconnected_at AS at, reconnected_at AS until,
               backfilled, reason AS note
        FROM ws_gaps WHERE reason <> 'paused_by_user' AND disconnected_at >= %s
        ORDER BY disconnected_at DESC LIMIT 25""", (since,))
    chaos = q("""
        SELECT 'chaos' AS kind, scenario AS symbol, at, NULL::timestamptz AS until,
               NULL::bigint AS backfilled, phase || coalesce(' · ' || nullif(note, ''), '') AS note
        FROM chaos_events WHERE at >= %s ORDER BY at DESC LIMIT 25""", (since,))
    return sorted(gaps + chaos, key=lambda e: e["at"], reverse=True)[:30]


# ---------------------------------------------------------------- chaos

def _run_chaos(scenario: str, down: int) -> None:
    env = {**os.environ, "DOWN": str(down)}
    p = subprocess.Popen(["bash", "experiments/e4_chaos/chaos.sh", scenario], cwd=ROOT, env=env,
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    for line in p.stdout:  # type: ignore[union-attr]
        with _chaos_lock:
            _chaos["log"].append(f"{time.strftime('%H:%M:%S', time.gmtime())}  {line.rstrip()}")
            _chaos["log"] = _chaos["log"][-200:]
    rc = p.wait()
    with _chaos_lock:
        _chaos.update(ended=datetime.now(timezone.utc), rc=rc)
        _chaos["log"].append(f"{time.strftime('%H:%M:%S', time.gmtime())}  [kết thúc] rc={rc}")


def start_chaos(scenario: str, down: int) -> tuple[int, dict]:
    if scenario not in SCENARIOS:
        return 400, {"error": f"kịch bản không hợp lệ: {scenario}"}
    with _chaos_lock:
        if _chaos["scenario"] and _chaos["ended"] is None:
            return 409, {"error": f"đang chạy {_chaos['scenario']}, chờ xong"}
        _chaos.update(scenario=scenario, started=datetime.now(timezone.utc), ended=None, rc=None,
                      log=[f"{time.strftime('%H:%M:%S', time.gmtime())}  chaos.sh {scenario} (DOWN={down}s)"])
    threading.Thread(target=_run_chaos, args=(scenario, down), daemon=True).start()
    return 202, {"started": scenario}


# ---------------------------------------------------------------- http

class Handler(BaseHTTPRequestHandler):
    server_version = "bdi-demo"

    def log_message(self, fmt, *args):  # giữ terminal sạch khi demo
        pass

    def _send(self, code: int, body, ctype: str = "application/json") -> None:
        data = body if isinstance(body, bytes) else json.dumps(body, default=_json_default).encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype + ("; charset=utf-8" if "text" in ctype or "json" in ctype else ""))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):  # noqa: N802
        u = urlparse(self.path)
        p = {k: v[0] for k, v in parse_qs(u.query).items()}
        try:
            if u.path in ("/", "/index.html"):
                return self._send(200, (STATIC / "index.html").read_bytes(), "text/html")
            if u.path == "/api/status":
                return self._send(200, status())
            if u.path == "/api/candles":
                sym = p.get("symbol", "BTCUSDT").upper()
                if sym not in SYMBOLS:
                    return self._send(400, {"error": "symbol"})
                return self._send(200, candles(sym, min(int(p.get("minutes", 30)), 720)))
            if u.path == "/api/reconcile":
                return self._send(200, reconcile(min(int(p.get("minutes", 30)), 720)))
            if u.path == "/api/events":
                return self._send(200, events())
            if u.path == "/api/chaos":
                with _chaos_lock:
                    return self._send(200, dict(_chaos))
            return self._send(404, {"error": "not found"})
        except psycopg.Error as e:
            return self._send(503, {"error": f"Postgres: {str(e)[:200]}"})

    def do_POST(self):  # noqa: N802
        u = urlparse(self.path)
        if u.path.startswith("/api/chaos/"):
            p = {k: v[0] for k, v in parse_qs(u.query).items()}
            down = max(10, min(int(p.get("down", 30)), 300))
            code, body = start_chaos(u.path.rsplit("/", 1)[1], down)
            return self._send(code, body)
        return self._send(404, {"error": "not found"})


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8088)
    a = ap.parse_args()
    srv = ThreadingHTTPServer((a.host, a.port), Handler)
    print(f"[ok] Demo Console: http://localhost:{a.port}  (Ctrl+C để dừng)")
    srv.serve_forever()


if __name__ == "__main__":
    main()
