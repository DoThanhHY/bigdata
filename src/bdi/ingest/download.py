"""Tải aggTrades + klines 1m từ data.binance.vision, kiểm checksum, giải nén.

Ví dụ:
    python -m bdi.ingest.download --kind aggTrades klines            # theo settings.yaml
    python -m bdi.ingest.download --kind aggTrades --symbols BTCUSDT --months 2022-11
    python -m bdi.ingest.download --kind aggTrades klines --days 2026-10-14   # file theo ngày (E5)

Cấu trúc đầu ra (data/ KHÔNG được commit — bẫy D7):
    data/raw/<kind>/<SYMBOL>/<SYMBOL>-aggTrades-2022-11.zip
    data/raw/<kind>/<SYMBOL>/<SYMBOL>-aggTrades-2022-11.csv
    data/raw/manifest.jsonl     (1 dòng / file: checksum, đường dẫn, số dòng)
"""

from __future__ import annotations

import argparse
import json
import sys
import zipfile
from pathlib import Path

import requests

from bdi.config import path, settings
from bdi.lineage import sha256_file


def file_url(kind: str, symbol: str, period: str) -> str:
    base = settings()["binance"]["data_base_url"]
    freq = "daily" if len(period) == 10 else "monthly"
    if kind == "aggTrades":
        return f"{base}/{freq}/aggTrades/{symbol}/{symbol}-aggTrades-{period}.zip"
    if kind == "klines":
        return f"{base}/{freq}/klines/{symbol}/1m/{symbol}-1m-{period}.zip"
    raise ValueError(kind)


def _download(url: str, dest: Path) -> None:
    tmp = dest.with_suffix(dest.suffix + ".part")
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(1 << 20):
                f.write(chunk)
    tmp.rename(dest)


def fetch(kind: str, symbol: str, period: str, force: bool = False) -> dict:
    url = file_url(kind, symbol, period)
    out_dir = path("raw") / kind / symbol
    out_dir.mkdir(parents=True, exist_ok=True)
    zpath = out_dir / url.rsplit("/", 1)[1]

    if force or not zpath.exists():
        print(f"[download] {url}", file=sys.stderr)
        _download(url, zpath)

    expected = requests.get(url + ".CHECKSUM", timeout=30).text.split()[0]
    actual = sha256_file(zpath)
    if actual != expected:
        zpath.unlink()
        raise RuntimeError(f"Sai checksum {zpath.name}: {actual} != {expected}")

    with zipfile.ZipFile(zpath) as z:
        name = z.namelist()[0]
        csv_path = out_dir / name
        if force or not csv_path.exists():
            z.extract(name, out_dir)

    return {"kind": kind, "symbol": symbol, "period": period, "url": url,
            "zip": str(zpath), "csv": str(csv_path), "sha256": actual}


def append_manifest(entry: dict) -> None:
    mpath = path("raw") / "manifest.jsonl"
    existing = load_manifest()
    existing[entry["csv"]] = entry
    with open(mpath, "w") as f:
        for e in existing.values():
            f.write(json.dumps(e) + "\n")


def load_manifest() -> dict[str, dict]:
    mpath = path("raw") / "manifest.jsonl"
    if not mpath.exists():
        return {}
    with open(mpath) as f:
        return {e["csv"]: e for e in map(json.loads, f) if e}


def main() -> None:
    cfg = settings()
    ap = argparse.ArgumentParser()
    ap.add_argument("--kind", nargs="+", default=["aggTrades", "klines"], choices=["aggTrades", "klines"])
    ap.add_argument("--symbols", nargs="+", default=cfg["symbols"])
    ap.add_argument("--months", nargs="+", default=cfg["months"])
    ap.add_argument("--days", nargs="*", default=[], help="YYYY-MM-DD; nếu có thì bỏ qua --months")
    ap.add_argument("--force", action="store_true")
    a = ap.parse_args()

    periods = a.days or a.months
    for kind in a.kind:
        for sym in a.symbols:
            for p in periods:
                try:
                    entry = fetch(kind, sym, p, a.force)
                    append_manifest(entry)
                    print(f"[ok] {kind} {sym} {p} sha256={entry['sha256'][:12]}…")
                except requests.HTTPError as e:
                    print(f"[skip] {kind} {sym} {p}: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
