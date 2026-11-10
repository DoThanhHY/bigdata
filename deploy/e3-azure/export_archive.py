from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pyarrow.dataset as ds
import pyarrow.parquet as pq


def to_us(value: str) -> int:
    return int(datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp() * 1_000_000)


def main() -> None:
    ap = argparse.ArgumentParser(description="Gom mot cua so raw archive thanh mot file Parquet de upload")
    ap.add_argument("--input", type=Path, default=Path("data/lake/bronze/raw_aggtrades"))
    ap.add_argument("--output", type=Path, required=True)
    ap.add_argument("--date", required=True)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument(
        "--max-gap-s",
        type=float,
        default=10.0,
        help="tu choi cua so neu khoang trong dau, giua hoac cuoi lon hon nguong nay",
    )
    a = ap.parse_args()

    start_us = to_us(a.start)
    end_us = to_us(a.end)

    dataset = ds.dataset(a.input, format="parquet", partitioning="hive")
    table = dataset.to_table(
        filter=(ds.field("date") == a.date)
        & (ds.field("recv_ts_us") >= start_us)
        & (ds.field("recv_ts_us") < end_us),
        columns=["value", "recv_ts_us", "kafka_partition", "kafka_offset", "kafka_ts"],
    ).sort_by([("recv_ts_us", "ascending"), ("kafka_partition", "ascending"), ("kafka_offset", "ascending")])
    if table.num_rows == 0:
        raise SystemExit("Khong co du lieu trong cua so da chon")

    recv = table.column("recv_ts_us").to_numpy(zero_copy_only=False)
    first = int(recv[0])
    last = int(recv[-1])
    head_gap_s = (first - start_us) / 1_000_000
    tail_gap_s = (end_us - last) / 1_000_000
    middle_gap_s = float(np.diff(recv).max() / 1_000_000) if len(recv) > 1 else float("inf")
    max_gap_s = max(head_gap_s, middle_gap_s, tail_gap_s)
    if max_gap_s > a.max_gap_s:
        raise SystemExit(
            f"[invalid] archive co gap {max_gap_s:.3f}s > {a.max_gap_s:.3f}s "
            f"(head={head_gap_s:.3f}, middle={middle_gap_s:.3f}, tail={tail_gap_s:.3f}); "
            "hay chon cua so sach khac"
        )

    out = a.output / f"date={a.date}" / "part-00000.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, out, compression="zstd")
    span_s = (last - first) / 1_000_000
    print(f"[ok] {table.num_rows:,} rows, span={span_s:.1f}s, max_gap={max_gap_s:.3f}s -> {out}")


if __name__ == "__main__":
    main()
