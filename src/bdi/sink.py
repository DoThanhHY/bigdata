"""C6 — Ghi idempotent vào Postgres: INSERT ... ON CONFLICT DO UPDATE.

Dùng cho cả batch (foreachPartition) lẫn streaming (foreachBatch). Kết hợp
Kafka offset + checkpoint của Spark => hiệu quả tương đương exactly-once:
một micro-batch bị chạy lại sau sự cố chỉ ghi đè đúng các dòng cũ.

KHÔNG dùng mode("append") của JDBC (bẫy D4 trong plan).
"""

from __future__ import annotations

from typing import Iterable

import psycopg
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from bdi.candles import CANDLE_COLS
from bdi.config import pg_dsn
from bdi.lineage import LINEAGE_COLS

CANDLE_DB_COLS = CANDLE_COLS + LINEAGE_COLS


def _upsert_sql(table: str, cols: list[str], key: list[str]) -> str:
    collist = ", ".join(cols)
    placeholders = ", ".join(["%s"] * len(cols))
    updates = [f"{c} = EXCLUDED.{c}" for c in cols if c not in key]
    if table == "candles":
        updates.append("emitted_at = now()")
    return (
        f"INSERT INTO {table} ({collist}) VALUES ({placeholders}) "
        f"ON CONFLICT ({', '.join(key)}) DO UPDATE SET {', '.join(updates)}"
    )


def upsert_rows(table: str, cols: list[str], key: list[str], rows: Iterable[tuple], dsn: str | None = None,
                page: int = 5000) -> int:
    sql = _upsert_sql(table, cols, key)
    n = 0
    buf: list[tuple] = []
    with psycopg.connect(dsn or pg_dsn()) as conn, conn.cursor() as cur:
        for r in rows:
            buf.append(tuple(r))
            if len(buf) >= page:
                cur.executemany(sql, buf)
                n += len(buf)
                buf.clear()
        if buf:
            cur.executemany(sql, buf)
            n += len(buf)
        conn.commit()
    return n


def _utc_strings(df: DataFrame) -> DataFrame:
    """Timestamp -> chuỗi ISO có 'Z'.

    PySpark trả timestamp về Python dưới dạng datetime KHÔNG múi giờ theo giờ máy
    (VD UTC+7), Postgres lại hiểu là UTC => lệch 7 giờ. Định dạng sẵn theo session
    timezone (UTC) để không phụ thuộc máy chạy.
    """
    for name, dtype in df.dtypes:
        if dtype.startswith("timestamp"):
            df = df.withColumn(name, F.date_format(name, "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"))
    return df


def upsert_candles(df: DataFrame, dsn: str | None = None) -> None:
    """Upsert một DataFrame nến (đã có cột lineage) vào bảng candles."""
    dsn = dsn or pg_dsn()
    cols = CANDLE_DB_COLS
    _utc_strings(df.select(*cols)).foreachPartition(
        lambda rows: upsert_rows("candles", cols, ["symbol", "minute", "source"], rows, dsn)
    )


def append_versions(df: DataFrame, batch_id: int, dsn: str | None = None) -> None:
    """C5: lưu mọi phiên bản nến streaming (dùng với output_mode=update)."""
    dsn = dsn or pg_dsn()
    cols = ["symbol", "minute", "source", "batch_id", "open", "high", "low", "close", "volume", "trades"]
    _utc_strings(df.withColumn("batch_id", F.lit(batch_id)).select(*cols)).foreachPartition(
        lambda rows: upsert_rows("candle_versions", cols, ["symbol", "minute", "source", "batch_id"], rows, dsn)
    )


def query(sql: str, params: tuple | None = None, dsn: str | None = None) -> list[tuple]:
    with psycopg.connect(dsn or pg_dsn()) as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall() if cur.description else []


def execute(sql: str, params: tuple | None = None, dsn: str | None = None) -> None:
    with psycopg.connect(dsn or pg_dsn()) as conn:
        conn.execute(sql, params)
        conn.commit()


def table_fingerprint(where: str = "TRUE", dsn: str | None = None) -> tuple[int, str]:
    """E4 (chạy lại batch): (số dòng, md5 nội dung) — phải không đổi sau lần chạy thứ 2."""
    sql = f"""
        SELECT count(*), md5(coalesce(string_agg(
            concat_ws('|', symbol, minute, source, open, high, low, close, volume, trades),
            ',' ORDER BY symbol, minute, source), ''))
        FROM candles WHERE {where}
    """
    return query(sql, dsn=dsn)[0]
