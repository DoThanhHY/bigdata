"""C2 — "Viết một lần, chạy hai tầng": logic dựng nến 1 phút dùng chung.

`build_candles(df)` là hàm PySpark thuần, KHÔNG biết mình đang chạy batch hay
streaming. Tầng batch (bdi.batch.build_batch) và tầng streaming
(bdi.stream.stream_candles) cùng import hàm này.

Ba điểm kỹ thuật:
  1. Open/Close dùng min_by/max_by theo struct(ts_us, agg_id): first()/last()
     không đảm bảo thứ tự trong streaming (và cả trong batch sau shuffle).
  2. Giá/khối lượng là Decimal (xem bdi.schema) => sum volume khớp tuyệt đối.
  3. trades = sum(last_trade_id - first_trade_id + 1) để so với trường
     "number of trades" của klines (1 aggTrade gồm nhiều trade lẻ).

Input: DataFrame canonical (bdi.schema.CANONICAL_TRADE_COLS), phải có event_time.
Với streaming, caller gọi .withWatermark("event_time", ...) TRƯỚC khi truyền vào.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from bdi.schema import QUOTE_VOLUME, VOLUME

CANDLE_COLS = [
    "symbol", "minute", "open", "high", "low", "close",
    "volume", "quote_volume", "trades", "n_agg", "first_agg_id", "last_agg_id",
]


def build_candles(df: DataFrame, interval: str = "1 minute") -> DataFrame:
    order_key = F.struct(F.col("ts_us"), F.col("agg_id"))
    return (
        df.groupBy(F.col("symbol"), F.window(F.col("event_time"), interval).alias("w"))
        .agg(
            F.min_by("price", order_key).alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.max_by("price", order_key).alias("close"),
            F.sum("qty").cast(VOLUME).alias("volume"),
            F.sum(F.col("price") * F.col("qty")).cast(QUOTE_VOLUME).alias("quote_volume"),
            F.sum(F.col("last_trade_id") - F.col("first_trade_id") + 1).alias("trades"),
            F.count(F.lit(1)).alias("n_agg"),
            F.min("agg_id").alias("first_agg_id"),
            F.max("agg_id").alias("last_agg_id"),
        )
        .select(F.col("symbol"), F.col("w.start").alias("minute"), *CANDLE_COLS[2:])
    )


def build_candles_naive_first_last(df: DataFrame, interval: str = "1 minute") -> DataFrame:
    """Phiên bản SAI có chủ đích (first/last) — chỉ dùng để minh họa trong test/báo cáo."""
    return (
        df.groupBy(F.col("symbol"), F.window(F.col("event_time"), interval).alias("w"))
        .agg(
            F.first("price").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.last("price").alias("close"),
            F.sum("qty").alias("volume"),
        )
        .select(F.col("symbol"), F.col("w.start").alias("minute"), "open", "high", "low", "close", "volume")
    )
