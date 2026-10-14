"""C1 — Lược đồ chuẩn (canonical schema) dùng chung cho 3 nguồn.

Nguồn:
  1. CSV aggTrades  (data.binance.vision, KHÔNG có header)
  2. CSV klines 1m  (data.binance.vision, KHÔNG có header) — "official"
  3. JSON WebSocket  (<symbol>@aggTrade, qua Kafka)

Mọi nguồn trade được đưa về CANONICAL_TRADE_COLS:
  symbol, agg_id, price, qty, first_trade_id, last_trade_id,
  ts_us (micro-giây), event_time (timestamp), is_buyer_maker, origin

Giá/khối lượng LUÔN đọc dưới dạng chuỗi rồi cast sang Decimal — không bao giờ
đi qua double (bẫy D2 trong plan).
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from bdi.timeutil import micros_to_ts, to_micros_col, to_micros_naive_col

# Binance spot: tối đa 8 chữ số thập phân. 38 chữ số tổng để sum() không tràn.
PRICE = T.DecimalType(24, 8)
QTY = T.DecimalType(24, 8)
VOLUME = T.DecimalType(38, 8)
QUOTE_VOLUME = T.DecimalType(38, 16)

# --- 1. CSV aggTrades (không header) -------------------------------------------
AGGTRADES_CSV_COLS = [
    "agg_id", "price", "qty", "first_trade_id", "last_trade_id",
    "transact_time", "is_buyer_maker", "is_best_match",
]
AGGTRADES_CSV_SCHEMA = T.StructType([T.StructField(c, T.StringType()) for c in AGGTRADES_CSV_COLS])

# --- 2. CSV klines (không header) ----------------------------------------------
KLINES_CSV_COLS = [
    "open_time", "open", "high", "low", "close", "volume", "close_time",
    "quote_volume", "trades", "taker_buy_base", "taker_buy_quote", "ignore",
]
KLINES_CSV_SCHEMA = T.StructType([T.StructField(c, T.StringType()) for c in KLINES_CSV_COLS])

# --- 3. JSON WebSocket aggTrade (combined stream: {"stream":..., "data":{...}}) --
# Producer đã bóc "data" và thêm recv_ts_us + origin trước khi gửi vào Kafka.
WS_AGGTRADE_SCHEMA = T.StructType([
    T.StructField("e", T.StringType()),    # "aggTrade"
    T.StructField("E", T.LongType()),      # event time
    T.StructField("s", T.StringType()),    # symbol
    T.StructField("a", T.LongType()),      # aggregate trade id
    T.StructField("p", T.StringType()),    # price (chuỗi!)
    T.StructField("q", T.StringType()),    # qty (chuỗi!)
    T.StructField("f", T.LongType()),      # first trade id
    T.StructField("l", T.LongType()),      # last trade id
    T.StructField("T", T.LongType()),      # trade time
    T.StructField("m", T.BooleanType()),   # is buyer maker
    T.StructField("recv_ts_us", T.LongType()),
    T.StructField("origin", T.StringType()),  # ws | rest_backfill | replay
])

CANONICAL_TRADE_COLS = [
    "symbol", "agg_id", "price", "qty", "first_trade_id", "last_trade_id",
    "ts_us", "event_time", "is_buyer_maker", "origin",
]


def _ts(c: str, naive: bool) -> Column:
    return to_micros_naive_col(c) if naive else to_micros_col(c)


def canonical_from_aggtrades_csv(df: DataFrame, symbol: str | Column, naive_time: bool = False) -> DataFrame:
    sym = F.lit(symbol) if isinstance(symbol, str) else symbol
    return (
        df.select(
            sym.alias("symbol"),
            F.col("agg_id").cast("long").alias("agg_id"),
            F.col("price").cast(PRICE).alias("price"),
            F.col("qty").cast(QTY).alias("qty"),
            F.col("first_trade_id").cast("long").alias("first_trade_id"),
            F.col("last_trade_id").cast("long").alias("last_trade_id"),
            _ts("transact_time", naive_time).alias("ts_us"),
            (F.lower(F.col("is_buyer_maker")) == "true").alias("is_buyer_maker"),
            F.lit("batch_csv").alias("origin"),
        )
        .withColumn("event_time", micros_to_ts("ts_us"))
        .select(*CANONICAL_TRADE_COLS)
    )


def canonical_from_ws_json(value_col: Column, naive_time: bool = False) -> list[Column]:
    """Trả về list cột canonical từ cột chuỗi JSON (Kafka value)."""
    j = F.from_json(value_col, WS_AGGTRADE_SCHEMA)
    ts = to_micros_naive_col(j["T"]) if naive_time else to_micros_col(j["T"])
    return [
        j["s"].alias("symbol"),
        j["a"].alias("agg_id"),
        j["p"].cast(PRICE).alias("price"),
        j["q"].cast(QTY).alias("qty"),
        j["f"].alias("first_trade_id"),
        j["l"].alias("last_trade_id"),
        ts.alias("ts_us"),
        micros_to_ts(ts).alias("event_time"),
        j["m"].alias("is_buyer_maker"),
        F.coalesce(j["origin"], F.lit("ws")).alias("origin"),
        j["recv_ts_us"].alias("recv_ts_us"),
    ]


def official_from_klines_csv(df: DataFrame, symbol: str | Column, naive_time: bool = False) -> DataFrame:
    """Nến chính thức (official) về cùng lược đồ với output của build_candles."""
    sym = F.lit(symbol) if isinstance(symbol, str) else symbol
    open_us = _ts("open_time", naive_time)
    return df.select(
        sym.alias("symbol"),
        micros_to_ts(open_us).alias("minute"),
        F.col("open").cast(PRICE).alias("open"),
        F.col("high").cast(PRICE).alias("high"),
        F.col("low").cast(PRICE).alias("low"),
        F.col("close").cast(PRICE).alias("close"),
        F.col("volume").cast(VOLUME).alias("volume"),
        F.col("quote_volume").cast(QUOTE_VOLUME).alias("quote_volume"),
        F.col("trades").cast("long").alias("trades"),
    )
