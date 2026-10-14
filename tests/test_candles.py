"""C2 — build_candles: Decimal, min_by/max_by, số trades; cùng kết quả batch vs streaming."""

from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from bdi.candles import build_candles
from bdi.schema import AGGTRADES_CSV_SCHEMA, canonical_from_aggtrades_csv

M0 = 1_786_795_200_000_000  # 2026-08-15T12:00:00Z (us)

# agg_id, price, qty, first, last, ts(us), maker, best — CỐ Ý xáo trộn thứ tự dòng
ROWS = [
    ("103", "100.30000000", "0.10000000", "13", "13", str(M0 + 40_000_000), "False", "True"),
    ("100", "100.00000000", "0.10000000", "10", "10", str(M0 + 1_000_000), "True", "True"),
    ("102", "99.50000000", "0.20000000", "12", "12", str(M0 + 30_000_000), "True", "True"),
    ("101", "101.10000000", "0.30000000", "11", "11", str(M0 + 20_000_000), "False", "True"),
    # cùng timestamp với 103 nhưng agg_id lớn hơn => là close
    ("104", "100.70000000", "0.10000000", "14", "16", str(M0 + 40_000_000), "False", "True"),
    # phút kế tiếp
    ("105", "100.80000000", "1.00000000", "17", "17", str(M0 + 61_000_000), "False", "True"),
]


@pytest.fixture
def trades(spark):
    raw = spark.createDataFrame(ROWS, AGGTRADES_CSV_SCHEMA).repartition(3)
    return canonical_from_aggtrades_csv(raw, "BTCUSDT")


def test_ohlcv_exact(trades):
    c = {r["minute"].minute: r for r in build_candles(trades).collect()}
    m = c[0]
    assert m["open"] == Decimal("100.00000000")
    assert m["high"] == Decimal("101.10000000")
    assert m["low"] == Decimal("99.50000000")
    assert m["close"] == Decimal("100.70000000")  # tie ts -> agg_id lớn nhất
    assert m["volume"] == Decimal("0.80000000")   # Decimal: không có 0.7999999999
    assert m["trades"] == 1 + 1 + 1 + 1 + 3       # sum(last - first + 1)
    assert m["n_agg"] == 5
    assert c[1]["open"] == c[1]["close"] == Decimal("100.80000000")


def test_volume_is_decimal_not_double(trades):
    dt = dict(build_candles(trades).dtypes)
    assert dt["volume"].startswith("decimal") and dt["open"].startswith("decimal")


def test_batch_equals_streaming(spark, trades, tmp_path):
    """Cùng hàm, chạy dạng streaming (file source) phải cho đúng kết quả như batch."""
    src = tmp_path / "in"
    trades.write.parquet(str(src))
    batch = {(r["symbol"], r["minute"]): r.asDict() for r in build_candles(trades).collect()}

    stream = (spark.readStream.schema(trades.schema).parquet(str(src))
              .withWatermark("event_time", "0 seconds"))
    q = (build_candles(stream).writeStream.outputMode("update").format("memory")
         .queryName("candles_test").start())
    q.processAllAvailable()
    q.stop()
    got = {(r["symbol"], r["minute"]): r.asDict() for r in spark.table("candles_test").collect()}
    assert got == batch
