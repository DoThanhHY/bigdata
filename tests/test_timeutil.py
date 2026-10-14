"""C1 — Contract test: 3 nguồn × 2 giai đoạn (2022 ms, 2026 us) đều quy về cùng micro-giây."""

import pytest

from bdi.timeutil import detect_unit, to_micros, to_micros_col, to_micros_naive

# Cùng một thời điểm 2022-11-09T00:00:00.123Z ở các đơn vị khác nhau
T22_US = 1_667_952_000_123_000
# 2026-08-15T12:00:00.456789Z
T26_US = 1_786_795_200_456_789

CASES = [
    # (nguồn, giá trị thô, micro-giây kỳ vọng)
    ("aggTrades CSV 2022 (ms)", T22_US // 1000, T22_US),
    ("aggTrades CSV 2026 (us)", T26_US, T26_US),
    ("klines CSV 2022 (ms)", 1_667_952_000_000, 1_667_952_000_000_000),
    ("klines CSV 2026 (us)", 1_786_795_200_000_000, 1_786_795_200_000_000),
    ("WS JSON 2022 (ms)", T22_US // 1000, T22_US),
    ("WS JSON 2026 (ms mặc định)", T26_US // 1000, T26_US // 1000 * 1000),
    ("WS JSON 2026 timeUnit=MICROSECOND", T26_US, T26_US),
    ("nano-giây", T26_US * 1000, T26_US),
    ("giây", 1_786_795_200, 1_786_795_200_000_000),
]


@pytest.mark.parametrize("name,raw,expected", CASES, ids=[c[0] for c in CASES])
def test_to_micros_python(name, raw, expected):
    assert to_micros(raw) == expected


def test_to_micros_spark_matches_python(spark):
    df = spark.createDataFrame([(c[1],) for c in CASES], "v long")
    got = [r[0] for r in df.select(to_micros_col("v")).collect()]
    assert got == [c[2] for c in CASES]


def test_spark_accepts_string_column(spark):
    df = spark.createDataFrame([(str(T26_US),), (str(T22_US // 1000),)], "v string")
    assert [r[0] for r in df.select(to_micros_col("v")).collect()] == [T26_US, T22_US]


def test_naive_breaks_2026_only():
    assert to_micros_naive(T22_US // 1000) == T22_US          # 2022 vẫn đúng
    assert to_micros_naive(T26_US) == T26_US * 1000           # 2026 sai 1000 lần (năm ~58000)


def test_detect_unit_boundaries():
    assert detect_unit(10**12) == "ms"
    assert detect_unit(10**14) == "us"
    with pytest.raises(ValueError):
        detect_unit(12345)
