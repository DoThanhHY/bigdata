"""C1 — Chuẩn hóa thời gian nhận biết schema drift.

Binance đổi đơn vị timestamp trong file spot từ mili-giây (trước 2025) sang
micro-giây (từ 2025). Thay vì viết cứng "trước 2025 là ms", ta suy ra đơn vị từ
ĐỘ LỚN giá trị, rồi quy mọi thứ về micro-giây (epoch, UTC):

    ~1.6e12  -> mili-giây      (năm 2001..5138 nằm trong [1e12, 1e14))
    ~1.6e15  -> micro-giây     ([1e14, 1e17))
    ~1.6e18  -> nano-giây      ([1e17, ...))
    ~1.6e9   -> giây           ([1e9, 1e12))

Mỗi hàm có 2 bản: Python thuần (dùng trong producer / test) và biểu thức Spark
(dùng trong batch + streaming), cùng một ngưỡng.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

# Ngưỡng biên dưới của từng đơn vị
_S_MIN, _MS_MIN, _US_MIN, _NS_MIN = 10**9, 10**12, 10**14, 10**17

# Mốc "vô lý" dùng trong E1: năm 3000 tính bằng micro-giây
YEAR_3000_US = 32_503_680_000 * 10**6


def detect_unit(value: int) -> str:
    v = abs(int(value))
    if v >= _NS_MIN:
        return "ns"
    if v >= _US_MIN:
        return "us"
    if v >= _MS_MIN:
        return "ms"
    if v >= _S_MIN:
        return "s"
    raise ValueError(f"timestamp quá nhỏ, không xác định được đơn vị: {value}")


def to_micros(value: int) -> int:
    """Bản drift-aware (Python)."""
    v = int(value)
    unit = detect_unit(v)
    return {"s": v * 1_000_000, "ms": v * 1_000, "us": v, "ns": v // 1_000}[unit]


def to_micros_naive(value: int) -> int:
    """Bản naive dùng cho ablation E1: coi mọi giá trị là mili-giây."""
    return int(value) * 1_000


def to_micros_col(c: Column | str) -> Column:
    """Bản drift-aware (Spark). Nhận cột kiểu số/chuỗi, trả về LongType micro-giây."""
    v = F.col(c) if isinstance(c, str) else c
    v = v.cast("long")
    a = F.abs(v)
    return (
        F.when(a >= F.lit(_NS_MIN), F.floor(v / 1_000).cast("long"))
        .when(a >= F.lit(_US_MIN), v)
        .when(a >= F.lit(_MS_MIN), v * 1_000)
        .when(a >= F.lit(_S_MIN), v * 1_000_000)
        .otherwise(F.lit(None).cast("long"))
    )


def to_micros_naive_col(c: Column | str) -> Column:
    v = F.col(c) if isinstance(c, str) else c
    return v.cast("long") * 1_000


def micros_to_ts(c: Column | str) -> Column:
    """micro-giây -> TimestampType (UTC)."""
    v = F.col(c) if isinstance(c, str) else c
    return F.timestamp_micros(v)
