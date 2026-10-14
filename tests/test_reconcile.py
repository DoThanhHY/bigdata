"""C3 — so khớp tuyệt đối + gắn nhãn nguyên nhân."""

from decimal import Decimal as D

import pandas as pd

from bdi.reconcile.compare import compare, simulate_late
from bdi.reconcile.taxonomy import label


def _c(minute, o, h, l, c, v, t, sym="BTCUSDT"):
    return {"symbol": sym, "minute": pd.Timestamp(minute, tz="UTC"), "open": D(o), "high": D(h),
            "low": D(l), "close": D(c), "volume": D(v), "trades": t}


def test_labels():
    off = pd.DataFrame([
        _c("2026-10-14 00:00", "1", "2", "1", "2", "1.0", 10),   # khớp
        _c("2026-10-14 00:01", "1", "2", "1", "2", "1.0", 10),   # stream thiếu do gap
        _c("2026-10-14 00:02", "1", "2", "1", "2", "1.0", 10),   # stream thừa
        _c("2026-10-14 00:03", "1", "2", "1", "2", "1.0", 10),   # stream thiếu do trễ
        _c("2026-10-14 00:04", "1", "2", "1", "2", "1.00000001", 10),  # chỉ lệch volume
        _c("2026-10-14 00:05", "1", "1", "1", "1", "0", 0),      # phút trống
    ])
    st = pd.DataFrame([
        _c("2026-10-14 00:00", "1.0", "2.00", "1", "2", "1", 10),  # Decimal 1.0 == 1
        _c("2026-10-14 00:01", "1", "2", "1", "2", "0.5", 5),
        _c("2026-10-14 00:02", "1", "2", "1", "2", "1.5", 12),
        _c("2026-10-14 00:03", "1", "2", "1", "2", "0.9", 9),
        _c("2026-10-14 00:04", "1", "2", "1", "2", "1.0", 10),
    ])
    gaps = pd.DataFrame({"symbol": ["BTCUSDT"],
                         "disconnected_at": [pd.Timestamp("2026-10-14 00:01:20", tz="UTC")],
                         "reconnected_at": [pd.Timestamp("2026-10-14 00:01:40", tz="UTC")]})
    late = pd.DataFrame({"symbol": ["BTCUSDT"], "minute": [pd.Timestamp("2026-10-14 00:03", tz="UTC")],
                         "late_count": [1]})
    m = compare(st, off).sort_values("minute").reset_index(drop=True)
    causes = label(m, gaps, late).tolist()
    assert m["all_match"].tolist()[0]
    assert causes[1:] == ["DISCONNECT_GAP", "DUPLICATE", "LATE_BEYOND_WATERMARK",
                          "NUMERIC_PRECISION", "EMPTY_MINUTE"]
    assert pd.isna(causes[0])


def test_simulate_late():
    raw = pd.DataFrame({
        "symbol": "BTCUSDT", "kafka_partition": 0,
        "kafka_offset": [0, 1, 2],
        "kafka_ts": pd.to_datetime([1, 2, 3], unit="s"),
        "ts_us": [60_000_000, 70_000_000, 62_000_000],  # bản tin thứ 3 trễ 8s
    })
    raw["minute"] = pd.to_datetime(raw["ts_us"] // 60_000_000 * 60, unit="s", utc=True)
    assert simulate_late(raw, 5)["late_count"].sum() == 1
    assert simulate_late(raw, 10).empty
