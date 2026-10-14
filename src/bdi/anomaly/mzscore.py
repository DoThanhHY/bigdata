"""E7 — Phát hiện bất thường bằng modified z-score (Iglewicz–Hoaglin).

    M = 0.6745 · (x − median) / MAD,   báo động khi |M| > 3.5

Áp lên log-return và log(1 + khối lượng) của nến 1 phút; median/MAD tính trên
cửa sổ rolling W phút TRƯỚC phút hiện tại (không nhìn trước).

Ba lựa chọn rút ra từ thực nghiệm (xem docs/experiments/E7-anomaly-ftx.md):
  - W = 1440 (1 ngày): cửa sổ 60 phút thích nghi quá nhanh, cả ngày FTX trông "bình thường".
  - log(1 + volume): khối lượng có đuôi rất dày, dùng giá trị thô thì báo động liên tục.
  - Sàn tuyệt đối |log-return| ≥ min_abs_return: M là độ bất ngờ TƯƠNG ĐỐI; thị trường
    càng yên thì MAD càng nhỏ, nên các cú nhảy rất nhỏ cũng bị gắn cờ.

MAD được xấp xỉ bằng median(|x_i − median_i|) với median_i là median cửa sổ tại
từng điểm (vector hóa, nhanh hơn ~100 lần so với tính đúng từng cửa sổ). Đổi lại
cần 2W − 1 phút lịch sử mới có điểm đầu tiên (W = 1 ngày => 2 ngày khởi động).

Dùng ở 2 nơi:
  - batch:  detect(candles_df)                  (experiments/e7_anomaly.py)
  - stream: detect_for_batch(spark_batch, src)  (gọi trong foreachBatch, --anomaly)
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from bdi.config import settings

K = 0.6745
METRICS = {"log_return": "log_return", "volume": "log_volume"}


def rolling_mz(x: pd.Series, window: int) -> pd.Series:
    # chấp nhận thiếu tối đa 10% điểm trong cửa sổ (phút trống, khoảng gián đoạn ngắn)
    mp = max(1, int(window * 0.9))
    prev = x.shift(1)
    med = prev.rolling(window, min_periods=mp).median()
    mad = (prev - med).abs().rolling(window, min_periods=mp).median()
    return K * (x - med) / mad.replace(0, np.nan)


def features(c: pd.DataFrame) -> pd.DataFrame:
    c = c.sort_values(["symbol", "minute"]).copy()
    close = c["close"].astype(float)
    prev_close = close.groupby(c["symbol"]).shift(1)
    # không nối qua khoảng trống dữ liệu (VD 2022-11 -> 2026-06)
    gap = c["minute"].diff().ne(pd.Timedelta(minutes=1)) | c["symbol"].ne(c["symbol"].shift(1))
    c["log_return"] = np.log(close / prev_close).mask(gap)
    c["log_volume"] = np.log1p(c["volume"].astype(float))
    return c


def detect(candles: pd.DataFrame, window: int | None = None, threshold: float | None = None,
           min_abs_return: float | None = None) -> pd.DataFrame:
    """candles: symbol, minute, close, volume. Trả về mọi điểm vượt ngưỡng (dạng dài)."""
    cfg = settings()["anomaly"]
    window = window or cfg["window_minutes"]
    threshold = threshold if threshold is not None else cfg["threshold"]
    min_abs_return = min_abs_return if min_abs_return is not None else cfg.get("min_abs_return", 0)
    c = features(candles)
    out = []
    for metric, col in METRICS.items():
        mz = c.groupby("symbol", group_keys=False)[col].apply(lambda s: rolling_mz(s, window))
        hit = mz.abs() > threshold
        if metric == "log_return" and min_abs_return:
            hit &= c[col].abs() >= min_abs_return
        out.append(pd.DataFrame({
            "symbol": c.loc[hit, "symbol"], "minute": c.loc[hit, "minute"], "metric": metric,
            "value": c.loc[hit, col], "mz_score": mz[hit],
        }))
    return pd.concat(out, ignore_index=True)


def detect_for_batch(batch, source: str, history_source: str | None = None) -> None:
    """Gọi trong foreachBatch: tính lại điểm cho các phút vừa được ghi.

    history_source: nguồn bù lịch sử khi nguồn chính chưa đủ 2W phút (VD luồng live mới
    chạy vài giờ => dùng 'official' do rest_klines kéo về). Phút nào nguồn chính có thì
    ưu tiên nguồn chính.
    """
    from pyspark.sql import functions as F

    from bdi.reconcile.compare import load_candles
    from bdi.sink import upsert_rows

    cfg = settings()["anomaly"]
    w = cfg["window_minutes"]
    # định dạng sẵn theo UTC: toPandas() trả timestamp theo giờ máy (xem sink._utc_strings)
    keys = batch.select("symbol", F.date_format("minute", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("minute")).toPandas()
    if keys.empty:
        return
    # MAD xấp xỉ cần 2W − 1 điểm (median cửa sổ, rồi median độ lệch trên W điểm nữa)
    start = (pd.to_datetime(keys["minute"].min(), utc=True) - pd.Timedelta(minutes=2 * w + 1)).isoformat()
    syms = sorted(keys["symbol"].unique())
    hist = load_candles(source, syms, start=start)
    if history_source:
        base = load_candles(history_source, syms, start=start)
        hist = pd.concat([hist, base]).drop_duplicates(["symbol", "minute"], keep="first")
    if hist.empty:
        return
    found = detect(hist, w, cfg["threshold"])
    new_minutes = set(pd.to_datetime(keys["minute"], utc=True))
    found = found[found["minute"].isin(new_minutes)]
    rows = ((r.symbol, r.minute.to_pydatetime(), r.metric, float(r.value), float(r.mz_score), source)
            for r in found.itertuples(index=False))
    upsert_rows("anomalies", ["symbol", "minute", "metric", "value", "mz_score", "source"],
                ["symbol", "minute", "metric", "source"], rows)
