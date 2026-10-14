"""C4 — Record linkage mờ giữa bản ghi stream và batch, với ground truth miễn phí.

Cố tình BỎ agg_id khi ghép; chỉ dùng (symbol, giá, khối lượng, |Δt| ≤ δ).
agg_id được giữ riêng làm nhãn đúng để tính Precision / Recall / F1.

Chiến lược blocking (giảm số cặp phải so sánh):
  none            : chỉ chặn theo symbol => so mọi cặp (tích Đề-các). Số cặp được TÍNH
                    giải tích n_s × n_b; kết quả ghép giống hệt "window" vì ràng buộc
                    |Δt| ≤ δ loại mọi cặp ngoài cửa sổ.
  window          : (symbol, ts // δ)           — bucket rộng đúng bằng δ, mở rộng ±1
  second          : (symbol, giây)              — mở rộng ±⌈δ/1s⌉ bucket để không lỡ cặp sát biên
  second_price    : (symbol, giây, bucket giá)  — bucket giá = floor(price / price_bucket)

Hai cách quyết định ghép (tham số `method`):
  greedy  : với mỗi bản ghi stream, chọn ứng viên batch có |Δt| nhỏ nhất trong số ứng
            viên khớp giá + khối lượng; ràng buộc 1-1 tham lam theo |Δt| tăng dần.
  ordered : trong từng nhóm (symbol, giá, khối lượng), đi hai con trỏ theo thời gian và
            ghép bản ghi thứ i của stream với bản ghi thứ i của batch nếu |Δt| ≤ δ.
            Dựa trên việc Kafka (key = symbol) giữ thứ tự giao dịch; giải quyết các nhóm
            "trùng hệt" (cùng giây, giá, khối lượng) mà greedy hay ghép nhầm.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

STRATEGIES = ["none", "window", "second", "second_price"]


def _bucket_keys(df: pd.DataFrame, strategy: str, delta_us: int, price_bucket: float,
                 expand: bool) -> pd.DataFrame:
    """Gán khóa block; phía stream (expand=True) được nhân bản sang các bucket kề."""
    width = max(delta_us, 1) if strategy == "window" else 1_000_000
    df = df.assign(_b=df["ts_us"] // width)
    if strategy == "second_price":
        df = df.assign(_p=np.floor(df["price"].astype(float) / price_bucket).astype("int64"))
    if expand:
        k = int(np.ceil(delta_us / width))
        df = pd.concat([df.assign(_b=df["_b"] + o) for o in range(-k, k + 1)], ignore_index=True)
    return df


def candidate_pairs(stream: pd.DataFrame, batch: pd.DataFrame, delta_us: int, strategy: str,
                    price_bucket: float = 1.0) -> pd.DataFrame:
    keys = ["symbol", "_b"] + (["_p"] if strategy == "second_price" else [])
    s = _bucket_keys(stream, strategy, delta_us, price_bucket, expand=True)
    b = _bucket_keys(batch, strategy, delta_us, price_bucket, expand=False)
    pairs = s.merge(b, on=keys, suffixes=("_s", "_b"))
    pairs["dt_us"] = (pairs["ts_us_s"] - pairs["ts_us_b"]).abs()
    return pairs


def link(stream: pd.DataFrame, batch: pd.DataFrame, delta_us: int, strategy: str = "second",
         price_bucket: float = 1.0, qty_tol: float = 0.0, method: str = "greedy") -> tuple[pd.DataFrame, int]:
    """Trả về (các cặp được ghép, số cặp ứng viên đã so sánh).

    stream/batch: cột symbol, ts_us, price, qty, rid (id ẩn danh), agg_id (CHỈ để chấm điểm).
    """
    if strategy == "none":
        n_compared = int((stream.groupby("symbol").size() * batch.groupby("symbol").size()).fillna(0).sum())
        pairs = candidate_pairs(stream, batch, delta_us, "window", price_bucket)
    else:
        pairs = candidate_pairs(stream, batch, delta_us, strategy, price_bucket)
        n_compared = len(pairs)
    ok = (pairs["dt_us"] <= delta_us) & (pairs["price_s"] == pairs["price_b"])
    if qty_tol == 0:
        ok &= pairs["qty_s"] == pairs["qty_b"]
    else:
        q_s, q_b = pairs["qty_s"].astype(float), pairs["qty_b"].astype(float)
        ok &= (q_s - q_b).abs() <= qty_tol * q_b.abs()
    cand = pairs.loc[ok, ["rid_s", "rid_b", "dt_us", "agg_id_s", "agg_id_b"]]
    if method == "ordered":
        return _ordered(stream, batch, delta_us), n_compared
    # 1-1 tham lam theo |Δt|
    cand = cand.sort_values("dt_us").drop_duplicates("rid_s").drop_duplicates("rid_b")
    return cand, n_compared


def _ordered(stream: pd.DataFrame, batch: pd.DataFrame, delta_us: int) -> pd.DataFrame:
    """Ghép giữ thứ tự trong từng nhóm (symbol, price, qty) bằng hai con trỏ."""
    key = ["symbol", "price", "qty"]
    s = stream.sort_values("ts_us").groupby(key, sort=False)
    b = {k: g for k, g in batch.sort_values("ts_us").groupby(key, sort=False)}
    out = []
    for k, gs in s:
        gb = b.get(k)
        if gb is None:
            continue
        ts_s, ts_b = gs["ts_us"].to_numpy(), gb["ts_us"].to_numpy()
        i = j = 0
        while i < len(ts_s) and j < len(ts_b):
            d = ts_s[i] - ts_b[j]
            if abs(d) <= delta_us:
                out.append((gs["rid"].iat[i], gb["rid"].iat[j], abs(d), gs["agg_id"].iat[i], gb["agg_id"].iat[j]))
                i += 1
                j += 1
            elif d > 0:
                j += 1      # bản ghi batch quá sớm, không có cặp
            else:
                i += 1      # bản ghi stream quá sớm, không có cặp
    return pd.DataFrame(out, columns=["rid_s", "rid_b", "dt_us", "agg_id_s", "agg_id_b"])


def score(matches: pd.DataFrame, stream: pd.DataFrame, batch: pd.DataFrame) -> dict:
    tp = int((matches["agg_id_s"] == matches["agg_id_b"]).sum())
    fp = len(matches) - tp
    # số cặp đúng có thể có = bản ghi stream có agg_id tồn tại bên batch
    positives = int(stream["agg_id"].isin(batch["agg_id"]).sum())
    fn = positives - tp
    p = tp / (tp + fp) if tp + fp else 0.0
    r = tp / positives if positives else 0.0
    f1 = 2 * p * r / (p + r) if p + r else 0.0
    return {"tp": tp, "fp": fp, "fn": fn, "precision": p, "recall": r, "f1": f1}


def prepare(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    """Gán id ẩn danh; agg_id chỉ còn là nhãn."""
    out = df[["symbol", "ts_us", "price", "qty", "agg_id"]].reset_index(drop=True)
    out["rid"] = prefix + out.index.astype(str)
    return out
