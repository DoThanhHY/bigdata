"""E7-ML — Phát hiện bất thường giao dịch đa biến bằng Học máy (Multivariate ML).

Mở rộng phương pháp thống kê đơn biến Modified Z-Score (src/bdi/anomaly/mzscore.py)
sang không gian đặc trưng đa chiều (11 đặc trưng Vi mô + Liên thị trường) trên nến 1 phút:
  A. Đặc trưng vi mô từng cặp (Single-Asset Microstructure):
     1. abs_return        : |ln(C_t / C_{t-1})| — biên độ lợi suất đóng cửa tuyệt đối
     2. hl_range          : ln(H_t / L_t)       — biên độ đỉnh–đáy trong phút (bắt rút râu nến / flash wick)
     3. wick_ratio        : 1 - |ln(C_t / O_t)| / (ln(H_t / L_t) + 1e-8) — tỉ lệ bóng nến
     4. mz_return         : |M_W(ln(C_t / C_{t-1}))| — độ lệch chuẩn hóa lợi suất (cửa sổ W=1440)
     5. mz_range          : M_W(ln(H_t / L_t))       — độ lệch chuẩn hóa biên độ đỉnh–đáy
     6. mz_volume         : M_W(ln(1 + V_t))         — độ lệch chuẩn hóa khối lượng
     7. mz_trades         : M_W(ln(1 + N_t))         — độ lệch chuẩn hóa số lệnh khớp
     8. mz_avg_size       : M_W(ln(1 + V_t / N_t))   — độ lệch chuẩn hóa kích thước lệnh trung bình
  B. Đặc trưng cụm biến động & lây lan liên thị trường (Temporal Clustering & Cross-Asset Contagion):
     9. roll15_mz_return  : trung bình mz_return trong 15 phút gần nhất (t-14..t) của cặp đó
    10. market_mz_return  : trung bình mz_return của cả 5 cặp tại cùng phút t (đồng pha giá)
    11. market_mz_volume  : trung bình mz_volume của cả 5 cặp tại cùng phút t (đồng pha thanh khoản)

Hỗ trợ 2 nhóm mô hình từ thư viện chuẩn scikit-learn:
  - Không giám sát (Unsupervised): IsolationForest, EllipticEnvelope (Robust Mahalanobis)
  - Có giám sát (Supervised Cross-Event): LogisticRegression, RandomForestClassifier,
    HistGradientBoostingClassifier (tương đương LightGBM/XGBoost)
"""

from __future__ import annotations

import functools
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.covariance import EllipticEnvelope
from sklearn.ensemble import (
    HistGradientBoostingClassifier,
    IsolationForest,
    RandomForestClassifier,
)
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import RobustScaler

from bdi.anomaly.mzscore import rolling_mz
from bdi.config import path, settings

MODEL_PATHS = {
    1: path("results") / "e7" / "hist_gbdt_model.joblib",
    2: path("results") / "e7" / "random_forest_model.joblib",
}
MODEL_NAMES = {
    1: "HistGradientBoosting",
    2: "RandomForest",
}
MODEL_METRICS = {
    1: "ml_gbdt",
    2: "ml_rf",
}
DEFAULT_MODEL_PATH = MODEL_PATHS[1]

FEATURE_COLS = [
    "abs_return",
    "hl_range",
    "wick_ratio",
    "mz_return",
    "mz_range",
    "mz_volume",
    "mz_trades",
    "mz_avg_size",
    "roll15_mz_return",
    "market_mz_return",
    "market_mz_volume",
]

# 5 đặc trưng cốt lõi chuẩn hóa bất biến theo chế độ thị trường (Giá + Biên độ + Thanh khoản + Đồng pha)
# dành cho mô hình không giám sát học trên năm 2026 và áp dụng sang năm 2022
UNSUP_FEATURE_COLS = [
    "mz_return",
    "mz_range",
    "mz_volume",
    "roll15_mz_return",
    "market_mz_return",
]


def extract_ml_features(candles: pd.DataFrame, window: int = 1440) -> pd.DataFrame:
    """Trích xuất 11 đặc trưng vi mô & liên thị trường không nhìn trước (causal window W phút)."""
    c = candles.sort_values(["symbol", "minute"]).copy()
    open_px = c["open"].astype(float)
    high_px = c["high"].astype(float)
    low_px = c["low"].astype(float).clip(lower=1e-12)
    close_px = c["close"].astype(float)
    vol = c["volume"].astype(float).clip(lower=0.0)
    trades = c["trades"].astype(float).clip(lower=1.0)

    prev_close = close_px.groupby(c["symbol"]).shift(1)
    gap = c["minute"].diff().ne(pd.Timedelta(minutes=1)) | c["symbol"].ne(c["symbol"].shift(1))

    log_ret = np.log(close_px / prev_close).mask(gap)
    hl_range = np.log((high_px / low_px).clip(lower=1.0))
    body = np.log(close_px / open_px.clip(lower=1e-12)).abs()
    wick_ratio = (1.0 - (body / (hl_range + 1e-8))).clip(0.0, 1.0)
    log_vol = np.log1p(vol)
    log_trades = np.log1p(trades)
    log_avg_size = np.log1p(vol / trades)

    c["log_return"] = log_ret
    c["abs_return"] = log_ret.abs()
    c["hl_range"] = hl_range
    c["wick_ratio"] = wick_ratio
    c["log_volume"] = log_vol
    c["log_trades"] = log_trades
    c["log_avg_size"] = log_avg_size

    c["mz_raw"] = c.groupby("symbol", group_keys=False)["log_return"].apply(lambda s: rolling_mz(s, window))
    c["mz_return"] = c["mz_raw"].abs().clip(upper=25.0)
    c["mz_range"] = c.groupby("symbol", group_keys=False)["hl_range"].apply(lambda s: rolling_mz(s, window)).clip(0.0, 25.0)
    c["mz_volume"] = c.groupby("symbol", group_keys=False)["log_volume"].apply(lambda s: rolling_mz(s, window)).clip(0.0, 25.0)
    c["mz_trades"] = c.groupby("symbol", group_keys=False)["log_trades"].apply(lambda s: rolling_mz(s, window)).clip(0.0, 25.0)
    c["mz_avg_size"] = c.groupby("symbol", group_keys=False)["log_avg_size"].apply(lambda s: rolling_mz(s, window)).clip(0.0, 25.0)

    # Cụm độ lệch chuẩn hóa 15 phút gần nhất (không nhìn trước, bất biến theo mặt bằng giá)
    c["roll15_mz_return"] = (
        c.groupby("symbol", group_keys=False)["mz_return"]
        .apply(lambda s: s.rolling(15, min_periods=3).mean())
    )

    # Độ đồng pha liên thị trường tại đúng phút t (trung bình trên các cặp cùng phút)
    c["market_mz_return"] = c.groupby("minute")["mz_return"].transform("mean")
    c["market_mz_volume"] = c.groupby("minute")["mz_volume"].transform("mean")

    # Sàn biên độ tối thiểu đa biến (tương đương sàn |r| >= 0.5% của E7 nhưng cho phép cả biên độ đỉnh-đáy >= 0.8%)
    c["material_move"] = ((c["abs_return"] >= 0.005) | (c["hl_range"] >= 0.008)).astype(float)

    return c


def valid_feature_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Lọc các dòng đã đủ lịch sử khởi động cửa sổ 2W và làm sạch giá trị vô cực."""
    sub = df.replace([np.inf, -np.inf], np.nan).dropna(subset=FEATURE_COLS).copy()
    return sub


def build_shock_labels(df: pd.DataFrame, event_days: list[str],
                       min_return: float = 0.005, min_range: float = 0.008) -> np.ndarray:
    """Gán nhãn cú sốc thị trường thực tế (Ground-truth proxy cho đánh giá Cross-Event):
    y = 1 khi phút giao dịch nằm trong ngày sự kiện khủng hoảng lịch sử (FTX hoặc LUNA)
    VÀ có biến động giá/thanh khoản thực chất:
      - Hoặc dịch chuyển giá đóng cửa |log_return| >= 0.5% kèm độ lệch > 3.5
      - Hoặc biên độ quét râu nến trong phút ln(H/L) >= 0.8% kèm khối lượng đột biến (mz_volume > 2.0)
    """
    day_str = df["minute"].dt.strftime("%Y-%m-%d")
    in_event = day_str.isin(event_days)
    cond_return = (df["abs_return"] >= min_return) & (df["mz_return"] > 3.5)
    cond_whipsaw = (df["hl_range"] >= min_range) & (df["mz_range"] > 3.5) & (df["mz_volume"] > 2.0)
    return (in_event & (cond_return | cond_whipsaw)).astype(int).to_numpy()


def calibrate_threshold_by_rate(scores: np.ndarray, n_days: float, target_per_day: float) -> float:
    """Chọn ngưỡng cắt sao cho số báo động/ngày bằng target_per_day."""
    k = max(1, int(round(target_per_day * n_days)))
    if k >= len(scores):
        return float(np.min(scores))
    return float(np.partition(scores, -k)[-k])


def build_unsupervised_models(random_state: int = 42) -> dict[str, Pipeline]:
    """Khởi tạo các mô hình Học máy không giám sát chuẩn từ scikit-learn."""
    return {
        "IsolationForest": Pipeline([
            ("scaler", RobustScaler()),
            ("model", IsolationForest(
                n_estimators=200,
                max_samples=4096,
                contamination="auto",
                random_state=random_state,
                n_jobs=-1,
            )),
        ]),
        "EllipticEnvelope": Pipeline([
            ("scaler", RobustScaler()),
            ("model", EllipticEnvelope(
                support_fraction=0.9,
                contamination=0.01,
                random_state=random_state,
            )),
        ]),
    }


def anomaly_score_unsupervised(pipe: Pipeline, X: np.ndarray) -> np.ndarray:
    """Điểm bất thường càng lớn => càng bất thường (đảo dấu score_samples của sklearn)."""
    return -pipe.score_samples(X)


def build_supervised_models(random_state: int = 42) -> dict[str, object]:
    """Khởi tạo các mô hình Học máy có giám sát chuẩn từ scikit-learn."""
    return {
        "LogisticRegression": Pipeline([
            ("scaler", RobustScaler()),
            ("clf", LogisticRegression(max_iter=500, C=1.0, random_state=random_state)),
        ]),
        "RandomForest": RandomForestClassifier(
            n_estimators=200,
            max_depth=10,
            min_samples_leaf=10,
            class_weight="balanced_subsample",
            random_state=random_state,
            n_jobs=-1,
        ),
        "HistGradientBoosting": HistGradientBoostingClassifier(
            max_iter=200,
            max_depth=6,
            learning_rate=0.05,
            class_weight="balanced",
            random_state=random_state,
        ),
    }


def evaluate_binary_predictions(y_true: np.ndarray, y_score: np.ndarray, y_pred: np.ndarray) -> dict[str, float]:
    """Tính bộ chỉ số đánh giá chuẩn ML: Precision, Recall, F1-Score, ROC-AUC, PR-AUC."""
    prec = float(precision_score(y_true, y_pred, zero_division=0))
    rec = float(recall_score(y_true, y_pred, zero_division=0))
    f1 = float(f1_score(y_true, y_pred, zero_division=0))
    roc = float(roc_auc_score(y_true, y_score)) if len(np.unique(y_true)) > 1 else 0.0
    pr_auc = float(average_precision_score(y_true, y_score)) if len(np.unique(y_true)) > 1 else 0.0
    return {
        "precision": prec,
        "recall": rec,
        "f1": f1,
        "roc_auc": roc,
        "pr_auc": pr_auc,
    }


def resolve_model_choice(choice: int | str | None = None) -> int:
    """Chuẩn hóa tham số chọn mô hình: 1 = HistGradientBoosting (mặc định), 2 = RandomForest."""
    if choice is None:
        choice = settings().get("anomaly", {}).get("ml_model", 1)
    if isinstance(choice, str):
        s = choice.strip().lower()
        if s in ("2", "rf", "randomforest", "random_forest"):
            return 2
        return 1
    return 2 if int(choice) == 2 else 1


def save_production_model(model: object, threshold: float,
                          model_path: Path | None = None,
                          model_choice: int | str = 1) -> Path:
    """Lưu mô hình đã huấn luyện (1=HistGradientBoosting, 2=RandomForest) kèm ngưỡng quyết định ra đĩa."""
    cid = resolve_model_choice(model_choice)
    target = Path(model_path or MODEL_PATHS[cid])
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "model_choice": cid,
        "model_name": MODEL_NAMES[cid],
        "metric": MODEL_METRICS[cid],
        "feature_cols": FEATURE_COLS,
        "threshold": float(threshold),
        "estimator": model,
    }
    joblib.dump(payload, target, compress=3)
    load_production_model.cache_clear()
    return target


@functools.lru_cache(maxsize=4)
def load_production_model(model_path: str | None = None, model_choice: int = 1) -> dict[str, object] | None:
    """Nạp mô hình (1=HistGradientBoosting, 2=RandomForest) vào bộ nhớ (cache 1 lần)."""
    cid = resolve_model_choice(model_choice)
    target = Path(model_path) if model_path else MODEL_PATHS[cid]
    if not target.exists():
        return None
    return joblib.load(target)


def detect_ml(candles: pd.DataFrame, window: int = 1440,
              model_path: Path | None = None,
              model_choice: int | str | None = None) -> pd.DataFrame:
    """Chạy suy luận bằng mô hình ML đã lưu trên tập nến (Batch hoặc Streaming).

    model_choice:
      - 1 (mặc định): HistGradientBoosting (results/e7/hist_gbdt_model.joblib, metric='ml_gbdt')
      - 2           : RandomForest (results/e7/random_forest_model.joblib, metric='ml_rf')
    """
    empty = pd.DataFrame(columns=["symbol", "minute", "metric", "value", "mz_score"])
    cid = resolve_model_choice(model_choice)
    bundle = load_production_model(str(model_path) if model_path else None, cid)
    if bundle is None or candles.empty:
        return empty
    feat_df = valid_feature_frame(extract_ml_features(candles, window=window))
    if feat_df.empty:
        return empty
    clf = bundle["estimator"]
    th = float(bundle["threshold"])
    cols = list(bundle["feature_cols"])
    metric_name = str(bundle.get("metric", MODEL_METRICS[cid]))
    X = feat_df[cols].to_numpy()
    mat = feat_df["material_move"].to_numpy()
    probs = clf.predict_proba(X)[:, 1] * mat
    hit = probs >= th
    if not np.any(hit):
        return empty
    sub = feat_df.loc[hit]
    val = np.maximum(sub["abs_return"].to_numpy(), sub["hl_range"].to_numpy())
    return pd.DataFrame({
        "symbol": sub["symbol"].to_numpy(),
        "minute": sub["minute"].to_numpy(),
        "metric": metric_name,
        "value": val,
        "mz_score": probs[hit] * 10.0,
    })


def main() -> None:
    import argparse
    from bdi.reconcile.compare import load_candles

    ap = argparse.ArgumentParser(description="Chạy phát hiện bất thường bằng mô hình ML đã lưu (1 hoặc 2)")
    ap.add_argument("--ml-model", type=int, choices=[1, 2], default=1,
                    help="1 = HistGradientBoosting (mặc định), 2 = RandomForest")
    ap.add_argument("--source", default="batch")
    ap.add_argument("--start", default="2022-11-01")
    ap.add_argument("--end", default="2022-12-01")
    a = ap.parse_args()

    c = load_candles(a.source, start=a.start, end=a.end)
    if c.empty:
        c = load_candles("official", start=a.start, end=a.end)
    res = detect_ml(c, model_choice=a.ml_model)
    mname = MODEL_NAMES[a.ml_model]
    print(f"[ML Model {a.ml_model}: {mname}] Giai đoạn {a.start}..{a.end}: phát hiện {len(res)} cảnh báo ({MODEL_METRICS[a.ml_model]})")
    if not res.empty:
        per_day = res.groupby(res["minute"].dt.strftime("%Y-%m-%d")).size().sort_values(ascending=False)
        print("Top 5 ngày nhiều cảnh báo nhất:")
        print(per_day.head(5).to_string())


if __name__ == "__main__":
    main()

