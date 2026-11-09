"""C4 (linkage) + E7 (modified z-score)."""

from decimal import Decimal as D

import numpy as np
import pandas as pd

from bdi.anomaly.mzscore import rolling_mz
from bdi.linkage.fuzzy import STRATEGIES, link, prepare, score


def _data():
    rng = np.random.default_rng(0)
    n = 400
    ts = np.sort(rng.integers(0, 60_000_000, n)) + 1_786_795_200_000_000
    price = [D("100.00") + D(int(x)) / 100 for x in rng.integers(0, 50, n)]
    qty = [D(int(x)) / 1000 for x in rng.integers(1, 20, n)]
    batch = pd.DataFrame({"symbol": "BTCUSDT", "ts_us": ts, "price": price, "qty": qty, "agg_id": np.arange(n)})
    # stream = batch + độ trễ mạng 5..80ms, mất 5% bản tin
    keep = rng.random(n) > 0.05
    stream = batch[keep].assign(ts_us=lambda d: d["ts_us"] + rng.integers(5_000, 80_000, len(d)))
    return prepare(stream, "s"), prepare(batch, "b")


def test_linkage_scores_and_blocking_consistent():
    s, b = _data()
    res = {}
    for strat in STRATEGIES:
        m, n_cmp = link(s, b, 100_000, strat, price_bucket=0.1)
        res[strat] = (score(m, s, b), n_cmp)
    sc = res["second"][0]
    assert sc["recall"] > 0.95 and sc["precision"] > 0.95
    # cùng δ => cùng kết quả ghép; blocking chỉ đổi chi phí
    assert len({r[0]["tp"] for r in res.values()}) == 1
    assert res["none"][1] > res["second"][1] > res["second_price"][1]


def test_small_delta_hurts_recall():
    s, b = _data()
    low = score(link(s, b, 10_000)[0], s, b)["recall"]
    high = score(link(s, b, 100_000)[0], s, b)["recall"]
    assert low < high


def test_mzscore_flags_spike():
    x = pd.Series(np.r_[np.random.default_rng(1).normal(0, 1, 200), 25.0])
    mz = rolling_mz(x, 60)
    assert abs(mz.iloc[-1]) > 3.5
    assert mz.iloc[:60].isna().all()          # chưa đủ lịch sử => không chấm điểm
    assert (mz.iloc[150:200].abs() < 6).all()  # nhiễu chuẩn không bị gắn cờ mạnh


def test_ml_features_and_models_detect_multivariate_shock(tmp_path):
    from bdi.anomaly.ml import (
        FEATURE_COLS,
        anomaly_score_unsupervised,
        build_supervised_models,
        build_unsupervised_models,
        detect_ml,
        extract_ml_features,
        save_production_model,
        valid_feature_frame,
    )

    rng = np.random.default_rng(7)
    n = 300
    minutes = pd.date_range("2026-06-01", periods=n, freq="1min", tz="UTC")
    close = 100.0 * np.exp( np.cumsum(rng.normal(0, 0.0005, n)) )
    high = close * (1.0 + np.abs(rng.normal(0, 0.0006, n)))
    low = close * (1.0 - np.abs(rng.normal(0, 0.0006, n)))
    vol = rng.lognormal(2.0, 0.2, n)
    trades = rng.integers(20, 80, n)

    # Bơm một pha Flash Wick / Whipsaw ở phút cuối: râu nến vọt +5% và volume gấp 30 lần
    high[-1] = close[-1] * 1.05
    low[-1] = close[-1] * 0.95
    vol[-1] = vol.mean() * 30.0
    trades[-1] = 1500

    df = pd.DataFrame({
        "symbol": "BTCUSDT",
        "minute": minutes,
        "open": np.r_[close[0], close[:-1]],
        "high": high,
        "low": low,
        "close": close,
        "volume": vol,
        "trades": trades,
    })
    feat = valid_feature_frame(extract_ml_features(df, window=60))
    assert set(FEATURE_COLS).issubset(feat.columns)
    X = feat[FEATURE_COLS].to_numpy()
    iforest = build_unsupervised_models(random_state=0)["IsolationForest"]
    iforest.fit(X[:-1])
    scores = anomaly_score_unsupervised(iforest, X)
    # Điểm bất thường của phút Flash Wick cuối cùng phải nằm ở đỉnh phân phối
    assert scores[-1] > np.quantile(scores[:-1], 0.98)

    # Kiểm thử lưu và chạy suy luận cả mô hình 1 (HistGradientBoosting) và 2 (RandomForest)
    y = np.zeros(len(X), dtype=int)
    y[-1] = 1
    sup = build_supervised_models(random_state=0)

    gbdt = sup["HistGradientBoosting"]
    gbdt.fit(X, y)
    mpath1 = save_production_model(gbdt, 0.3, tmp_path / "gbdt.joblib", model_choice=1)
    found1 = detect_ml(df, window=60, model_path=mpath1, model_choice=1)
    assert not found1.empty
    assert (found1["metric"] == "ml_gbdt").all()
    assert minutes[-1] in set(found1["minute"])

    rf = sup["RandomForest"]
    rf.fit(X, y)
    mpath2 = save_production_model(rf, 0.2, tmp_path / "rf.joblib", model_choice=2)
    found2 = detect_ml(df, window=60, model_path=mpath2, model_choice=2)
    assert not found2.empty
    assert (found2["metric"] == "ml_rf").all()
    assert minutes[-1] in set(found2["minute"])

