"""E7-ML — So sánh phát hiện bất thường: Modified Z-Score vs. Machine Learning (Thuận chiều thời gian).

Thiết kế chống rò rỉ tương lai (Zero Time-Travel / Strict Chronological Split):
  - Tập Huấn luyện (Quá khứ: Tháng 05/2022 - 210.230 nến):
    + Mô hình Unsupervised (IsolationForest, EllipticEnvelope): chỉ học trên các ngày bình thường
      của tháng 05/2022 (không nhìn sự kiện, không nhìn dữ liệu tương lai).
    + Mô hình Supervised (LogisticRegression, RandomForest, HistGradientBoosting): học trên tháng 05/2022
      (sự kiện LUNA 09–12/05/2022 là nhãn cú sốc quá khứ).
    + Mọi việc chọn ngưỡng (threshold calibration) đều thực hiện 100% trên dữ liệu tháng 05/2022.
  - Tập Kiểm thử Độc lập ở Tương lai (Future Out-of-Sample Test - 865.430 nến):
    + Tương lai +6 tháng : Tháng 11/2022 (Sự kiện sập sàn FTX 08–09/11/2022, 203.030 nến).
    + Tương lai +4 năm   : Tháng 06–08/2026 (Giai đoạn thị trường yên ắng, 662.400 nến - đo báo động giả).

Đo lường đồng thời 2 nhóm tiêu chí:
  1. Chất lượng phát hiện (Precision, Recall, F1-Score, ROC-AUC, PR-AUC, % FTX, bắt Flash Wick, SNR).
  2. Tốc độ thực thi (Thời gian huấn luyện fit_time_s, Độ trễ suy luận ms/1k nến, Thông lượng dòng/giây).

Chạy:
    PYTHONPATH=src .venv/bin/python experiments/e7_ml_compare.py
"""

from __future__ import annotations

import time

import _common
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.metrics import f1_score, precision_recall_curve, roc_curve

from bdi.anomaly.ml import (
    FEATURE_COLS,
    UNSUP_FEATURE_COLS,
    anomaly_score_unsupervised,
    build_shock_labels,
    build_supervised_models,
    build_unsupervised_models,
    calibrate_threshold_by_rate,
    evaluate_binary_predictions,
    extract_ml_features,
    save_production_model,
    valid_feature_frame,
)
from bdi.reconcile.compare import load_candles

LUNA_DAYS = ["2022-05-09", "2022-05-10", "2022-05-11", "2022-05-12"]
FTX_DAYS = ["2022-11-08", "2022-11-09"]


def load_period(source: str, start: str, end: str) -> pd.DataFrame:
    c = load_candles(source, start=start, end=end)
    if c.empty:
        c = load_candles("official", start=start, end=end)
    if c.empty:
        raise SystemExit(f"Thiếu dữ liệu nến giai đoạn {start}..{end}")
    return valid_feature_frame(extract_ml_features(c, window=1440))


def event_stats(df: pd.DataFrame, pred: np.ndarray, event_days: list[str], n_days: float) -> dict[str, object]:
    """Thống kê số báo động theo ngày sự kiện, tỉ trọng tập trung và số pha rút râu nến bắt được."""
    hits = df.loc[pred == 1]
    per_day = hits.groupby(hits["minute"].dt.strftime("%Y-%m-%d")).size()
    rank = per_day.rank(ascending=False, method="min")
    ev_alerts = int(sum(per_day.get(d, 0) for d in event_days))
    total_alerts = int(len(hits))
    share_pct = (100.0 * ev_alerts / total_alerts) if total_alerts > 0 else 0.0
    ranks_str = ",".join(str(int(rank.get(d, 99))) for d in event_days)

    # Đếm các phút rút râu nến (Flash Wick / Whipsaw): biên độ đỉnh-đáy >= 0.8%, mz_range > 3.5,
    # mz_volume > 2.0 nhưng lợi suất đóng cửa |r| < 0.5% (Modified Z-Score đơn biến bỏ sót 100%)
    day_str = df["minute"].dt.strftime("%Y-%m-%d")
    is_whipsaw = (
        day_str.isin(event_days)
        & (df["hl_range"] >= 0.008)
        & (df["mz_range"] > 3.5)
        & (df["mz_volume"] > 2.0)
        & (df["abs_return"] < 0.005)
    )
    whipsaw_total = int(is_whipsaw.sum())
    whipsaw_caught = int((is_whipsaw & (pred == 1)).sum())
    return {
        "total_alerts": total_alerts,
        "alerts_per_day": total_alerts / n_days,
        "event_alerts": ev_alerts,
        "event_share_pct": share_pct,
        "event_ranks": ranks_str,
        "whipsaw_caught": whipsaw_caught,
        "whipsaw_total": whipsaw_total,
    }


def main() -> None:
    print("1. Đang đọc dữ liệu nến từ PostgreSQL và trích xuất 11 đặc trưng (cửa sổ trượt nhân quả W=1440)...")
    t0_feat = time.perf_counter()
    df_train_may22 = load_period("batch", "2022-05-01", "2022-06-01")
    df_test_ftx22 = load_period("batch", "2022-11-01", "2022-12-01")
    df_test_quiet26 = load_period("batch", "2026-06-01", "2026-09-01")
    feat_time_s = time.perf_counter() - t0_feat

    # Tập con các ngày bình thường trong tháng 05/2022 (ngoài 4 ngày LUNA) để fit Unsupervised & căn ngưỡng
    may_day_str = df_train_may22["minute"].dt.strftime("%Y-%m-%d")
    df_train_normal_may22 = df_train_may22[~may_day_str.isin(LUNA_DAYS)].copy()

    days_train_all = df_train_may22["minute"].dt.normalize().nunique()
    days_train_norm = df_train_normal_may22["minute"].dt.normalize().nunique()
    days_ftx = df_test_ftx22["minute"].dt.normalize().nunique()
    days_quiet26 = df_test_quiet26["minute"].dt.normalize().nunique()

    print(
        f"   - [QUÁ KHỨ - TRAIN] Tháng 05/2022 (LUNA): {len(df_train_may22):,} dòng ({days_train_all} ngày, "
        f"trong đó {days_train_norm} ngày ngoài sự kiện)\n"
        f"   - [TƯƠNG LAI +6 THÁNG - TEST 1] Tháng 11/2022 (FTX): {len(df_test_ftx22):,} dòng ({days_ftx} ngày)\n"
        f"   - [TƯƠNG LAI +4 NĂM   - TEST 2] Tháng 06–08/2026 (Quiet): {len(df_test_quiet26):,} dòng ({days_quiet26} ngày)\n"
        f"   - Thời gian trích xuất 11 đặc trưng cho {len(df_train_may22) + len(df_test_ftx22) + len(df_test_quiet26):,} dòng: {feat_time_s:.2f}s"
    )

    # Tập Test tổng hợp ở tương lai (2022-11 FTX + 2026-06..08 Quiet)
    df_future_test = pd.concat([df_test_ftx22, df_test_quiet26], ignore_index=True)
    y_train_may22 = build_shock_labels(df_train_may22, LUNA_DAYS)
    y_future_test = build_shock_labels(df_future_test, FTX_DAYS)

    # Ma trận cho Unsupervised (5 đặc trưng Z-score bất biến chế độ)
    X_unsup_train = df_train_normal_may22[UNSUP_FEATURE_COLS].to_numpy()
    X_unsup_ftx = df_test_ftx22[UNSUP_FEATURE_COLS].to_numpy()
    X_unsup_quiet = df_test_quiet26[UNSUP_FEATURE_COLS].to_numpy()
    X_unsup_future = df_future_test[UNSUP_FEATURE_COLS].to_numpy()

    # Ma trận cho Supervised (11 đặc trưng)
    X_sup_train = df_train_may22[FEATURE_COLS].to_numpy()
    X_sup_ftx = df_test_ftx22[FEATURE_COLS].to_numpy()
    X_sup_quiet = df_test_quiet26[FEATURE_COLS].to_numpy()
    X_sup_future = df_future_test[FEATURE_COLS].to_numpy()

    mat_train_norm = df_train_normal_may22["material_move"].to_numpy()
    mat_train_all = df_train_may22["material_move"].to_numpy()
    mat_ftx = df_test_ftx22["material_move"].to_numpy()
    mat_quiet = df_test_quiet26["material_move"].to_numpy()
    mat_future = df_future_test["material_move"].to_numpy()

    # Tỉ lệ báo động cơ sở của Baseline trên các ngày bình thường của tháng 05/2022
    base_pred_train_norm = (
        (df_train_normal_may22["mz_return"] > 3.5) & (df_train_normal_may22["abs_return"] >= 0.005)
    ).astype(int).to_numpy()
    target_norm_rate = float(base_pred_train_norm.sum()) / days_train_norm
    print(f"   - Mức cảnh báo nền trên ngày thường của 05/2022 (dùng để căn ngưỡng Unsupervised): {target_norm_rate:.2f} báo động/ngày")

    results_rows = []
    roc_curves = {}
    pr_curves = {}
    n_future = len(df_future_test)

    # --- 1. Baseline: Modified Z-Score (Đơn biến, không cần fit) ---
    t0 = time.perf_counter()
    base_score_future = (df_future_test["mz_return"] * (df_future_test["abs_return"] >= 0.005)).to_numpy()
    base_pred_future = ((df_future_test["mz_return"] > 3.5) & (df_future_test["abs_return"] >= 0.005)).astype(int).to_numpy()
    infer_s_base = time.perf_counter() - t0

    base_pred_ftx = ((df_test_ftx22["mz_return"] > 3.5) & (df_test_ftx22["abs_return"] >= 0.005)).astype(int).to_numpy()
    base_pred_quiet = ((df_test_quiet26["mz_return"] > 3.5) & (df_test_quiet26["abs_return"] >= 0.005)).astype(int).to_numpy()
    base_pred_luna = ((df_train_may22["mz_return"] > 3.5) & (df_train_may22["abs_return"] >= 0.005)).astype(int).to_numpy()

    m_base = evaluate_binary_predictions(y_future_test, base_score_future, base_pred_future)
    ev_ftx_base = event_stats(df_test_ftx22, base_pred_ftx, FTX_DAYS, days_ftx)
    ev_luna_base = event_stats(df_train_may22, base_pred_luna, LUNA_DAYS, days_train_all)
    q_rate_base = float(base_pred_quiet.sum()) / days_quiet26

    results_rows.append({
        "model": "Modified Z-Score (Baseline)",
        "category": "Statistical (Univariate)",
        **m_base,
        "quiet_test_per_day": q_rate_base,
        "ftx_event_alerts": ev_ftx_base["event_alerts"],
        "ftx_event_share_pct": ev_ftx_base["event_share_pct"],
        "ftx_ranks": ev_ftx_base["event_ranks"],
        "whipsaw_caught_ftx": ev_ftx_base["whipsaw_caught"],
        "whipsaw_total_ftx": ev_ftx_base["whipsaw_total"],
        "snr_ftx": (ev_ftx_base["event_alerts"] / 2.0) / max(q_rate_base, 0.05),
        "luna_train_share_pct": ev_luna_base["event_share_pct"],
        "fit_time_s": 0.0,
        "infer_ms_per_1k": (infer_s_base * 1000.0) / (n_future / 1000.0),
        "infer_rows_per_s": n_future / max(infer_s_base, 1e-6),
    })
    roc_curves["Modified Z-Score"] = roc_curve(y_future_test, base_score_future)
    pr_curves["Modified Z-Score"] = precision_recall_curve(y_future_test, base_score_future)

    # --- 2. Nhóm Unsupervised ML (Fit trên ngày thường của 2022-05, Test trên 2022-11 FTX + 2026-06..08) ---
    print("2. Đang huấn luyện Unsupervised ML trên ngày thường tháng 05/2022...")
    unsup_models = build_unsupervised_models(random_state=42)
    unsup_scores_cache = {}
    for name, pipe in unsup_models.items():
        t_fit0 = time.perf_counter()
        pipe.fit(X_unsup_train)
        fit_s = time.perf_counter() - t_fit0

        sc_train_norm = anomaly_score_unsupervised(pipe, X_unsup_train) * mat_train_norm
        th = calibrate_threshold_by_rate(sc_train_norm, days_train_norm, target_norm_rate * 0.90)

        t_inf0 = time.perf_counter()
        sc_future = anomaly_score_unsupervised(pipe, X_unsup_future) * mat_future
        pred_future = (sc_future >= th).astype(int)
        infer_s = time.perf_counter() - t_inf0

        sc_ftx = sc_future[:len(df_test_ftx22)]
        sc_quiet = sc_future[len(df_test_ftx22):]
        sc_luna = anomaly_score_unsupervised(pipe, df_train_may22[UNSUP_FEATURE_COLS].to_numpy()) * mat_train_all
        unsup_scores_cache[name] = (sc_train_norm, sc_quiet, sc_ftx, sc_luna)

        pred_ftx = (sc_ftx >= th).astype(int)
        pred_quiet = (sc_quiet >= th).astype(int)
        pred_luna = (sc_luna >= th).astype(int)

        m_unsup = evaluate_binary_predictions(y_future_test, sc_future, pred_future)
        ev_ftx = event_stats(df_test_ftx22, pred_ftx, FTX_DAYS, days_ftx)
        ev_luna = event_stats(df_train_may22, pred_luna, LUNA_DAYS, days_train_all)
        q_rate = float(pred_quiet.sum()) / days_quiet26

        results_rows.append({
            "model": name,
            "category": "Unsupervised ML",
            **m_unsup,
            "quiet_test_per_day": q_rate,
            "ftx_event_alerts": ev_ftx["event_alerts"],
            "ftx_event_share_pct": ev_ftx["event_share_pct"],
            "ftx_ranks": ev_ftx["event_ranks"],
            "whipsaw_caught_ftx": ev_ftx["whipsaw_caught"],
            "whipsaw_total_ftx": ev_ftx["whipsaw_total"],
            "snr_ftx": (ev_ftx["event_alerts"] / 2.0) / max(q_rate, 0.05),
            "luna_train_share_pct": ev_luna["event_share_pct"],
            "fit_time_s": fit_s,
            "infer_ms_per_1k": (infer_s * 1000.0) / (n_future / 1000.0),
            "infer_rows_per_s": n_future / max(infer_s, 1e-6),
        })
        roc_curves[name] = roc_curve(y_future_test, sc_future)
        pr_curves[name] = precision_recall_curve(y_future_test, sc_future)

    # --- 3. Nhóm Supervised ML (Fit trên 2022-05 LUNA, Test trên tương lai 2022-11 FTX + 2026-06..08) ---
    print("3. Đang huấn luyện Supervised ML trên quá khứ (05/2022 LUNA) và kiểm thử trên tương lai (11/2022 FTX + 2026)...")
    sup_models = build_supervised_models(random_state=42)
    rf_importance = None
    for name, clf in sup_models.items():
        t_fit0 = time.perf_counter()
        clf.fit(X_sup_train, y_train_may22)
        fit_s = time.perf_counter() - t_fit0

        prob_train = clf.predict_proba(X_sup_train)[:, 1] * mat_train_all
        # Chọn ngưỡng tối ưu F1 hoàn toàn trên tập Train quá khứ (2022-05)
        ths = np.linspace(0.20, 0.90, 36)
        best_th, best_f1 = 0.5, -1.0
        for cand_th in ths:
            f1_tr = f1_score(y_train_may22, (prob_train >= cand_th).astype(int), zero_division=0)
            if f1_tr > best_f1:
                best_f1 = f1_tr
                best_th = float(cand_th)

        t_inf0 = time.perf_counter()
        prob_future = clf.predict_proba(X_sup_future)[:, 1] * mat_future
        pred_future = (prob_future >= best_th).astype(int)
        infer_s = time.perf_counter() - t_inf0

        prob_ftx = prob_future[:len(df_test_ftx22)]
        prob_quiet = prob_future[len(df_test_ftx22):]
        pred_ftx = (prob_ftx >= best_th).astype(int)
        pred_quiet = (prob_quiet >= best_th).astype(int)
        pred_luna = (prob_train >= best_th).astype(int)

        m_sup = evaluate_binary_predictions(y_future_test, prob_future, pred_future)
        ev_ftx = event_stats(df_test_ftx22, pred_ftx, FTX_DAYS, days_ftx)
        ev_luna = event_stats(df_train_may22, pred_luna, LUNA_DAYS, days_train_all)
        q_rate = float(pred_quiet.sum()) / days_quiet26

        results_rows.append({
            "model": name,
            "category": "Supervised ML (Past->Future)",
            **m_sup,
            "quiet_test_per_day": q_rate,
            "ftx_event_alerts": ev_ftx["event_alerts"],
            "ftx_event_share_pct": ev_ftx["event_share_pct"],
            "ftx_ranks": ev_ftx["event_ranks"],
            "whipsaw_caught_ftx": ev_ftx["whipsaw_caught"],
            "whipsaw_total_ftx": ev_ftx["whipsaw_total"],
            "snr_ftx": (ev_ftx["event_alerts"] / 2.0) / max(q_rate, 0.05),
            "luna_train_share_pct": ev_luna["event_share_pct"],
            "fit_time_s": fit_s,
            "infer_ms_per_1k": (infer_s * 1000.0) / (n_future / 1000.0),
            "infer_rows_per_s": n_future / max(infer_s, 1e-6),
        })
        roc_curves[name] = roc_curve(y_future_test, prob_future)
        pr_curves[name] = precision_recall_curve(y_future_test, prob_future)

        if name == "RandomForest":
            rf_importance = pd.DataFrame({
                "feature": FEATURE_COLS,
                "importance": clf.feature_importances_,
            }).sort_values("importance", ascending=False)
            saved_rf = save_production_model(clf, best_th, model_choice=2)
            print(f"   -> Đã lưu mô hình #2 RandomForest (ngưỡng={best_th:.3f}) tại: {saved_rf}")
        elif name == "HistGradientBoosting":
            saved_path = save_production_model(clf, best_th, model_choice=1)
            print(f"   -> Đã lưu mô hình #1 HistGradientBoosting (ngưỡng={best_th:.3f}) tại: {saved_path}")

    out_dir = _common.out_dir("e7")
    df_res = pd.DataFrame(results_rows)
    df_res.to_csv(out_dir / "ml_comparison.csv", index=False)
    if rf_importance is not None:
        rf_importance.to_csv(out_dir / "ml_feature_importance.csv", index=False)

    # --- 4. Quét đường cong ngân sách báo động (Alert Budget Curve) thuận chiều thời gian ---
    budget_rates = [2.0, 5.0, 10.0, 15.0, 20.0, 30.0]
    budget_rows = []
    base_sc_tnorm = (df_train_normal_may22["mz_return"] * (df_train_normal_may22["abs_return"] >= 0.005)).to_numpy()
    base_sc_quiet = (df_test_quiet26["mz_return"] * (df_test_quiet26["abs_return"] >= 0.005)).to_numpy()
    base_sc_ftx = (df_test_ftx22["mz_return"] * (df_test_ftx22["abs_return"] >= 0.005)).to_numpy()

    for b_rate in budget_rates:
        th_b = max(calibrate_threshold_by_rate(base_sc_tnorm, days_train_norm, b_rate), 1e-6)
        budget_rows.append({
            "model": "Modified Z-Score",
            "train_budget_per_day": b_rate,
            "future_quiet_per_day": float((base_sc_quiet >= th_b).sum()) / days_quiet26,
            "ftx_event_alerts": int(df_test_ftx22.loc[base_sc_ftx >= th_b, "minute"].dt.strftime("%Y-%m-%d").isin(FTX_DAYS).sum()),
        })
        for name, (sc_tn, sc_qt, sc_ftx, _) in unsup_scores_cache.items():
            th_u = max(calibrate_threshold_by_rate(sc_tn, days_train_norm, b_rate), 1e-6)
            budget_rows.append({
                "model": name,
                "train_budget_per_day": b_rate,
                "future_quiet_per_day": float((sc_qt >= th_u).sum()) / days_quiet26,
                "ftx_event_alerts": int(df_test_ftx22.loc[sc_ftx >= th_u, "minute"].dt.strftime("%Y-%m-%d").isin(FTX_DAYS).sum()),
            })
    pd.DataFrame(budget_rows).to_csv(out_dir / "ml_budget_curve.csv", index=False)

    print("\n=== BẢNG 1: CHẤT LƯỢNG PHÁT HIỆN TRÊN TẬP TEST TƯƠNG LAI (FTX 2022-11 & QUIET 2026-06..08) ===")
    cols_q = [
        "model", "category", "precision", "recall", "f1", "roc_auc", "pr_auc",
        "quiet_test_per_day", "ftx_event_alerts", "ftx_event_share_pct",
        "whipsaw_caught_ftx", "snr_ftx",
    ]
    print(df_res[cols_q].round(4).to_string(index=False))

    print("\n=== BẢNG 2: ĐÁNH GIÁ TỐC ĐỘ THỰC THI (HUẤN LUYỆN & SUY LUẬN TRỰC TUYẾN) ===")
    cols_s = ["model", "fit_time_s", "infer_ms_per_1k", "infer_rows_per_s"]
    print(df_res[cols_s].round(3).to_string(index=False))

    if rf_importance is not None:
        print("\n=== ĐỘ QUAN TRỌNG ĐẶC TRƯNG (RANDOM FOREST TRÊN TẬP TRAIN 2022-05) ===")
        print(rf_importance.round(4).to_string(index=False))

    # --- 5. Vẽ biểu đồ 1: Chất lượng phát hiện trên tương lai (FTX 2022-11 + Quiet 2026) ---
    fig, axes = plt.subplots(1, 2, figsize=(12.5, 4.6))
    models_short = [
        m.replace(" (Baseline)", "")
        .replace("LogisticRegression", "Logistic Reg.")
        .replace("HistGradientBoosting", "HistGradBoost\n(XGBoost/LGBM)")
        .replace("IsolationForest", "Isolation Forest")
        .replace("EllipticEnvelope", "Elliptic Env.\n(Mahalanobis)")
        .replace("RandomForest", "Random Forest")
        for m in df_res["model"]
    ]
    x = np.arange(len(models_short))
    w = 0.25

    ax0 = axes[0]
    ax0.bar(x - w, df_res["precision"], width=w, label="Precision", color=_common.SERIES[0])
    ax0.bar(x, df_res["recall"], width=w, label="Recall", color=_common.SERIES[1])
    ax0.bar(x + w, df_res["f1"], width=w, label="F1-Score", color=_common.SERIES[2])
    ax0.set_xticks(x)
    ax0.set_xticklabels(models_short, rotation=18, ha="right", fontsize=8.5)
    ax0.set_ylim(0, 1.12)
    ax0.set_ylabel("Điểm số (0..1)")
    ax0.set_title("A. Đánh giá Tương lai (Train: 05/2022 LUNA -> Test: 11/2022 FTX + 2026)")
    ax0.legend(frameon=False, loc="upper left", ncol=3, fontsize=8.5)
    for i, f1_v in enumerate(df_res["f1"]):
        ax0.text(x[i] + w, f1_v + 0.02, f"{f1_v:.2f}", ha="center", va="bottom", fontsize=7.5, fontweight="bold")

    ax1 = axes[1]
    whip_pct = 100.0 * df_res["whipsaw_caught_ftx"] / df_res["whipsaw_total_ftx"].clip(lower=1)
    bars1 = ax1.bar(x - w / 2, df_res["ftx_event_share_pct"], width=w * 1.1,
                    label="Tỉ trọng cảnh báo đúng 2 ngày FTX (%)", color=_common.SERIES[0])
    bars2 = ax1.bar(x + w / 2, whip_pct, width=w * 1.1,
                    label="Tỉ lệ bắt pha rút râu nến / Flash Wick FTX (%)", color=_common.SERIES[1])
    ax1.set_xticks(x)
    ax1.set_xticklabels(models_short, rotation=18, ha="right", fontsize=8.5)
    ax1.set_ylim(0, 112)
    ax1.set_ylabel("Tỉ lệ (%)")
    ax1.set_title("B. Độ tập trung sự kiện FTX & Khả năng bắt Flash Wick")
    ax1.legend(frameon=False, loc="upper left", fontsize=8.5)
    for b in list(bars1) + list(bars2):
        h = b.get_height()
        ax1.text(b.get_x() + b.get_width() / 2, h + 1.2, f"{h:.1f}%", ha="center", va="bottom", fontsize=7.5)

    fig.tight_layout()
    _common.save(fig, "e7", "ml_comparison_metrics")

    # --- 6. Vẽ biểu đồ 2: Đường cong PR + Độ quan trọng đặc trưng + Tốc độ suy luận ---
    fig2, axes2 = plt.subplots(1, 2, figsize=(12, 4.4))
    ax_pr = axes2[0]
    palette = [_common.SERIES[0], _common.SERIES[1], _common.SERIES[2], _common.SERIES[3], "#8c564b", "#e377c2"]
    for idx, (m_name, (prec_c, rec_c, _)) in enumerate(pr_curves.items()):
        ap_val = float(df_res.loc[df_res["model"].str.startswith(m_name), "pr_auc"].iloc[0])
        lw = 2.2 if m_name in ("RandomForest", "HistGradientBoosting", "IsolationForest") else 1.4
        ls = "--" if m_name == "Modified Z-Score" else "-"
        ax_pr.plot(rec_c, prec_c, label=f"{m_name} (AP={ap_val:.3f})",
                   color=palette[idx % len(palette)], linewidth=lw, linestyle=ls)
    ax_pr.set_xlabel("Recall (Độ phủ cú sốc FTX thực tế)")
    ax_pr.set_ylabel("Precision (Độ chính xác báo động)")
    ax_pr.set_title("A. Đường cong Precision–Recall (Test Tương lai: FTX + 2026)")
    ax_pr.set_xlim(0, 1.02)
    ax_pr.set_ylim(0, 1.05)
    ax_pr.legend(frameon=False, loc="lower left", fontsize=8)

    ax_fi = axes2[1]
    if rf_importance is not None:
        fi_sorted = rf_importance.sort_values("importance", ascending=True)
        ax_fi.barh(fi_sorted["feature"], fi_sorted["importance"], color=_common.SERIES[0], height=0.65)
        for i, (feat, imp) in enumerate(zip(fi_sorted["feature"], fi_sorted["importance"])):
            ax_fi.text(imp + 0.004, i, f"{imp * 100:.1f}%", va="center", fontsize=8)
        ax_fi.set_xlim(0, float(fi_sorted["importance"].max()) * 1.18)
        ax_fi.set_xlabel("Mức đóng góp tương đối (Gini Importance)")
        ax_fi.set_title("B. Độ quan trọng 11 đặc trưng (Train: 05/2022)")

    fig2.tight_layout()
    _common.save(fig2, "e7", "ml_pr_feature_importance")


if __name__ == "__main__":
    main()
