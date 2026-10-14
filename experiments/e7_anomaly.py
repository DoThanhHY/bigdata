"""E7 — Phát hiện bất thường (modified z-score) + đánh giá theo sự kiện FTX.

Tiền điều kiện: nến batch (hoặc official) của 2022-11 và 2026-06..08 đã có trong Postgres.

    python experiments/e7_anomaly.py --source batch
    # kiểm lại trên sự kiện khác, GIỮ NGUYÊN tham số đã chọn (tránh khớp theo dữ liệu):
    python experiments/e7_anomaly.py --month 2022-05 --event-days 2022-05-09 2022-05-10 2022-05-11 \
        --windows 1440 --thresholds 3.5 --floors 0.005 --tag luna

Quét (cửa sổ W, ngưỡng |M|, sàn |log-return|) và đo, trên báo động log-return:
  - ftx_8 / ftx_9      : số báo động ngày 8 và 9/11/2022
  - ftx_rank_8/9       : thứ hạng của 2 ngày đó trong 30 ngày tháng 11/2022 (1 = nhiều nhất)
  - per_day_nov22      : báo động/ngày trung bình tháng 11/2022 (5 cặp)
  - per_day_quiet26    : báo động/ngày giai đoạn yên 6–8/2026 (proxy báo động giả)
Không có nhãn chuẩn => precision/recall chỉ mang tính tương đối (ghi rõ trong báo cáo).
"""

from __future__ import annotations

import argparse

import _common
import matplotlib.pyplot as plt
import pandas as pd

from bdi.anomaly.mzscore import features, rolling_mz
from bdi.reconcile.compare import load_candles

FTX_DAYS = ["2022-11-08", "2022-11-09"]
QUIET = ("2026-06-01", "2026-09-01")


def scores(c: pd.DataFrame, window: int) -> pd.DataFrame:
    f = features(c)
    f["mz"] = f.groupby("symbol", group_keys=False)["log_return"].apply(lambda s: rolling_mz(s, window))
    f["mz_vol"] = f.groupby("symbol", group_keys=False)["log_volume"].apply(lambda s: rolling_mz(s, window))
    return f


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default="batch")
    ap.add_argument("--windows", nargs="+", type=int, default=[60, 1440])
    ap.add_argument("--thresholds", nargs="+", type=float, default=[3.5, 7.0, 10.0])
    ap.add_argument("--floors", nargs="+", type=float, default=[0.0, 0.005, 0.01])
    ap.add_argument("--month", default="2022-11", help="tháng chứa sự kiện, YYYY-MM")
    ap.add_argument("--event-days", nargs="+", default=FTX_DAYS, help="các ngày sự kiện YYYY-MM-DD")
    ap.add_argument("--tag", default="", help="hậu tố tên file kết quả, VD luna")
    a = ap.parse_args()
    event_days = a.event_days
    m0 = pd.Timestamp(a.month + "-01", tz="UTC")
    m1 = m0 + pd.offsets.MonthBegin(1)
    n_days = (m1 - m0).days
    suf = f"_{a.tag}" if a.tag else ""

    c22 = load_candles(a.source, start=m0.isoformat(), end=m1.isoformat())
    if c22.empty:
        raise SystemExit(f"Không có nến {a.source} cho {a.month}: tải + dựng batch tháng đó trước")
    c26 = load_candles(a.source, start=QUIET[0], end=QUIET[1])
    quiet_days = c26["minute"].dt.normalize().nunique() or 1
    rows, best = [], None
    for w in a.windows:
        f22, f26 = scores(c22, w), scores(c26, w)
        for th in a.thresholds:
            for floor in a.floors:
                h22 = f22[(f22["mz"].abs() > th) & (f22["log_return"].abs() >= floor)]
                h26 = f26[(f26["mz"].abs() > th) & (f26["log_return"].abs() >= floor)]
                per = h22.groupby(h22["minute"].dt.strftime("%Y-%m-%d")).size()
                rank = per.rank(ascending=False, method="min")
                row = {"window": w, "threshold": th, "min_abs_return": floor}
                if event_days == FTX_DAYS:
                    row.update({"ftx_8": int(per.get(FTX_DAYS[0], 0)), "ftx_9": int(per.get(FTX_DAYS[1], 0)),
                                "ftx_rank_8": int(rank.get(FTX_DAYS[0], 99)), "ftx_rank_9": int(rank.get(FTX_DAYS[1], 99))})
                else:
                    for d in event_days:
                        row[f"alerts_{d}"] = int(per.get(d, 0))
                        row[f"rank_{d}"] = int(rank.get(d, 99))
                rows.append({
                    **row,
                    "per_day_nov22": len(h22) / n_days, "per_day_quiet26": len(h26) / quiet_days,
                    "volume_alerts_per_day_quiet26": int((f26["mz_vol"].abs() > th).sum()) / quiet_days,
                })
                if (w, th, floor) == (1440, 3.5, 0.005):
                    best = h22
    df = pd.DataFrame(rows)
    df.to_csv(_common.out_dir("e7") / f"sweep{suf}.csv", index=False)
    if event_days != FTX_DAYS:
        print(df.to_string(index=False))
        return  # hình FTX chỉ vẽ cho tháng 11/2022
    print(df.round({"per_day_nov22": 2, "per_day_quiet26": 2, "volume_alerts_per_day_quiet26": 2}).to_string(index=False))

    # Hình: giá BTC 11/2022 + điểm bất thường log-return (cấu hình chọn: W=1440, |M|>3.5, sàn 0,5%)
    btc = c22[c22["symbol"] == "BTCUSDT"].sort_values("minute")
    fig, ax = plt.subplots(figsize=(10, 4))
    for d in FTX_DAYS:
        ax.axvspan(pd.Timestamp(d, tz="UTC"), pd.Timestamp(d, tz="UTC") + pd.Timedelta(days=1),
                   color=_common.GRID, alpha=0.9, lw=0)
    ax.plot(btc["minute"], btc["close"].astype(float), color=_common.SERIES[0], linewidth=1, label="BTCUSDT close")
    if best is not None:
        k = best[best["symbol"] == "BTCUSDT"]
        px = btc.set_index("minute")["close"].astype(float).reindex(k["minute"])
        ax.scatter(k["minute"], px, s=12, color=_common.SERIES[1], zorder=3,
                   label="bất thường (W=1 ngày, |M|>3,5, |r|≥0,5%)")
    ax.set_xlim(pd.Timestamp("2022-11-01", tz="UTC"), pd.Timestamp("2022-12-01", tz="UTC"))
    ax.set_title("E7 · BTCUSDT 11/2022 — vùng tô: 8–9/11 (FTX)")
    ax.set_ylabel("giá (USDT)")
    ax.legend(frameon=False, loc="upper right")
    _common.save(fig, "e7", "ftx_btc")

    # Hình: số báo động mỗi ngày tháng 11/2022 (cấu hình chọn), 5 cặp cộng dồn
    if best is not None:
        per = best.groupby(best["minute"].dt.floor("D")).size()
        fig, ax = plt.subplots(figsize=(10, 3.2))
        colors = [_common.SERIES[1] if d.strftime("%Y-%m-%d") in FTX_DAYS else _common.SERIES[0] for d in per.index]
        ax.bar(per.index, per.values, width=0.8, color=colors)
        ax.set_xlim(pd.Timestamp("2022-10-31 12:00", tz="UTC"), pd.Timestamp("2022-11-30 12:00", tz="UTC"))
        ax.set_ylabel("báo động / ngày (5 cặp)")
        ax.set_title("E7 · Báo động log-return theo ngày, 11/2022 — cam: 8–9/11 (FTX)")
        _common.save(fig, "e7", "alerts_per_day_nov22")


if __name__ == "__main__":
    main()
