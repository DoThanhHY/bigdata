# E1 · Ablation schema drift

**Câu hỏi:** Nếu bỏ qua schema drift thì dữ liệu hỏng đến mức nào?
**Chứng minh cho:** C1 · **Phụ trách:** Mảng 1

## Thiết kế

| | Naive | Drift-aware |
|---|---|---|
| Hàm | `to_micros_naive_col`: luôn nhân 1000 | `to_micros_col`: suy đơn vị theo độ lớn |
| 2022 (ms) | đúng | đúng |
| 2026 (us) | **sai 1000 lần** (năm ~58 000) | đúng |

## Chạy

```bash
# cần: aggTrades + klines của ít nhất 1 tháng 2022 và 1 tháng 2026
python experiments/e1_drift_ablation.py
python experiments/e1_drift_ablation.py --symbols BTCUSDT --periods 2022-11 2026-08   # bản nhanh
```

## Đầu ra

`results/e1/ablation.csv`, mỗi dòng ứng với một bộ symbol × kỳ × phiên bản:

| Cột | Ý nghĩa |
|---|---|
| `pct_bad_ts` | % bản ghi có mốc thời gian vô lý (> năm 3000 hoặc < 2017) |
| `pct_candle_ok` | % nến official có nến dựng tương ứng khớp tuyệt đối OHLCV + trades |

**Kết quả mong đợi:** naive cho `pct_bad_ts ≈ 100%` và `pct_candle_ok ≈ 0%` ở 2026, đúng 100% ở 2022. Drift-aware cho `pct_bad_ts = 0` ở mọi kỳ.

## Kết quả (13/10/2026, SOLUSDT + XRPUSDT)

| Cặp | Kỳ | Phiên bản | Số bản ghi | % mốc vô lý | % nến đúng |
|---|---|---|---|---|---|
| SOLUSDT | 2022-11 | naive | 7 077 107 | 0% | 100% |
| SOLUSDT | 2022-11 | drift-aware | 7 077 107 | 0% | 100% |
| SOLUSDT | 2026-08 | naive | 4 977 119 | **100%** | **0%** |
| SOLUSDT | 2026-08 | drift-aware | 4 977 119 | 0% | 100% |
| XRPUSDT | 2022-11 | naive | 4 776 769 | 0% | 100% |
| XRPUSDT | 2022-11 | drift-aware | 4 776 769 | 0% | 100% |
| XRPUSDT | 2026-08 | naive | 6 016 728 | **100%** | **0%** |
| XRPUSDT | 2026-08 | drift-aware | 6 016 728 | 0% | 100% |

Đúng như kỳ vọng: naive hỏng toàn bộ 2026 và vẫn đúng ở 2022; drift-aware đúng 100% ở cả hai.

## Checklist

- [x] Chạy SOL + XRP × {2022-11, 2026-08}
- [ ] Chạy với cả 5 cặp × 4 tháng (khoảng 1–2 giờ: `python experiments/e1_drift_ablation.py`)
- [ ] Bảng ablation 2022 vs 2026 đưa vào báo cáo
- [x] `pytest tests/test_timeutil.py` xanh (contract test 3 nguồn × 2 giai đoạn), chụp output đưa vào phụ lục
- [ ] Nêu trong báo cáo: drift còn xảy ra *giữa các nguồn* (WS dùng ms, CSV 2026 dùng us), và cùng một hàm xử lý được cả hai
