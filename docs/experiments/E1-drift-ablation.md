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
# cần: aggTrades + klines của 5 cặp × 4 tháng (bdi.ingest.download)
python experiments/e1_drift_ablation.py --symbols BTCUSDT ETHUSDT BNBUSDT SOLUSDT XRPUSDT \
    --periods 2022-11 2026-06 2026-07 2026-08                                      # bản đầy đủ: 40 dòng
python experiments/e1_drift_ablation.py --symbols SOLUSDT --periods 2026-08        # chạy thử
python experiments/e1_drift_ablation.py --plot-only                                # chỉ vẽ lại hình từ CSV
python experiments/e1_check.py                                                       # kiểm tiêu chí đạt
```

**Luôn truyền `--periods`.** Thiếu tham số này, script lấy mọi file aggTrades trong manifest, kể cả file theo ngày (13/10, ngày D của C5) trên máy dữ liệu, nên bảng sẽ dư dòng. Script ghi đè `results/e1/ablation.csv` mỗi lần chạy.

## Đầu ra

`results/e1/ablation.csv`, mỗi dòng ứng với một bộ symbol × kỳ × phiên bản:

| Cột | Ý nghĩa |
|---|---|
| `pct_bad_ts` | % bản ghi có mốc thời gian vô lý (> năm 3000, < 2017, hoặc không suy được đơn vị) |
| `pct_candle_ok` | % nến official có nến dựng tương ứng khớp tuyệt đối OHLCV + trades |
| `raw_digits` | Số chữ số của timestamp thô trong file (13 = ms, 16 = µs), dùng làm hồ sơ nguồn |
| `first_ts`, `last_ts` | Mốc đầu và mốc cuối của file theo bản drift-aware (bản naive để trống) |

`results/e1/ablation.png`: cột nhóm theo kỳ, naive so với drift-aware, gộp mọi cặp có trong CSV.

**Kết quả mong đợi:** naive cho `pct_bad_ts ≈ 100%` và `pct_candle_ok ≈ 0%` ở 2026, đúng 100% ở 2022. Drift-aware cho `pct_bad_ts = 0` ở mọi kỳ.

## Kết quả (25/10/2026, 5 cặp × 4 tháng)

Chạy trên máy Windows 11 (i5-12400F, 32 GB, Spark `local[*]`), khác máy chạy lần đầu ngày 13/10.

| Kỳ | Đơn vị | Phiên bản | Số cặp | Số bản ghi | % mốc vô lý | % nến đúng |
|---|---|---|---|---|---|---|
| 2022-11 | ms | naive | 5 | 214 295 686 | 0% | 100% |
| 2022-11 | ms | drift-aware | 5 | 214 295 686 | 0% | 100% |
| 2026-06 | µs | naive | 5 | 84 092 148 | **100%** | **0%** |
| 2026-06 | µs | drift-aware | 5 | 84 092 148 | 0% | 100% |
| 2026-07 | µs | naive | 5 | 57 411 961 | **100%** | **0%** |
| 2026-07 | µs | drift-aware | 5 | 57 411 961 | 0% | 100% |
| 2026-08 | µs | naive | 5 | 62 357 036 | **100%** | **0%** |
| 2026-08 | µs | drift-aware | 5 | 62 357 036 | 0% | 100% |

- Tổng 418 156 831 bản ghi, khớp số dòng aggTrades ghi trong `02-roadmap.md`. Tổng 878 400 nến official, khớp E2.
- Đúng như kỳ vọng: bản naive hỏng toàn bộ 15/15 tổ hợp cặp × tháng của 2026 và vẫn đúng ở 2022; bản drift-aware đúng 100% ở cả 40 dòng.
- **Tái lập được:** 8 dòng SOL, XRP × {2022-11, 2026-08} trùng từng số với lần chạy 13/10 trên máy khác (`experiments/e1_check.py`).
- **Hồ sơ nguồn:** cả 5 file 2022-11 có timestamp 13 chữ số (ms), cả 15 file 2026 có 16 chữ số (µs). Mốc đầu và cuối của mọi file nằm trọn trong tháng của file.
- Hình: `results/e1/ablation.png`.

<details>
<summary>Bảng đầy đủ 40 dòng</summary>

| Cặp | Kỳ | Phiên bản | Số bản ghi | Chữ số timestamp | % mốc vô lý | % nến đúng |
|---|---|---|---|---|---|---|
| BNBUSDT | 2022-11 | naive | 7 977 084 | 13 | 0% | 100% |
| BNBUSDT | 2022-11 | drift-aware | 7 977 084 | 13 | 0% | 100% |
| BNBUSDT | 2026-06 | naive | 10 058 771 | 16 | **100%** | **0%** |
| BNBUSDT | 2026-06 | drift-aware | 10 058 771 | 16 | 0% | 100% |
| BNBUSDT | 2026-07 | naive | 5 772 626 | 16 | **100%** | **0%** |
| BNBUSDT | 2026-07 | drift-aware | 5 772 626 | 16 | 0% | 100% |
| BNBUSDT | 2026-08 | naive | 6 961 577 | 16 | **100%** | **0%** |
| BNBUSDT | 2026-08 | drift-aware | 6 961 577 | 16 | 0% | 100% |
| BTCUSDT | 2022-11 | naive | 171 900 348 | 13 | 0% | 100% |
| BTCUSDT | 2022-11 | drift-aware | 171 900 348 | 13 | 0% | 100% |
| BTCUSDT | 2026-06 | naive | 34 074 359 | 16 | **100%** | **0%** |
| BTCUSDT | 2026-06 | drift-aware | 34 074 359 | 16 | 0% | 100% |
| BTCUSDT | 2026-07 | naive | 24 186 730 | 16 | **100%** | **0%** |
| BTCUSDT | 2026-07 | drift-aware | 24 186 730 | 16 | 0% | 100% |
| BTCUSDT | 2026-08 | naive | 23 939 440 | 16 | **100%** | **0%** |
| BTCUSDT | 2026-08 | drift-aware | 23 939 440 | 16 | 0% | 100% |
| ETHUSDT | 2022-11 | naive | 22 564 378 | 13 | 0% | 100% |
| ETHUSDT | 2022-11 | drift-aware | 22 564 378 | 13 | 0% | 100% |
| ETHUSDT | 2026-06 | naive | 30 664 042 | 16 | **100%** | **0%** |
| ETHUSDT | 2026-06 | drift-aware | 30 664 042 | 16 | 0% | 100% |
| ETHUSDT | 2026-07 | naive | 21 128 759 | 16 | **100%** | **0%** |
| ETHUSDT | 2026-07 | drift-aware | 21 128 759 | 16 | 0% | 100% |
| ETHUSDT | 2026-08 | naive | 20 462 172 | 16 | **100%** | **0%** |
| ETHUSDT | 2026-08 | drift-aware | 20 462 172 | 16 | 0% | 100% |
| SOLUSDT | 2022-11 | naive | 7 077 107 | 13 | 0% | 100% |
| SOLUSDT | 2022-11 | drift-aware | 7 077 107 | 13 | 0% | 100% |
| SOLUSDT | 2026-06 | naive | 5 116 327 | 16 | **100%** | **0%** |
| SOLUSDT | 2026-06 | drift-aware | 5 116 327 | 16 | 0% | 100% |
| SOLUSDT | 2026-07 | naive | 3 679 293 | 16 | **100%** | **0%** |
| SOLUSDT | 2026-07 | drift-aware | 3 679 293 | 16 | 0% | 100% |
| SOLUSDT | 2026-08 | naive | 4 977 119 | 16 | **100%** | **0%** |
| SOLUSDT | 2026-08 | drift-aware | 4 977 119 | 16 | 0% | 100% |
| XRPUSDT | 2022-11 | naive | 4 776 769 | 13 | 0% | 100% |
| XRPUSDT | 2022-11 | drift-aware | 4 776 769 | 13 | 0% | 100% |
| XRPUSDT | 2026-06 | naive | 4 178 649 | 16 | **100%** | **0%** |
| XRPUSDT | 2026-06 | drift-aware | 4 178 649 | 16 | 0% | 100% |
| XRPUSDT | 2026-07 | naive | 2 644 553 | 16 | **100%** | **0%** |
| XRPUSDT | 2026-07 | drift-aware | 2 644 553 | 16 | 0% | 100% |
| XRPUSDT | 2026-08 | naive | 6 016 728 | 16 | **100%** | **0%** |
| XRPUSDT | 2026-08 | drift-aware | 6 016 728 | 16 | 0% | 100% |

</details>

## Checklist

- [x] Chạy SOL + XRP × {2022-11, 2026-08} (13/10)
- [x] Chạy với cả 5 cặp × 4 tháng (25/10): 40 dòng, đạt mọi tiêu chí của `experiments/e1_check.py`
- [x] Hình cột nhóm cho slide 6: `results/e1/ablation.png`
- [x] `pytest tests/test_timeutil.py` xanh (contract test 3 nguồn × 2 giai đoạn), chụp output đưa vào phụ lục
- [x] Bảng ablation và ý "drift còn xảy ra *giữa các nguồn*" đã có trong nháp báo cáo C1 của Mảng 1
- [ ] (tùy chọn) E1b: đo drift giữa nguồn bằng thí nghiệm, trộn CSV (µs) với REST/WS (ms) trong cùng một giờ
