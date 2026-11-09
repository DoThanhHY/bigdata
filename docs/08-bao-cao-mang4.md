# 08 · Nội dung báo cáo và thuyết trình — Mảng 4

Tài liệu này tổng hợp toàn bộ phần viết báo cáo học thuật (theo khung 5 mục quy định tại `docs/07-huong-dan-thanh-vien.md` §A5) và lời thuyết trình cho các slide của **Mảng 4** (Thống kê mô tả, Phát hiện bất thường E7, Stress test velocity và Grafana).

---

## Phần 1 · Thống kê mô tả tập dữ liệu (mở đầu phần Thực nghiệm trong Báo cáo)

### 1.1 Quy mô và phân bố giao dịch theo cặp × tháng
Script `experiments/descriptive_stats.py` quét toàn bộ 20 tập tin `aggTrades` và bảng nến 1 phút (`candles`) của 5 cặp giao dịch (`BTCUSDT`, `ETHUSDT`, `BNBUSDT`, `SOLUSDT`, `XRPUSDT`) qua hai giai đoạn đối lập:
- **Giai đoạn biến động cực đoan (`2022-11`):** Tháng xảy ra sự kiện sàn FTX sụp đổ (timestamp đơn vị mili-giây).
- **Giai đoạn thị trường yên ắng (`2026-06`, `2026-07`, `2026-08`):** 3 tháng giao dịch bình thường (timestamp đơn vị micro-giây).

Tổng cộng tập dữ liệu lịch sử gồm **418 156 831 bản ghi `aggTrades`**, đại diện cho **1 049 045 915 giao dịch khớp lệnh lẻ (`trades`)** trên **878 400 cây nến 1 phút**, với tổng giá trị giao dịch đạt **400,35 tỷ USDT** (chi tiết tại `results/desc/trades_by_month.csv`):

| Cặp | Kỳ | Số phút | Dòng `aggTrades` (`n_agg`) | Giao dịch khớp lệnh (`n_trades`) | Khối lượng (tỷ USDT) | `trades`/phút p50 | `trades`/phút p95 | Mua chủ động (`is_buyer_maker=false`) |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| BTCUSDT | 2022-11 | 43 200 | 171 900 348 | 204 731 238 | 163,722 | 3 717,5 | 11 179,1 | 50,22% |
| BTCUSDT | 2026-06 | 43 200 | 34 074 359 | 128 668 052 | 39,984 | 1 974,0 | 8 614,0 | 50,82% |
| BTCUSDT | 2026-07 | 44 640 | 24 186 730 | 84 214 146 | 31,598 | 1 399,0 | 5 113,0 | 51,36% |
| BTCUSDT | 2026-08 | 44 640 | 23 939 440 | 91 916 644 | 33,605 | 1 436,0 | 6 439,0 | 52,05% |
| ETHUSDT | 2022-11 | 43 200 | 22 564 378 | 31 374 908 | 30,251 | 427,0 | 2 238,0 | 50,35% |
| ETHUSDT | 2026-06 | 43 200 | 30 664 042 | 121 460 065 | 18,362 | 1 651,0 | 7 823,1 | 49,68% |
| ETHUSDT | 2026-07 | 44 640 | 21 128 759 | 70 199 896 | 13,891 | 1 032,0 | 4 643,0 | 49,94% |
| ETHUSDT | 2026-08 | 44 640 | 20 462 172 | 73 659 144 | 17,970 | 1 114,5 | 6 080,0 | 51,49% |
| BNBUSDT | 2022-11 | 43 200 | 7 977 084 | 13 249 754 | 6,261 | 190,0 | 862,0 | 52,33% |
| BNBUSDT | 2026-06 | 43 200 | 10 058 771 | 27 093 849 | 3,211 | 458,0 | 1 739,0 | 54,82% |
| BNBUSDT | 2026-07 | 44 640 | 5 772 626 | 15 836 742 | 1,737 | 258,0 | 987,0 | 55,23% |
| BNBUSDT | 2026-08 | 44 640 | 6 961 577 | 22 000 171 | 2,618 | 308,0 | 1 404,0 | 54,89% |
| SOLUSDT | 2022-11 | 43 200 | 7 077 107 | 13 550 287 | 4,591 | 121,0 | 1 349,0 | 52,95% |
| SOLUSDT | 2026-06 | 43 200 | 5 116 327 | 32 404 016 | 6,202 | 462,0 | 2 183,0 | 52,46% |
| SOLUSDT | 2026-07 | 44 640 | 3 679 293 | 18 385 315 | 3,946 | 277,0 | 1 259,0 | 55,12% |
| SOLUSDT | 2026-08 | 44 640 | 4 977 119 | 26 880 743 | 6,587 | 341,0 | 1 941,0 | 53,51% |
| XRPUSDT | 2022-11 | 43 200 | 4 776 769 | 8 245 257 | 4,898 | 123,0 | 547,0 | 48,49% |
| XRPUSDT | 2026-06 | 43 200 | 4 178 649 | 24 272 426 | 3,666 | 328,0 | 1 834,0 | 52,53% |
| XRPUSDT | 2026-07 | 44 640 | 2 644 553 | 12 473 086 | 2,061 | 177,0 | 954,0 | 51,20% |
| XRPUSDT | 2026-08 | 44 640 | 6 016 728 | 28 430 176 | 5,190 | 246,0 | 2 225,0 | 51,00% |

### 1.2 Nhận xét từ các biểu đồ thống kê mô tả
1. **Phân phối số giao dịch mỗi phút (`results/desc/trades_per_minute.png`):**
   - Số giao dịch mỗi phút có phân phối lệch phải rất mạnh (đuôi dày), trải rộng qua nhiều bậc độ lớn từ vài chục đến hơn `88 000` giao dịch/phút.
   - Riêng một tháng `2022-11` đã chiếm tới **51,2% tổng số dòng `aggTrades`** (`214,3M / 418,2M`) của cả 4 tháng. Ở cặp dẫn dắt thị trường là `BTCUSDT`, trung vị số giao dịch/phút trong tháng `2022-11` đạt `3 718` (p95 = `11 179`), cao gấp `2,4 lần` so với giai đoạn `2026` (`1 577` giao dịch/phút).
2. **Khối lượng giao dịch USDT theo ngày (`results/desc/daily_quote_volume.png`):**
   - Trong hai ngày 8–9/11/2022 khi FTX mất khả năng thanh khoản, khối lượng giao dịch của cả 5 cặp đồng loạt dựng đứng: riêng `BTCUSDT` ngày 8/11 đạt **15,05 tỷ USDT**, gấp ~15 lần mức trung bình ngày của mùa yên 2026 (~1,1 tỷ USDT/ngày).
3. **Tỉ lệ lệnh mua chủ động (`results/desc/taker_buy_ratio.png`):**
   - Tỉ lệ lệnh có `is_buyer_maker = false` (người mua khớp lệnh thị trường chủ động vào sổ lệnh chờ bán) dao động quanh ngưỡng cân bằng 50% (từ `48,49%` đến `55,23%`). Trong tháng `2022-11`, tỉ lệ mua chủ động ở tất cả các cặp đều thấp hơn giai đoạn 2026 (đặc biệt `XRPUSDT` tụt xuống dưới 50%, còn `48,49%`), phản ánh áp lực bán tháo chủ động áp đảo trong khủng hoảng FTX.

---

## Phần 2 · Thực nghiệm E7: Phát hiện bất thường & Stress test Velocity

### 2.1 Câu hỏi nghiên cứu
1. Một bộ phát hiện bất thường thống kê phi tham số có thể tự động nhận diện chính xác thời điểm xảy ra khủng hoảng thị trường (sự kiện FTX ngày 8–9/11/2022) với tỷ lệ báo động giả thấp trong giai đoạn thị trường yên (6–8/2026) hay không?
2. Bộ tham số chọn trên FTX có tổng quát hóa được sang một sự kiện sụp đổ khác ngoài mẫu (**LUNA/UST tháng 5/2022**) mà không cần chỉnh lại tham số không?
3. Khi chạy cùng bộ phát hiện đó trên luồng Streaming với tốc độ phát lại gấp `100×`–`1 000×` thời gian thực, kết quả cảnh báo giữa tầng Streaming và tầng Batch có trùng khớp tuyệt đối không (mở rộng đóng góp **C2**), và ngưỡng chịu tải tối đa (**Velocity**) của hệ thống là bao nhiêu bản tin/giây?

### 2.2 Thiết kế thí nghiệm & Phương pháp
- **Mô hình phát hiện (`src/bdi/anomaly/mzscore.py`):** Sử dụng chỉ số **Modified Z-Score** của Iglewicz & Hoaglin trên cửa sổ trượt nhân quả (chỉ dùng $W$ phút **trước** phút hiện tại $t$, không nhìn trước tương lai):
  $$M_t = \frac{0,6745 \cdot (x_t - \text{median}_{t-W..t-1})}{\text{MAD}_{t-W..t-1}}, \quad \text{cảnh báo khi } |M_t| > 3,5$$
- **Đặc trưng đầu vào & 3 cải tiến kỹ thuật:**
  1. **Biến đổi `log(1 + volume)`:** Khối lượng giao dịch có phân phối đuôi dày cực đoan; nếu áp trực tiếp lên `volume` thô sẽ gây báo động liên tục.
  2. **Cửa sổ nền $W = 1440$ phút (1 ngày):** Cửa sổ ngắn $W = 60$ phút thích nghi quá nhanh với biến động cao kéo dài — chỉ sau 1 giờ bán tháo, mức biến động mới trở thành "bình thường" khiến ngày 9/11/2022 tụt xuống hạng 29/30.
  3. **Sàn biến động tuyệt đối $|\text{log\_return}| \ge 0,005$ (0,5%):** Vì $M_t$ đo độ bất ngờ *tương đối* so với $\text{MAD}$ của cửa sổ trước đó, trong giai đoạn thị trường đi ngang (2026), $\text{MAD}$ co lại rất nhỏ khiến các biến động nhiễu vi mô (0,05%–0,1%) cũng vượt ngưỡng $|M_t| > 3,5$. Sàn tuyệt đối 0,5% loại bỏ triệt để hiện tượng này.
- **Tích hợp hai tầng (Batch & Streaming):**
  - Tầng Batch gọi `detect(candles_df)` trên toàn bộ lịch sử.
  - Tầng Streaming gọi `detect_for_batch(batch_df, source)` bên trong `foreachBatch` của Spark Structured Streaming (`src/bdi/stream/stream_candles.py`), nạp $2W + 1$ phút lịch sử từ Postgres (vì xấp xỉ MAD vector hóa hai tầng median cần $2W - 1$ điểm khởi động — bẫy kỹ thuật **P15**) và ghi idempotent vào bảng `anomalies`.

### 2.3 Kết quả thực nghiệm

#### a) Quét tham số trên sự kiện FTX (11/2022) vs Thị trường yên (6–8/2026)
Bảng trích từ `results/e7/sweep.csv` (tổng số báo động `log_return` trên cả 5 cặp):

| Cửa sổ $W$ (phút) | Ngưỡng $\|M\| >$ | Sàn $\|r\|$ | Báo động 8/11 | Báo động 9/11 | Hạng ngày 8/11 | Hạng ngày 9/11 | TB / ngày 11/2022 | TB / ngày yên 2026 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 60 | 3,5 | 0% | 197 | 86 | 4 | 29 | 144,2 | 201,8 |
| 1440 | 3,5 | 0% | 930 | 210 | 1 | 12 | 207,6 | 243,2 |
| **1440** | **3,5** | **0,5%** | **576** | **207** | **1** | **2** | **60,1** | **9,3** |
| 1440 | 3,5 | 1,0% | 235 | 116 | 1 | 2 | 18,3 | 0,9 |
| 1440 | 7,0 | 0,5% | 248 | 10 | 1 | 14 | 21,3 | 5,5 |

- Hình minh họa: `results/e7/ftx_btc.png` và `results/e7/alerts_per_day_nov22.png`.

#### b) Kiểm chứng độc lập (Out-of-sample) trên sự kiện LUNA sụp đổ (5/2022)
Giữ nguyên hoàn toàn bộ tham số đã chọn ($W = 1440$, $|M| > 3,5$, sàn $0,5\%$) và chạy trên dữ liệu tháng `2022-05` (`results/e7/sweep_luna.csv`):

| Ngày sự kiện LUNA | Số báo động `log_return` (5 cặp) | Thứ hạng trong 31 ngày tháng 5/2022 | Diễn biến thị trường tương ứng |
|---|---:|---:|---|
| **11/5/2022** | **384** | **Hạng 1 / 31** | Đỉnh điểm UST mất chốt (depeg), LUNA sụp đổ dây chuyền |
| **9/5/2022** | **237** | **Hạng 2 / 31** | UST bắt đầu mất mốc 1 USD, làn sóng bán tháo khởi phát |
| **12/5/2022** | **106** | **Hạng 3 / 31** | LUNA mất 99%+ giá trị, chuỗi Terra tạm dừng |
| **10/5/2022** | **82** | **Hạng 4 / 31** | Biến động cực mạnh trong nhịp hồi kỹ thuật ngắn |
| **Trung bình tháng 5/2022** | **46,4 / ngày** | *(Mùa yên 2026: **9,3 / ngày**)* | 4 ngày sự kiện chiếm **809 / 1 439 (56,2%)** báo động cả tháng |

#### c) Đối soát cảnh báo Streaming vs Batch (Replay FTX 100×) & Stress test Velocity (`1×` → `1 000×`)
- **Replay 4 ngày sự kiện FTX (`BTCUSDT`, 6–10/11/2022, ~41 triệu bản tin ở tốc độ `100×`):**
  - Nến streaming khớp official tuyệt đối **5 758 / 5 758 phút** (`100%`).
  - Cảnh báo bất thường phát ra trên luồng streaming trùng khít với tầng batch **111 / 111 cảnh báo** (`0` cảnh báo chỉ có ở stream, `0` cảnh báo chỉ có ở batch — `results/e7/stream_vs_batch.csv`).
- **Stress test theo bậc tốc độ (`results/e7/stress.csv`, `results/e7/stress.png`):**
  Phát lại 20 phút cao điểm nhất của FTX (`2022-11-09 14:00–14:20` UTC, `140 423` bản tin) qua 7 bậc tốc độ:
  - Tốc độ xử lý của Spark đạt đỉnh **23 766 bản tin/s** ở bậc `200×` (tải đưa vào `18 817 bản tin/s`, độ trễ p50 = `6,54 s`, p95 = `7,75 s`), gấp **1 278 lần** lưu lượng live trung bình (`18,6 bản tin/s`).
  - Độ trễ ghi nến p95 tạo đường cong chữ U: từ `9,86 s` ở `1×` (do chờ watermark 5s theo thời gian thực), giảm xuống đáy **`4,83 s`** ở `50×`, rồi tăng lại lên `9,32 s` ở `500×–1 000×` khi tải đưa vào (`36k–47,5k bản tin/s`) vượt ngưỡng công suất xử lý của 4 luồng CPU.

#### d) Mở rộng Học máy Đa biến (E7-ML: Isolation Forest, Random Forest & HistGradientBoosting)
Để khắc phục điểm mù của phương pháp đơn biến (bỏ sót các pha "rút râu nến" / *Flash Wick* khi giá đóng cửa rút chân về gần giá mở cửa dù biên độ đỉnh–đáy và khối lượng nổ tung), nhóm xây dựng module `src/bdi/anomaly/ml.py` và kịch bản `experiments/e7_ml_compare.py` trích xuất **11 đặc trưng cấu trúc vi mô và đồng pha liên thị trường** (`abs_return`, `hl_range`, `wick_ratio`, `mz_return`, `mz_range`, `mz_volume`, `mz_trades`, `mz_avg_size`, `roll15_mz_return`, `market_mz_return`, `market_mz_volume`).

Bảng đối sánh kết quả kiểm thử thuận chiều thời gian lịch sử (**Zero Time-Travel**: học trên quá khứ **LUNA `2022-05`**, kiểm thử độc lập trên tương lai **FTX `2022-11`** + **Quiet `2026-06..08`**, `results/e7/ml_comparison.csv`, hình `results/e7/ml_comparison_metrics.png` và `results/e7/ml_pr_feature_importance.png`):

| Mô hình (`scikit-learn`) | Nhóm phương pháp | Precision | Recall | F1-Score | ROC-AUC | PR-AUC | Báo động/ngày (`2026`) | % Cảnh báo FTX | Bắt Flash Wick (`/179`) | SNR (`FTX / 2026`) | Train (`s`) | Suy luận (`ms / 1k dòng`) |
|---|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **Modified Z-Score (Baseline)** | Thống kê đơn biến | 0,295 | 0,781 | 0,428 | 0,9091 | 0,195 | 9,36 | 43,4% | 0 (0,0%) | 41,8 | 0,00 | 0,005 |
| **Isolation Forest** | Unsupervised ML | 0,281 | 0,792 | 0,415 | 0,9985 | 0,280 | 9,60 | 49,1% | 145 (81,0%) | 49,9 | 1,27 | 7,762 |
| **Elliptic Envelope (Mahalanobis)** | Unsupervised ML | 0,289 | 0,822 | 0,427 | 0,9986 | 0,308 | 9,78 | 53,3% | 163 (91,1%) | 53,6 | 14,66 | 0,064 |
| **Logistic Regression** | Supervised ML | 0,376 | 0,738 | 0,498 | 0,9989 | 0,473 | 4,33 | **62,3%** | 106 (59,2%) | 113,2 | 2,48 | 0,069 |
| **Random Forest** | Supervised ML | **0,456** | 0,806 | **0,582** | 0,9994 | 0,614 | **3,53** | 55,6% | 101 (56,4%) | **114,5** | 7,03 | 1,320 |
| **HistGradientBoosting (XGBoost/LGBM)** | Supervised ML | 0,422 | **0,910** | 0,577 | **0,9995** | **0,672** | 4,43 | 52,2% | **162 (90,5%)** | 103,8 | 5,21 | **0,974** |

- **Khả năng tổng quát hóa tương lai (Past $\rightarrow$ Future Generalization):** Các mô hình cây học trên sự kiện LUNA (`05/2022`) và kiểm thử trên sự kiện FTX xảy ra 6 tháng sau (`11/2022`) nâng **F1-Score** từ `0,428` lên **`0,582` (`RandomForest`)** và **`0,577` (`HistGradientBoosting`)**, nâng **PR-AUC** gấp **3,1–3,4 lần** (từ `0,195` lên **`0,614 – 0,672`**), đồng thời giảm более 2 lần báo động giả ở tương lai năm 2026 (từ `9,36` xuống `3,53 – 4,43` báo động/ngày, đưa tỉ số Tín hiệu/Nhiễu `SNR` tăng từ `41,8` lên **`103,8 – 114,5`**).
- **Bắt các pha quét râu nến (*Flash Wick / Whipsaw*):** Trong 2 ngày sập sàn FTX có **179 phút** biến động đỉnh–đáy cực mạnh ($\ln(H/L) \ge 0,8\%$, $M(\text{range}) > 3,5$, $M(\text{vol}) > 2,0$) nhưng giá đóng cửa rút chân làm $|\text{log\_return}| < 0,5\%$. `Modified Z-Score` đơn biến bỏ sót **100% (`0/179`)**, trong khi `Isolation Forest` bắt được **81,0% (`145/179`)** và `HistGradientBoosting` bắt được **90,5% (`162/179`)**.
- **Độ quan trọng đặc trưng & Tốc độ thực thi:** Biên độ đỉnh–đáy trong phút (`hl_range`: **26,7%** và `mz_range`: **25,1%**) chiếm **51,8%** quyền quyết định. Về tốc độ, `HistGradientBoosting` huấn luyện mất `5,21 s` và suy luận đạt **0,974 ms / 1 000 dòng** (`> 1,02 triệu dòng/giây`), không gây bất kỳ độ trễ nào khi nhúng vào `foreachBatch` của Spark Streaming.

### 2.4 Diễn giải
1. **Hiệu quả của thiết kế cửa sổ 1 ngày kết hợp sàn tuyệt đối:** Giúp giữ nguyên ngưỡng chuẩn lý thuyết $|M| > 3,5$ của Iglewicz–Hoaglin mà vẫn đưa 2 ngày FTX (8–9/11/2022) lên đúng hạng 1 và 2 của tháng, đồng thời kéo số báo động mùa yên từ `201,8/ngày` xuống `9,3/ngày` (chỉ ~1,8 báo động/cặp/ngày).
2. **Tính tổng quát hóa (không bị overfit):** Khi kiểm chứng độc lập trên tháng 5/2022 (sự kiện LUNA), cả 4 ngày sự kiện 9–12/5/2022 chiếm trọn 4 vị trí đầu bảng (hạng 1, 2, 3, 4) trong 31 ngày mà không cần thay đổi bất kỳ tham số nào.
3. **Mở rộng đóng góp C2 ("Viết một lần, chạy hai tầng"):** Không chỉ việc dựng nến OHLCV đạt độ khớp 100%, mà ngay cả mô hình phát hiện bất thường chạy trên luồng streaming tốc độ `100×` cũng cho tập cảnh báo trùng khít `111/111` với tầng batch.

### 2.5 Hạn chế
1. **Không có nhãn chuẩn (ground truth) cho từng phút bất thường:** Thị trường tài chính không có nhãn đúng/sai ở mức từng phút, nên không thể tính Precision/Recall tuyệt đối như ở bài toán ghép bản ghi E5. Nhóm đánh giá thông qua thứ hạng ngày sự kiện lớn (FTX, LUNA) và dùng số báo động/ngày ở giai đoạn yên 2026 làm proxy cho tỷ lệ báo động giả.
2. **Đặc trưng đơn biến:** Mô hình hiện xét riêng từng chuỗi `log_return` và `log_volume`; hướng phát triển tiếp theo là kết hợp đa biến (như biên độ `high/low`, độ lệch `taker_buy`) bằng Isolation Forest hoặc khoảng cách Mahalanobis bền vững.

---

## Phần 3 · Gợi ý lời nói khi thuyết trình (Slide 14, 15, 17, 18)

- **Slide 14 (E7 — Phát hiện bất thường trên Batch: FTX & LUNA):**
  > *"Ở thí nghiệm E7, câu hỏi đặt ra là liệu hệ thống có tự động bắt được các cú sập lịch sử mà không báo động giả liên tục ở mùa yên hay không. Nếu dùng cửa sổ 60 phút và không có sàn biên độ theo kế hoạch ban đầu, cửa sổ thích nghi quá nhanh khiến ngày 9/11 của FTX tụt xuống hạng 29, trong khi mùa yên 2026 lại báo động hơn 200 lần mỗi ngày. Khi nhóm chuyển sang cửa sổ nền 1 ngày kết hợp sàn log-return 0,5%, ngày 8 và 9/11 lập tức vọt lên hạng 1 và hạng 2 toàn tháng, còn mùa yên giảm hơn 20 lần xuống chỉ còn 9,3 báo động/ngày. Đặc biệt, để chứng minh không chọn tham số khớp theo dữ liệu FTX, nhóm giữ nguyên bộ tham số này và chạy kiểm chứng độc lập trên tháng 5/2022 khi LUNA sụp đổ: kết quả là cả 4 ngày sự kiện từ 9 đến 12/5 đều chiếm trọn Top 1, 2, 3 và 4 của tháng."*
- **Slide 15 (E7 — Replay FTX trên Streaming & Stress test Velocity):**
  > *"Tiếp theo, nhóm phát lại dữ liệu FTX ở tốc độ gấp 100 lần thời gian thực qua Kafka và Spark Streaming. Kết quả là 5.758 trên 5.758 cây nến khớp tuyệt đối với official, và toàn bộ 111 cảnh báo bất thường trên stream trùng khít 100% với tầng batch — chứng minh triết lý 'viết một lần, chạy hai tầng' đúng cho cả tầng phân tích bất thường. Khi thực hiện stress test tăng tốc từ 1× lên 1.000× trên 20 phút cao điểm nhất của FTX, hệ thống đạt đỉnh xử lý 23.766 bản tin/giây ở mốc 200× — tức gấp gần 1.300 lần lưu lượng giao dịch thực tế của Binance."*
- **Slide 17 (Bài học kỹ thuật):**
  > *"Trong quá trình vận hành thực tế, nhóm phát hiện 16 bẫy kỹ thuật không có trong sách vở. Đáng chú ý nhất là: (1) Nếu backfill từng cặp xen kẽ với tin live, watermark toàn cục của Spark sẽ tiến lên và vứt bỏ toàn bộ dữ liệu bù của các cặp sau; (2) Nếu replay E3 bằng cách đặt offset Kafka về đầu thay vì phát lại có nhịp, mọi bản tin dồn vào một micro-batch nên không bản tin nào bị coi là trễ; và (3) Bộ tính MAD xấp xỉ hai lần trượt cần 2W - 1 phút lịch sử thay vì W phút để khởi động."*
- **Slide 18 (Kết luận & Hạn chế):**
  > *"Tóm lại, hệ thống đã giải quyết trọn vẹn 3 câu hỏi nghiên cứu bằng số liệu thực nghiệm: chuẩn hóa thời gian tự nhận diện đơn vị đạt độ đúng 100% qua hai thời kỳ ms và µs; tầng batch và streaming tái tạo khớp tuyệt đối 100% cả nến lẫn cảnh báo bất thường; và chính sách chọn nguồn định lượng rõ: dùng streaming cho độ tươi với thời gian hội tụ trung vị 13 giây, dùng batch làm nguồn chuẩn sau T+1."*
