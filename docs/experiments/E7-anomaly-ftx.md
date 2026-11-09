# E7 · Phát hiện bất thường + replay FTX

**Câu hỏi:** Có phát hiện được sự kiện FTX không?
**Phụ trách:** Mảng 4

## Phương pháp

Modified z-score `M = 0.6745 · (x − median) / MAD`, báo động khi `|M| > 3.5` (Iglewicz–Hoaglin). Áp lên log-return và log(1 + khối lượng) của nến 1 phút. Median và MAD tính trên cửa sổ rolling W phút **trước** phút hiện tại (`anomaly/mzscore.py`). Mặc định W = 1440 phút và sàn |log-return| ≥ 0,5% (lý do ở phần kết quả).

## Phần 1: Batch

```bash
python experiments/e7_anomaly.py --source batch     # quét W ∈ {60,1440} × |M| ∈ {3.5,7,10} × sàn ∈ {0, 0,5%, 1%}
```

- `results/e7/sweep.csv`: `ftx_8`, `ftx_9`, `ftx_rank_8`, `ftx_rank_9`, `per_day_nov22`, `per_day_quiet26`, `volume_alerts_per_day_quiet26`
- `results/e7/ftx_btc.png`: giá BTC 11/2022 với các điểm bất thường, vùng tô là 8–9/11

Vì không có nhãn chuẩn, precision và recall chỉ mang tính tương đối. **Phải ghi rõ điều này trong báo cáo.** Dùng "số báo động/ngày giai đoạn yên ắng 6–8/2026" làm proxy cho tỷ lệ báo động giả.

### Kết quả (14/10/2026, nến batch 5 cặp)

Lần chạy đầu theo đúng plan (log-return và khối lượng thô, W = 60 phút, |M| > 3,5) **không đạt**:
- 700–900 báo động/ngày;
- 8–9/11 không nằm trong top 3 ngày;
- giai đoạn yên 2026 còn báo nhiều hơn tháng 11/2022.

Nguyên nhân và cách sửa (đã đưa vào `anomaly/mzscore.py` và `config/settings.yaml`):

| Vấn đề | Cách sửa |
|---|---|
| Khối lượng có đuôi rất dày | Dùng `log(1 + volume)` |
| W = 60 phút thích nghi quá nhanh: cả ngày FTX trở thành "bình thường" | W = 1440 phút (baseline 1 ngày) |
| M đo độ bất ngờ **tương đối**: thị trường yên thì MAD nhỏ, cú nhảy rất nhỏ cũng bị gắn cờ (BTC 6–8/2026: 29 báo động/ngày) | Thêm sàn tuyệt đối \|log-return\| ≥ 0,5% |
| Cửa sổ 7 ngày cho 0 báo động ở 8–9/11 | Không phải kết quả: dữ liệu bắt đầu 1/11 nên chưa đủ lịch sử. Cần tải thêm 10/2022 nếu muốn thử |

Bảng chọn lọc từ `results/e7/sweep.csv` (báo động log-return, 5 cặp cộng dồn):

| W | \|M\| > | Sàn | Ngày 8/11 | Ngày 9/11 | Hạng 8/11 | Hạng 9/11 | /ngày 11/2022 | /ngày yên 2026 |
|---|---|---|---|---|---|---|---|---|
| 60 | 3,5 | 0 | 197 | 86 | 4 | 29 | 144,2 | 201,8 |
| 1440 | 3,5 | 0 | 930 | 210 | 1 | 12 | 202,5 | 242,5 |
| **1440** | **3,5** | **0,5%** | **576** | **207** | **1** | **2** | **60,1** | **9,3** |
| 1440 | 3,5 | 1% | 235 | 116 | 1 | 2 | 18,3 | 0,9 |
| 1440 | 7 | 0,5% | 248 | 10 | 1 | 14 | 21,1 | 5,4 |

**Cấu hình chọn: W = 1 ngày, |M| > 3,5 (giữ đúng ngưỡng Iglewicz–Hoaglin), sàn 0,5%.**
- 8/11 và 9/11/2022 là **hai ngày nhiều báo động nhất tháng**; riêng 8/11 gấp khoảng 3 lần ngày thứ ba (14/11).
- Giai đoạn yên 2026 còn khoảng 9 báo động/ngày trên 5 cặp, tức khoảng 1,8/cặp/ngày.

Hình: `results/e7/ftx_btc.png` (giá BTC + điểm bất thường) và `results/e7/alerts_per_day_nov22.png` (báo động theo ngày).

### Kiểm chứng độc lập trên sự kiện LUNA (5/2022, giữ nguyên tham số)

Vì cửa sổ W = 1440 và sàn 0,5% được chọn sau khi xem dữ liệu tháng 11/2022 (FTX), nhóm kiểm chứng lại trên sự kiện **LUNA/UST sụp đổ (9–12/5/2022)** với bộ tham số giữ nguyên hoàn toàn (`results/e7/sweep_luna.csv`):

| Ngày sự kiện | Báo động log-return (5 cặp) | Thứ hạng trong 31 ngày tháng 5/2022 | Diễn biến thực tế |
|---|---|---|---|
| **11/5/2022** | **384** | **Hạng 1** | Đỉnh điểm UST mất chốt (depeg) sâu, LUNA lao dốc tự do |
| **9/5/2022** | **237** | **Hạng 2** | UST bắt đầu mất mốc 1 USD, bán tháo lan sang toàn thị trường |
| **12/5/2022** | **106** | **Hạng 3** | LUNA về sát 0, chuỗi Terra tạm dừng hoạt động |
| **10/5/2022** | **82** | **Hạng 4** | Biến động mạnh trong nhịp hồi kỹ thuật ngắn hạn |
| Trung bình tháng 5/2022 | 46,4 / ngày | | 4 ngày sự kiện chiếm **809 / 1 439 (56,2%)** báo động cả tháng |

Cả 4 ngày của sự kiện LUNA đều chiếm **4 vị trí dẫn đầu (hạng 1, 2, 3, 4)** trong 31 ngày của tháng 5/2022 mà không cần tinh chỉnh lại bất kỳ tham số nào, chứng minh bộ phát hiện tổng quát hóa tốt sang sự kiện ngoài mẫu.

## Phần 2: Replay FTX qua streaming

```bash
experiments/e7_stress.sh ftx           # 6–10/11/2022 ở 100× (~58 phút; 6–7/11 là 2 ngày khởi động, vì MAD xấp xỉ cần 2W − 1 phút)
```

Cảnh báo được ghi vào bảng `anomalies` với `source='stream:e7_ftx_x100'` và hiện trên Grafana. So sánh với kết quả batch: có cùng phát hiện các mốc không, và phát hiện sau bao lâu.

### Kết quả replay FTX (14/10/2026, BTCUSDT 6–10/11/2022, 100×)

| Chỉ số | Giá trị |
|---|---|
| Bản tin phát lại | khoảng 41 triệu (2 đoạn: crash `BufferError` lúc 8/11 16:16, rồi phát tiếp từ aggId cuối đã vào Kafka) |
| **Nến stream so với official** | **5 758/5 758 phút khớp tuyệt đối** (6/11 00:00 → 9/11 23:58) |
| **Báo động stream so với batch** (cùng cấu hình, cùng khởi động từ 6/11) | **111/111 trùng tuyệt đối**: 0 chỉ có ở stream, 0 chỉ có ở batch |
| Báo động theo ngày (stream) | 8/11: 51 log-return + 14 khối lượng; 9/11: 46 log-return |

Hành trình chạy cũng là một bài kiểm tra chịu lỗi ngoài kế hoạch:
- producer replay chết vì `BufferError` (đã thêm backpressure, P14);
- job Spark không theo kịp 100× và bị dừng giữa chừng;
- lần chạy lại bị `OutOfMemoryError` vì lô 6,4 triệu bản tin, nên phải xóa file kế hoạch lô chưa commit (`offsets/215` + `.215.crc`) và chạy lại với `--max-offsets-per-trigger 500000`.

Dù vậy, kết quả cuối vẫn khớp tuyệt đối, nhờ checkpoint, `dropDuplicatesWithinWatermark` và ghi upsert.

**Throughput** (máy 16 GB, load 20–48, đang chạy song song luồng live, luồng C5 và một cụm khác):

| Chỉ số | Bản tin/giây |
|---|---|
| Lưu lượng live trung bình (5 cặp) | 18,6 (p95 = 58) |
| Tải replay đưa vào tối đa (100×, giờ cao điểm FTX) | 26 964 |
| Xử lý của Spark (lô 500k): trung vị / tối đa | **13 879 / 25 454** |

Pipeline xử lý được khoảng **750–1 370 lần** lưu lượng live trung bình. Ở 100× giờ cao điểm FTX, tải vượt khả năng xử lý trên máy này nên độ trễ tích lũy (job tụt lại phía sau). Stress test dưới đây nên chạy trên máy rảnh để có con số sạch.

## Phần 3: Stress test velocity

```bash
experiments/e7_stress.sh stress        # SPEEDS="1 10 50 100 200 500 1000"
```

Phát lại cùng cửa sổ 20 phút sôi động nhất của sự kiện FTX (`BTCUSDT`, `2022-11-09 14:00–14:20` UTC, **140 423 bản tin `aggTrades`**) ở 7 bậc tốc độ từ `1×` đến `1 000×` (`results/e7/stress.csv`, `results/e7/stress.png`):

| Tốc độ (`×`) | Bản tin | Tải đưa vào (bản tin/s) | Xử lý tối đa của Spark (bản tin/s) | Độ trễ p50 (s) | Độ trễ p95 (s) |
|---|---|---|---|---|---|
| 1× | 140 423 | 116,9 | 4 704 | 8,73 | 9,86 |
| 10× | 140 423 | 1 157,5 | 14 589 | 4,82 | 5,09 |
| 50× | 140 423 | 5 555,3 | 15 606 | 3,90 | **4,83** |
| 100× | 140 423 | 10 598,9 | 22 000 | 4,46 | 5,51 |
| **200×** | 140 423 | **18 817,4** | **23 766** | 6,54 | 7,75 |
| 500× | 140 423 | 35 971,9 | 14 848 | 8,60 | 9,32 |
| 1 000× | 140 423 | 47 532,1 | 12 087 | 9,08 | 9,32 |

Diễn giải:
1. **Điểm cắt công suất (saturation point):** Từ `1×` đến `200×`, tốc độ xử lý tối đa của Spark luôn cao hơn tải đưa vào và đạt đỉnh **23 766 bản tin/s** ở `200×` (gấp **1 278 lần** lưu lượng live trung bình 18,6 bản tin/s). Từ `500×` trở lên (`36k–47,5k` bản tin/s), tải đưa vào vượt ngưỡng xử lý của 4 luồng Spark nên micro-batch bị dồn ứ.
2. **Đường cong độ trễ hình chữ U:** Ở `1×`, độ trễ p95 là `9,86 s` do phải chờ `5 s` thời gian thực cho watermark vượt qua cuối phút cộng với 2 chu kỳ trigger (`2 s`). Khi tăng lên `10×` và `50×`, thời gian chờ watermark theo đồng hồ thật co lại (`5 / k` giây) trong khi Spark vẫn dư công suất nên độ trễ p95 giảm xuống đáy **4,83 s**. Từ `100×` đến `1 000×`, độ trễ tăng ngược trở lại lên `9,32 s` do chi phí xử lý lô lớn.

## Phần 4: Mở rộng Học máy Đa biến (E7-ML — Unsupervised & Cross-Event Supervised ML)

```bash
PYTHONPATH=src .venv/bin/python experiments/e7_ml_compare.py
```

Để khắc phục hạn chế của Modified Z-Score đơn biến (bỏ sót các pha "rút râu nến" / *Flash Wick* khi giá đóng cửa quay về gần giá mở cửa dù biên độ đỉnh–đáy và khối lượng nổ tung), module `src/bdi/anomaly/ml.py` trích xuất **11 đặc trưng cấu trúc vi mô & đồng pha liên thị trường** không nhìn trước (`abs_return`, `hl_range`, `wick_ratio`, `mz_return`, `mz_range`, `mz_volume`, `mz_trades`, `mz_avg_size`, `roll15_mz_return`, `market_mz_return`, `market_mz_volume`) và đối sánh 6 mô hình (`results/e7/ml_comparison.csv`, `results/e7/ml_comparison_metrics.png`, `results/e7/ml_pr_feature_importance.png`).

Đánh giá độc lập theo đúng trục thời gian lịch sử (**Zero Time-Travel / Strict Chronological Split**):
- **Tập Huấn luyện (Quá khứ — Tháng `05/2022`, 210 230 nến):** Mô hình không giám sát (`IsolationForest`, `EllipticEnvelope`) chỉ học trên 26 ngày ngoài sự kiện của tháng 5/2022; mô hình có giám sát (`LogisticRegression`, `RandomForest`, `HistGradientBoosting`) học trên toàn bộ tháng 5/2022 (sự kiện LUNA `09–12/05/2022`). Toàn bộ việc chọn ngưỡng cắt đều thực hiện trên tháng 5/2022.
- **Tập Kiểm thử ở Tương lai (852 460 nến):** Tháng `11/2022` (sự kiện sập sàn FTX sau đó 6 tháng — `08–09/11/2022`) và giai đoạn thị trường yên `06–08/2026` (sau đó 4 năm, 91 ngày).

| Mô hình (`scikit-learn`) | Nhóm phương pháp | Precision | Recall | F1-Score | ROC-AUC | PR-AUC | Báo động/ngày (`2026`) | % Cảnh báo FTX | Bắt Flash Wick (`/179`) | SNR (`FTX / 2026`) | Train (`s`) | Suy luận (`ms / 1k dòng`) | Thông lượng (`dòng / s`) |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **Modified Z-Score (Baseline)** | Thống kê đơn biến | 0,295 | 0,781 | 0,428 | 0,9091 | 0,195 | 9,36 | 43,4% | 0 (0,0%) | 41,8 | 0,00 | 0,005 | 208 678 000 |
| **Isolation Forest** | Unsupervised ML | 0,281 | 0,792 | 0,415 | 0,9985 | 0,280 | 9,60 | 49,1% | 145 (81,0%) | 49,9 | 1,27 | 7,762 | 128 836 |
| **Elliptic Envelope (Mahalanobis)** | Unsupervised ML | 0,289 | 0,822 | 0,427 | 0,9986 | 0,308 | 9,78 | 53,3% | 163 (91,1%) | 53,6 | 14,66 | 0,064 | 15 642 550 |
| **Logistic Regression** | Supervised ML | 0,376 | 0,738 | 0,498 | 0,9989 | 0,473 | 4,33 | **62,3%** | 106 (59,2%) | 113,2 | 2,48 | 0,069 | 14 574 050 |
| **Random Forest** | Supervised ML | **0,456** | 0,806 | **0,582** | 0,9994 | 0,614 | **3,53** | 55,6% | 101 (56,4%) | **114,5** | 7,03 | 1,320 | 757 498 |
| **HistGradientBoosting (XGBoost/LGBM)** | Supervised ML | 0,422 | **0,910** | 0,577 | **0,9995** | **0,672** | 4,43 | 52,2% | **162 (90,5%)** | 103,8 | 5,21 | **0,974** | **1 026 521** |

Kết luận chính:
1. **Vượt trội về độ phân tách tương lai (PR-AUC & F1-Score):** Học trên vụ sụp đổ LUNA (`05/2022`) và dự báo cho vụ sập FTX 6 tháng sau (`11/2022`), `RandomForest` và `HistGradientBoosting` nâng **F1-Score** từ `0,428` lên **`0,577 – 0,582`**, nâng **PR-AUC** gấp **3,1–3,4 lần** (từ `0,195` lên **`0,614 – 0,672`**), đồng thời giảm 2,1–2,6 lần báo động giả ở tương lai năm 2026 (từ `9,36` xuống **`3,53 – 4,43` báo động/ngày**, đưa `SNR` tăng từ `41,8` lên **`103,8 – 114,5`**).
2. **Khắc phục điểm mù Flash Wick / Whipsaw:** Trong 2 ngày sập sàn FTX (`08–09/11/2022`) có **179 phút** quét râu nến dữ dội ($\ln(H/L) \ge 0,8\%$, $M(\text{range}) > 3,5$, $M(\text{vol}) > 2,0$) nhưng giá đóng cửa rút chân làm $|\text{log\_return}| < 0,5\%$. `Modified Z-Score` bỏ sót **100% (`0/179`)**, trong khi `Isolation Forest` bắt được **81,0% (`145/179`)** và `HistGradientBoosting` bắt được **90,5% (`162/179`)**.
3. **Độ quan trọng đặc trưng & Tốc độ suy luận thời gian thực:** Hai đặc trưng biên độ đỉnh–đáy (`hl_range`: **26,7%**, `mz_range`: **25,1%**) chiếm **51,8%** quyền quyết định của `RandomForest`. Về tốc độ, `HistGradientBoosting` chỉ mất **5,21 giây** để huấn luyện trên 210 nghìn nến và đạt tốc độ suy luận **0,974 ms / 1 000 dòng** (tức **> 1,02 triệu nến/giây** — trong Spark Streaming mỗi phút 5 cặp chỉ có 5 nến, tốn `< 0,01 ms`, hoàn toàn không gây trễ luồng trực tuyến).

## Checklist

- [x] Bảng quét tham số + hình FTX (cấu hình chọn: 8–9/11 là top-2 ngày)
- [x] Kiểm tra lại trên sự kiện khác (LUNA 5/2022) để tránh chọn tham số theo dữ liệu: 4 ngày 9–12/5/2022 chiếm trọn top 1–4 của tháng
- [x] Replay 100× có cảnh báo trên stream: 111/111 trùng batch, nến 5 758/5 758 khớp official
- [x] Throughput sơ bộ: xử lý 13,9k (trung vị) – 25,5k (tối đa) bản tin/s, khoảng 750–1 370 lần lưu lượng live
- [x] Stress test theo bậc tốc độ (`1×` → `1 000×`, `e7_stress.sh stress`) → `results/e7/stress.csv`, `results/e7/stress.png`
- [x] Mở rộng Học máy đa biến (`IsolationForest`, `EllipticEnvelope`, `LogisticRegression`, `RandomForest`, `HistGradientBoosting`) → `results/e7/ml_comparison.csv`, `results/e7/ml_comparison_metrics.png`, `results/e7/ml_pr_feature_importance.png`
- [ ] (nếu còn thời gian) Replay 1× cho 1 giờ quanh đỉnh 9/11
