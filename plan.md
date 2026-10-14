# Plan: Đóng góp & Thực nghiệm — Tích hợp batch và streaming từ Binance

**Môn:** IT5427 · Tích hợp và xử lý dữ liệu lớn
**Nhóm 3:** Vũ Tuấn Minh, Đỗ Xuân Thanh, Đào Quang Huy, Trần Anh Vũ

> **Nguyên tắc xuyên suốt:** mỗi đóng góp phải có ít nhất một thí nghiệm chứng minh bằng số liệu. Đóng góp không có thực nghiệm đi kèm chỉ là lời hứa.

> **Checkpoint 13/10:** nếu luồng streaming chưa chạy trọn vẹn cho ít nhất 1 cặp, ưu tiên xử lý việc đó trước mọi mục trong plan này (theo đúng kế hoạch rủi ro của nhóm).

---

## Phần A — Đóng góp (Contributions)

| Mã | Đóng góp | Trả lời | Chứng minh bằng |
|---|---|---|---|
| C1 | Lược đồ chuẩn nhận biết schema drift | RQ1 | E1 |
| C2 | "Viết một lần, chạy hai tầng" có kiểm chứng tương đương | RQ2 | E2, E3 |
| C3 | Khung đối soát 3 nguồn + phân loại nguyên nhân sai lệch | RQ2 | E2, E3, E4 |
| C4 | Record linkage mờ với ground truth miễn phí | Kỹ thuật môn học | E5 |
| C5 | Chính sách chọn nguồn dựa trên độ hội tụ | RQ3 | E3, E4 |
| C6 | Truy vết nguồn gốc từng cây nến + ghi idempotent | Quản trị dữ liệu | E4 |

### C1. Lược đồ chuẩn nhận biết schema drift (drift-aware canonical schema)

Nhóm xây một lược đồ trung gian dùng chung cho ba nguồn: CSV aggTrades, CSV klines và JSON WebSocket. Hàm chuẩn hóa thời gian **tự nhận diện đơn vị theo độ lớn giá trị**: khoảng 1,6×10¹² là mili-giây, khoảng 1,6×10¹⁵ là micro-giây. Hàm không viết cứng quy tắc "trước 2025 là ms", và mọi giá trị đều được quy về micro-giây.

Cách kiểm chứng là contract test trên cả ba nguồn, mỗi nguồn ở cả hai giai đoạn 2022 và 2026.

### C2. "Viết một lần, chạy hai tầng" (single-logic Lambda)

Logic dựng nến là một hàm PySpark thuần, `build_candles(df)`, được tầng batch và tầng streaming import chung.

Ba điểm kỹ thuật cần nêu trong báo cáo:

- Trong streaming, `first()` và `last()` không đảm bảo thứ tự, nên Open và Close phải tính bằng `min_by(price, struct(ts, agg_id))` và `max_by(price, struct(ts, agg_id))`.
- Giá và khối lượng dùng `DecimalType` thay vì `double`, để đối chiếu khớp tuyệt đối với klines chính thức.
- Số trade mỗi phút được tính bằng `sum(last_trade_id − first_trade_id + 1)`, để so được với trường "number of trades" của klines.

### C3. Khung đối soát 3 nguồn + phân loại nguyên nhân sai lệch

Mỗi cây nến lệch so với nến chính thức được gắn **nhãn nguyên nhân**:

| Nhãn | Ý nghĩa |
|---|---|
| `LATE_BEYOND_WATERMARK` | Bản tin đến sau watermark, bị loại |
| `DISCONNECT_GAP` | Mất bản tin do WebSocket ngắt kết nối |
| `DUPLICATE` | Bản tin trùng chưa được loại |
| `EMPTY_MINUTE` | Phút không có giao dịch (cần kiểm tra Binance có sinh nến volume 0 không) |
| `NUMERIC_PRECISION` | Sai số do kiểu số |
| `UNKNOWN` | Chưa giải thích được |

Kết quả không dừng ở "khớp X%" mà trả lời được "lệch vì sao".

### C4. Record linkage mờ với ground truth miễn phí

Đây là cách biến điểm yếu nhất của đề tài (đang tự đánh giá "Yếu") thành một thí nghiệm có giá trị.

Nhóm cố tình **bỏ aggId** và ghép bản ghi stream với batch bằng so khớp mờ trên bộ (giá, khối lượng, thời gian ± δ). Vì aggId có sẵn, nhóm có nhãn đúng 100% để tính precision, recall và F1. Hiếm bài toán record linkage nào có ground truth sạch như vậy.

Nhóm cũng so sánh nhiều chiến lược blocking, ví dụ theo (symbol, giây) hoặc theo (symbol, giây, bucket giá), về số cặp phải so sánh và độ chính xác.

### C5. Chính sách chọn nguồn dựa trên độ hội tụ

Nhóm định nghĩa chỉ số **time-to-convergence**: khoảng thời gian từ khi một phút kết thúc đến khi nến streaming trùng với nến chính thức.

Quy tắc chọn nguồn rút ra:

- **Streaming** dùng cho độ tươi (dashboard, cảnh báo).
- **Batch / official** dùng cho độ đúng sau T+1.
- **Official** là chuẩn khi ba nguồn xung đột.

Tiêu chí đánh giá mỗi nguồn gồm độ đầy đủ (completeness), độ kịp thời (timeliness) và độ nhất quán (consistency).

### C6. Truy vết nguồn gốc + idempotent

Mỗi cây nến mang theo các thông tin: `source`, `file_checksum`, `ingested_at`, `watermark_config`, `pipeline_version`.

Việc ghi dùng upsert với khóa `(symbol, minute, source)`, ví dụ `INSERT ... ON CONFLICT DO UPDATE` qua `foreachBatch`. Khi kết hợp với Kafka offset và checkpoint, hệ thống đạt hiệu quả tương đương exactly-once.

---

## Phần B — Thực nghiệm (tích hợp)

### Tổng quan

| # | Câu hỏi | Biến thay đổi | Đo gì | Hình/bảng chính |
|---|---|---|---|---|
| E1 | Bỏ qua drift thì hỏng đến đâu? | naive vs drift-aware | % bản ghi sai mốc, % nến sai | Bảng ablation 2022 vs 2026 |
| E2 | Batch có khớp official không? | 5 cặp × 4 tháng | % nến khớp tuyệt đối từng trường OHLCV + số trades | Heatmap cặp × tháng |
| E3 | Watermark đánh đổi thế nào? | watermark 0 / 2 / 5 / 10 / 30 s | % khớp official, độ trễ p50/p95 | **Đường cong trade-off 2 trục** |
| E4 | Hệ thống chịu lỗi ra sao? | ngắt WS, kill Spark, restart Kafka | Thời gian phục hồi, số nến thiếu/trùng | Timeline sự cố có đánh dấu |
| E5 | Ghép mờ được không? | dung sai δ, chiến lược blocking | Precision / Recall / F1, số cặp so sánh | Đường P–R theo δ |
| E6 | Truy vấn xấp xỉ có đáng không? | `rsd` 0.01/0.05/0.1, `relativeError` | Sai số & thời gian trên ~185M dòng | Sai số vs tốc độ |
| E7 | Phát hiện được FTX không? | Ngưỡng z, cửa sổ rolling | Bắt được 8–9/11/2022 không, số báo động/ngày khi thị trường yên | Giá BTC 11/2022 phủ điểm bất thường |

### E1 — Ablation schema drift

- **Cách làm:** chạy pipeline với hai phiên bản hàm chuẩn hóa thời gian. Bản *naive* coi mọi giá trị là mili-giây, bản *drift-aware* tự nhận diện đơn vị.
- **Đo:** % bản ghi có mốc thời gian vô lý (ví dụ năm > 3000), % nến sai, chia theo giai đoạn 2022 và 2026.
- **Kết quả mong đợi:** bản naive hỏng toàn bộ dữ liệu năm 2026, còn bản drift-aware đúng 100%.

### E2 — Đối soát batch vs official

- **Cách làm:** dựng nến từ aggTrades bằng `build_candles`, rồi join với klines chính thức theo `(symbol, minute)`.
- **Đo:** % khớp tuyệt đối cho từng trường O, H, L, C, V và số trades, chia theo cặp và tháng. Mọi nến lệch được gắn nhãn theo taxonomy ở C3.
- **Hình:** heatmap tỷ lệ khớp theo cặp × tháng, kèm bảng phân bố nguyên nhân lệch.

### E3 — Đường cong trade-off watermark

- **Cách làm:** chỉ ghi dữ liệu live vào Kafka một lần, sau đó replay (đặt offset về đầu) với từng cấu hình watermark: 0, 2, 5, 10 và 30 giây.
- **Đo:** % nến khớp official và độ trễ xử lý p50/p95 ở mỗi cấu hình.
- **Hình:** trục X là watermark, trục Y trái là % khớp, trục Y phải là độ trễ p95. Đây là hình thể hiện trọn vẹn bản chất tích hợp batch–stream: càng nhanh thì càng thiếu.

### E4 — Fault injection (chaos test)

| Kịch bản | Cách gây lỗi | Kỳ vọng |
|---|---|---|
| WS ngắt tự nhiên | Binance tự ngắt kết nối sau 24h (theo tài liệu) | Bộ nhận tự nối lại |
| WS ngắt cưỡng bức | Ngắt mạng / kill container bộ nhận | Tự nối lại + backfill |
| Spark driver chết | `docker kill` container Spark | Phục hồi từ checkpoint, không mất/trùng |
| Kafka restart | Restart broker | Không mất bản tin đã ghi |
| Chạy lại batch | Chạy cùng một lô 2 lần | Số dòng & checksum bảng không đổi |

- **Lấp khoảng trống:** gọi REST `/api/v3/aggTrades?fromId=<aggId cuối đã nhận + 1>`.
- **Đo:** thời gian phục hồi, số nến thiếu/trùng trước và sau backfill.
- **Mạng dự phòng:** nếu endpoint chính bị chặn, thử `wss://data-stream.binance.vision` (endpoint chỉ phục vụ market data).

### E5 — Record linkage mờ

- **Cách làm:** ẩn aggId, ghép bản ghi stream với batch theo (giá, khối lượng, |Δt| ≤ δ) với δ ∈ {10ms, 50ms, 100ms, 500ms, 1s}. Dùng aggId làm nhãn đúng.
- **Đo:** Precision, Recall, F1 và số cặp ứng viên, so sánh có/không có blocking.
- **Hình:** đường Precision–Recall theo δ, kèm bảng so sánh chi phí giữa các chiến lược blocking.

### E6 — Truy vấn xấp xỉ

- **Cách làm:** trên ~185M dòng, so sánh `approx_count_distinct` (rsd = 0.01 / 0.05 / 0.1) và `approxQuantile` (relativeError = 0.001 / 0.01 / 0.05) với phép tính chính xác tương ứng.
- **Đo:** sai số tương đối và thời gian chạy.
- **Mở rộng (nếu còn thời gian):** so sánh 1 worker vs 2 worker.

### E7 — Phát hiện bất thường + replay FTX

- **Phương pháp:** modified z-score = 0,6745 · (x − median) / MAD, ngưỡng 3,5 (Iglewicz–Hoaglin), áp lên log-return và khối lượng trong cửa sổ rolling 60 phút.
- **Đánh giá theo sự kiện:**
  - Có bắt được các mốc 8–9/11/2022 (giai đoạn FTX sụp đổ) không.
  - Số báo động/ngày trong giai đoạn yên ắng 6–8/2026 (một proxy cho tỷ lệ báo động giả).
  - Precision/recall chỉ mang tính tương đối vì không có nhãn chuẩn, cần ghi rõ trong báo cáo.
- **Replay FTX qua pipeline streaming:**
  - Đẩy aggTrades 11/2022 vào Kafka ở tốc độ 1×, 10× và 100×. Bộ phát hiện "sống qua" sự kiện FTX theo thời gian thực, nên phần mở rộng "cảnh báo trên stream" hoàn thành với chi phí thấp.
  - **Stress test velocity:** tăng tốc độ replay cho đến khi độ trễ p95 vượt 10 giây, rồi ghi lại throughput tối đa (bản tin/giây). Con số này chứng minh chữ V velocity mạnh hơn lưu lượng live.

---

## Phần C — Lộ trình (4 tuần, co giãn theo hạn nộp)

| Tuần | Việc chính | Đầu ra |
|---|---|---|
| 1 | Hoàn thiện `build_candles` dùng chung + hàm chuẩn hóa thời gian; chạy E1, E2; **khởi động chạy streaming 24h+ sớm nhất có thể** | Bảng E1, heatmap E2, dữ liệu live cho E3/E4 |
| 2 | E3 (replay nhiều watermark), E4 (fault injection, backfill REST), Grafana bản đầu | Đường cong trade-off, timeline sự cố |
| 3 | E5 (linkage), E6 (truy vấn xấp xỉ), E7 + replay FTX + stress test | Đường P–R, bảng sai số, hình FTX |
| 4 | Viết taxonomy sai lệch, chính sách chọn nguồn (C3, C5); chốt hình; viết báo cáo; tập demo | Báo cáo + demo |

### Phân chia 4 mảng song song (gợi ý)

| Mảng | Phụ trách | Thực nghiệm |
|---|---|---|
| 1 | Ingestion batch, checksum, lineage | E1, E2 |
| 2 | Streaming, Kafka, chaos test | E3, E4 |
| 3 | Record linkage, truy vấn xấp xỉ | E5, E6 |
| 4 | Thống kê mô tả, phát hiện bất thường, Grafana | E7 |

### Thứ tự cắt nếu trễ tiến độ

1. E6 (truy vấn xấp xỉ, có thể chỉ giữ 1 cấu hình)
2. E5 (linkage, có thể chỉ giữ 1–2 giá trị δ)
3. Replay 100× và stress test

**Không được cắt:** E2, E3, E4. Đó là xương sống của trọng tâm "tích hợp batch–streaming".

---

## Phần D — Bẫy kỹ thuật cần tránh

- [ ] Dùng `first()` / `last()` trong streaming dẫn đến Open/Close sai. Thay bằng `min_by` / `max_by`.
- [ ] Dùng `double` cho giá/khối lượng khiến tổng volume lệch vài chữ số cuối. Thay bằng `DecimalType`.
- [ ] Quên khai báo tên cột khi đọc CSV Binance (file không có header).
- [ ] Ghi `append` thay vì upsert, khiến dữ liệu trùng khi chạy lại.
- [ ] Không lưu aggId cuối cùng đã nhận, khiến không backfill được khi mất kết nối.
- [ ] Không xử lý ping/pong và ngắt kết nối 24h của WebSocket Binance.
- [ ] Đẩy dữ liệu thô lên GitHub (vi phạm cam kết trong đề xuất).

---

## Phần E — Mẹo demo

Chạy dashboard Grafana live, rồi **rút dây mạng hoặc kill container ngay trước mặt hội đồng**. Sau đó cho thấy hệ thống tự nối lại, backfill khoảng trống, và nến vẫn khớp với official.

Ba mươi giây đó thuyết phục hơn mười slide.