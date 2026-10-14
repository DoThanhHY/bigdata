# 03 · Bẫy kỹ thuật và chỗ đã xử lý

## Từ plan (Phần D)

| # | Bẫy | Đã xử lý ở đâu | Cách kiểm |
|---|---|---|---|
| D1 | `first()`/`last()` trong streaming làm Open/Close sai | `candles.py`: `min_by`/`max_by` theo `struct(ts_us, agg_id)` | `tests/test_candles.py::test_ohlcv_exact` (dữ liệu bị xáo trộn, hai trade trùng timestamp) |
| D2 | `double` làm volume lệch ở vài chữ số cuối | `schema.py`: đọc chuỗi rồi cast sang `Decimal(24,8)`; volume dùng `Decimal(38,8)`; Postgres dùng `numeric` | `test_volume_is_decimal_not_double` |
| D3 | CSV Binance không có header | `AGGTRADES_CSV_SCHEMA`, `KLINES_CSV_SCHEMA` khai báo tên cột, `header=False` | Chạy E2: nếu thiếu header thì dòng đầu tiên sẽ mất |
| D4 | Ghi `append` gây trùng khi chạy lại | `sink.py`: `INSERT … ON CONFLICT (symbol, minute, source) DO UPDATE` | `chaos.sh batch_rerun` |
| D5 | Không lưu aggId cuối nên không backfill được | `ws_producer.py` + bảng `ingest_state`, chỉ lưu sau khi Kafka ack | `chaos.sh ws_kill` → `ws_gaps.backfilled > 0` |
| D6 | Ping/pong và ngắt kết nối 24h | `websockets` tự trả pong; `ws_max_session_seconds = 85800` chủ động nối lại; 30s không có tin thì coi là chết | Để chạy > 24h, xem `ws_gaps.reason` |
| D7 | Đẩy dữ liệu thô lên GitHub | `.gitignore` chặn `data/`, `*.zip`, `*.csv.gz` | `git status` trước khi commit |

## Phát hiện thêm khi dựng project

| # | Bẫy | Hệ quả nếu bỏ qua | Đã xử lý |
|---|---|---|---|
| P1 | Backfill từng symbol xen kẽ với tin live | Watermark toàn cục tiến lên, dữ liệu backfill bị loại vì trễ, nến sau sự cố vẫn thiếu | Backfill tất cả symbol trước, xem [01-architecture.md](01-architecture.md) §1 |
| P2 | E3 replay bằng cách đặt offset về đầu | Mọi cấu hình watermark khớp gần 100%, đường cong phẳng, kết luận sai | `replay.py` phát lại có nhịp |
| P3 | E5 dùng trade time `T` cho phía stream | Δt luôn bằng 0, P = R = 1 với mọi δ, thí nghiệm vô nghĩa | Dùng `recv_ts_us` |
| P4 | Thay đổi query streaming nhưng giữ checkpoint cũ | Job lỗi khi khởi động hoặc state sai | Mỗi cấu hình dùng một `--run-id`, nên checkpoint riêng |
| P5 | Spark 4 bật ANSI mode | Cast chuỗi lỗi sang Decimal sẽ ném exception thay vì trả NULL | Đúng ý muốn: thà dừng còn hơn âm thầm sai. Nếu gặp, xem dòng lỗi trong CSV |
| P6 | Đồng hồ máy lệch | Độ trễ E3 bị sai | Bật NTP; có thể ghi `recv_ts_us − E` làm chỉ số lệch đồng hồ |
| P7 | `foreachBatch` chạy lại cùng `batch_id` sau sự cố | Ghi trùng | Upsert theo khóa nên idempotent; `candle_versions` cũng có khóa chứa `batch_id` |
| P8 | PySpark `collect`/`foreachPartition` trả timestamp dạng `datetime` không múi giờ theo **giờ máy** (UTC+7) | Nến ghi vào Postgres lệch 7 giờ (phát hiện khi chạy thử 13/10) | `sink._utc_strings()` định dạng chuỗi ISO `Z` trước khi ghi |
| P9 | Executor Spark dùng `python3` hệ thống thay vì venv | `ModuleNotFoundError: psycopg` trong `foreachPartition` | `get_spark()` đặt `PYSPARK_PYTHON = sys.executable` |
| P10 | `foreachPartition` với 16 partition cho vài chục nến | 16 kết nối Postgres mỗi micro-batch, batch 2–5s vượt trigger | `coalesce(1)` trong `foreachBatch`, batch còn 0,7–1s |
| P11 | Phút đầu tiên của luồng thu thập | Thiếu trade vì bắt đầu giữa phút, bị gắn `UNKNOWN` | Nhãn mới `CAPTURE_START` trong `taxonomy.py` |
| P12 | Máy ngủ trong lúc chạy thí nghiệm dài | Replay đứng im hàng giờ rồi dồn dữ liệu một lúc, làm sai toàn bộ số đo độ trễ (đã gặp ở lượt E3 jitter đầu tiên) | Chạy `caffeinate -dims` trong suốt thí nghiệm; kiểm tra dòng `trễ so với nhịp mục tiêu` trong log replay phải luôn ≈ 0 |
| P13 | CPU quá tải (load ≫ số lõi) | Event loop của producer trả pong chậm, Binance đóng kết nối (1008 Pong timeout) | Tự nối lại + backfill nên không mất dữ liệu; tránh chạy quá nhiều job Spark cùng lúc với luồng live |
| P14 | Producer Kafka đẩy nhanh hơn broker nhận (replay 100× trên máy quá tải, hoặc backfill rất lớn) | `BufferError: Local: Queue full`, tiến trình chết, 100 000 bản tin trong hàng đợi bị mất (đã gặp ở E7) | `_produce()` trong `replay.py` và `_send()` trong `ws_producer.py`: bắt `BufferError`, `poll(0.2)` rồi gửi lại |
| P15 | MAD xấp xỉ (median độ lệch quanh median cửa sổ) cần 2W − 1 điểm | Bộ phát hiện trên stream chỉ nạp W + 1 phút lịch sử nên không bao giờ báo động | `detect_for_batch` nạp 2W + 1 phút; replay FTX khởi động từ 6/11 |
| P16 | Postgres rớt kết nối giữa chừng (máy quá tải, restart) | Producer chết với `psycopg.OperationalError` (đã gặp lúc 06:00 ngày 14/10); Docker tự khởi động lại nên không mất dữ liệu, nhưng có một khoảng gián đoạn | `StateStore._retry`: nối lại và thử lại tối đa 5 lần, giãn cách tăng dần |
