# 09 · Nháp báo cáo C1, C2 và C6 (Mảng 1)

**Phạm vi:** Mảng 1 · Đóng góp C1 (lược đồ chuẩn nhận biết schema drift), C2 ("viết một lần, chạy hai tầng") và C6 (lineage + idempotent)
**Bằng chứng:** `tests/test_timeutil.py`, `tests/test_candles.py`, `results/e1/ablation.csv`, `results/e2/match_by_field.csv`, `results/e4/batch_rerun_{1,2}.txt`, `sql/init.sql`

---

## C1 · Lược đồ chuẩn nhận biết schema drift

### Vấn đề

Binance đổi đơn vị timestamp trong file dữ liệu spot từ **mili-giây** (đến hết 2024) sang **micro-giây** (từ 2025). WebSocket JSON thì vẫn mặc định mili-giây. Vì vậy drift xảy ra theo **hai trục**:

| Trục | Ví dụ |
|---|---|
| Theo thời gian, cùng một nguồn | CSV aggTrades 2022 dùng ms, CSV aggTrades 2026 dùng µs |
| Giữa các nguồn, cùng một thời điểm | Năm 2026: CSV dùng µs, WS JSON dùng ms |

Một quy tắc viết cứng kiểu "trước 2025 là ms" chỉ xử lý được trục thứ nhất, và sẽ sai ngay với WS 2026.

### Giải pháp

`src/bdi/timeutil.py` suy ra đơn vị từ **độ lớn giá trị**, rồi quy mọi mốc thời gian về micro-giây UTC:

| Khoảng giá trị tuyệt đối | Đơn vị | Phép quy đổi |
|---|---|---|
| `[1e9, 1e12)` | giây | × 1 000 000 |
| `[1e12, 1e14)` | mili-giây | × 1 000 |
| `[1e14, 1e17)` | micro-giây | giữ nguyên |
| `≥ 1e17` | nano-giây | ÷ 1 000 |

Các khoảng này không chồng lấn với mọi mốc thời gian hợp lệ từ năm 2001 đến năm 5138, nên quy tắc không phụ thuộc vào năm của dữ liệu. Hàm có hai bản dùng chung một bộ ngưỡng: bản Python (`to_micros`, dùng trong producer) và bản biểu thức Spark (`to_micros_col`, dùng trong batch và streaming).

### Kiểm chứng

**Contract test** (`tests/test_timeutil.py`): 9 trường hợp gồm 3 nguồn (aggTrades CSV, klines CSV, WS JSON) × 2 giai đoạn (2022, 2026), cộng giây và nano-giây. Test kiểm tra thêm ba điều: bản Spark cho cùng kết quả với bản Python, bản Spark nhận được cột kiểu chuỗi, và bản naive chỉ hỏng ở dữ liệu 2026 (đúng như ablation dự đoán).

**Ablation E1** (`experiments/e1_drift_ablation.py`): chạy cùng pipeline với hai hàm chuẩn hóa trên **5 cặp × 4 tháng, 418 156 831 bản ghi, 878 400 nến official** (40/40 dòng đúng quy luật; hình `results/e1/ablation.png`). Kết quả đầy đủ xem ở [05-ket-qua.md](05-ket-qua.md#e1--ablation-schema-drift--chứng-minh-c1) và `results/e1/ablation.csv`.

- Bản **naive** (coi mọi giá trị là ms): 100% bản ghi năm 2026 có mốc vô lý (khoảng năm 58 000), 0% nến khớp official. Dữ liệu 2022 vẫn đúng, nên lỗi này **không lộ ra** nếu chỉ kiểm thử trên dữ liệu cũ.
- Bản **drift-aware**: 0% mốc vô lý và 100% nến khớp official ở mọi kỳ.

### Hạn chế

- Quy tắc dựa trên độ lớn giả định timestamp là epoch dương sau năm 2001. Giá trị nhỏ hơn `1e9` bị từ chối (Python ném lỗi, Spark trả `NULL`) thay vì đoán.
- Nếu Binance đổi sang một đơn vị trùng khoảng giá trị với đơn vị cũ (ví dụ "10 µs"), quy tắc sẽ không phân biệt được. Contract test sẽ là nơi phát hiện sớm.

---

## C2 · "Viết một lần, chạy hai tầng"

### Ý tưởng

Logic dựng nến 1 phút là **một hàm PySpark thuần duy nhất**, `build_candles(df)` trong `src/bdi/candles.py`. Hàm không biết mình đang chạy batch hay streaming:

- Tầng batch (`bdi.batch.build_batch`) gọi hàm trên DataFrame đọc từ silver parquet.
- Tầng streaming (`bdi.stream.stream_candles`) gọi hàm trên DataFrame đọc từ Kafka, sau khi đã `withWatermark("event_time", ...)`.

Nhờ vậy, mọi sai lệch giữa hai tầng chỉ có thể đến từ **dữ liệu đầu vào** (trễ, mất, trùng), không đến từ code. Đây là tiền đề để khung đối soát C3 gắn được nhãn nguyên nhân.

### Ba điểm kỹ thuật

**1. Open/Close dùng `min_by`/`max_by` thay vì `first()`/`last()`**

`first()` và `last()` trả về phần tử đầu/cuối theo thứ tự xử lý, mà thứ tự này không được đảm bảo trong streaming, và cả trong batch sau khi shuffle. Code dùng:

```python
order_key = F.struct(F.col("ts_us"), F.col("agg_id"))
F.min_by("price", order_key).alias("open")
F.max_by("price", order_key).alias("close")
```

Khóa sắp xếp gồm cả `agg_id` để phá thế hòa khi hai giao dịch cùng micro-giây. `test_ohlcv_exact` cố ý xáo trộn thứ tự dòng và chia thành 3 partition; test kiểm tra Close là giao dịch có `agg_id` lớn nhất trong số các giao dịch cùng mốc thời gian cuối.

**2. Giá và khối lượng dùng `DecimalType` thay vì `double`**

Giá và khối lượng luôn được đọc dưới dạng chuỗi rồi cast sang Decimal (`PRICE`, `QTY` là `Decimal(24, 8)`; `VOLUME` là `Decimal(38, 8)`; `QUOTE_VOLUME` là `Decimal(38, 16)`). Với `double`, tổng khối lượng lệch ở vài chữ số cuối (ví dụ `0.7999999999` thay vì `0.8`), và phép so sánh khớp tuyệt đối với klines chính thức sẽ thất bại. `test_volume_is_decimal_not_double` khóa kiểu dữ liệu này.

**3. Số giao dịch = `sum(last_trade_id − first_trade_id + 1)`**

Một bản ghi aggTrade gộp nhiều giao dịch lẻ cùng giá, cùng phía, cùng lệnh taker. Đếm số dòng aggTrade (`n_agg`) sẽ ra con số nhỏ hơn trường "number of trades" của klines. Công thức trên khôi phục đúng số giao dịch lẻ. Test kiểm tra một aggTrade gồm 3 giao dịch lẻ (`first = 14`, `last = 16`) được đếm là 3.

### Kiểm chứng tương đương batch = streaming

`test_batch_equals_streaming` chạy **cùng hàm** trên cùng dữ liệu theo hai cách: batch (`collect()` trực tiếp) và streaming (file source, watermark 0 giây, output mode `update`). Test so sánh từng trường của từng nến, và hai kết quả phải giống hệt nhau.

Trên dữ liệu thật, tương đương này được kiểm chứng ở quy mô lớn hơn:

| Thí nghiệm | Phạm vi | Kết quả |
|---|---|---|
| E2 · Batch vs official | 5 cặp × 4 tháng, 878 400 nến | 100% khớp tuyệt đối O, H, L, C, V, trades |
| E3 · Stream vs official | Cửa sổ 2 giờ, 60 001 bản tin, W = 0/2/5/10/30 giây | 100% khớp ở mọi W (dữ liệu thật, không jitter) |
| C5 · 3 nguồn vs official | 24 giờ, 7 200 nến/nguồn | 100% consistency cho `stream`, `stream:update`, `batch` |
| E7 · Replay FTX 100× | 4 ngày 11/2022 | 5 758/5 758 nến khớp official; cảnh báo stream trùng batch 111/111 |

Kết quả E7 cho thấy nguyên tắc "viết một lần" mở rộng được sang cả bộ phát hiện bất thường, không chỉ riêng việc dựng nến.

### Hạn chế

- Tương đương được kiểm chứng trên **nến hoàn chỉnh** (output mode `append` sau watermark). Ở mode `update`, các phiên bản trung gian của một nến tất nhiên khác nến cuối; C5 dùng chính các phiên bản này để đo time-to-convergence.
- Hàm giả định đầu vào đã qua lược đồ chuẩn C1 và đã loại trùng theo `agg_id`. Nếu bỏ bước loại trùng ở tầng streaming, nến sẽ lệch với nhãn `DUPLICATE` (C3).

---

## C6 · Truy vết nguồn gốc từng cây nến + ghi idempotent

### Lineage

Mỗi nến trong bảng `candles` mang 5 cột lineage (`src/bdi/lineage.py`, `sql/init.sql`):

| Cột | Batch | Official | Stream |
|---|---|---|---|
| `source` | `batch` | `official` | `stream`, `stream:update`, `stream:wm=<W>s`, … |
| `file_checksum` | sha256 của file zip aggTrades (đã đối chiếu `.CHECKSUM` của Binance) | sha256 file klines, hoặc `rest:/api/v3/klines` | `kafka:<topic>` |
| `ingested_at` | lúc ghi | lúc ghi | lúc ghi |
| `watermark_config` | (trống) | (trống) | `5s`, `30s`, … |
| `pipeline_version` | `0.1.0` | `0.1.0` | `0.1.0` |

Ví dụ thật (BTCUSDT, nến mới nhất của mỗi nguồn, truy vấn ngày 11/11/2026):

```text
 source   | minute                 | file_checksum                | watermark_config | pipeline_version
----------+------------------------+------------------------------+------------------+-----------------
 batch    | 2026-10-13 23:59:00+00 | 516831c4abed130f9c734629…    |                  | 0.1.0
 official | 2026-11-11 16:38:00+00 | rest:/api/v3/klines          |                  | 0.1.0
 stream   | 2026-11-11 16:38:00+00 | kafka:binance.aggtrades.live | 5s               | 0.1.0
```

Với lineage, mọi nến lệch đều trả lời được ba câu: nến này đến từ **file nào hoặc topic nào**, được xử lý với **cấu hình watermark nào**, bởi **phiên bản pipeline nào**. Đây cũng là bằng chứng rằng "batch khớp official 100%" (E2) không phải so một nguồn với chính nó: 881 280 nến batch đến từ 22 file aggTrades, còn 922 935 nến official đến từ 21 nguồn klines khác nhau (file tháng/ngày và REST).

### Ghi idempotent

Khóa chính của `candles` là `(symbol, minute, source)`. Mọi lần ghi đều là `INSERT … ON CONFLICT (symbol, minute, source) DO UPDATE` (`src/bdi/sink.py`), từ cả tầng batch lẫn `foreachBatch` của streaming. Hệ quả:

- **Chạy lại batch không sinh dòng trùng.** `chaos.sh batch_rerun` dựng lại XRPUSDT 2026-08 hai lần liên tiếp; vân tay bảng `(số dòng, md5)` không đổi: `(175680, '407f1b6e23e448a86b5a729831c1403c')` ở cả hai lần (`results/e4/batch_rerun_{1,2}.txt`).
- **Streaming đạt hiệu quả tương đương exactly-once.** Producer gửi at-least-once: state `aggId` chỉ được lưu sau khi Kafka ack, nên khi bị kill có thể gửi lại một đoạn ngắn. Spark loại trùng bằng `dropDuplicatesWithinWatermark(symbol, agg_id)`, cộng với checkpoint offset Kafka và upsert theo khóa. E4 chứng minh bằng **0 nến thừa** qua cả 5 sự cố, kể cả kill Spark driver giữa chừng.

### Hạn chế

- Lineage ở mức **nến**, không ở mức từng giao dịch. Muốn truy ngược một nến stream về đúng các offset Kafka thì phải dùng `first_agg_id`/`last_agg_id` cộng với raw archive.
- Upsert ghi đè phiên bản cũ, nên không xem lại được nến "đã từng là gì". Riêng C5 giữ mọi phiên bản trong `candle_versions`; hướng tiếp là Delta Lake time-travel.

---

## Gợi ý đưa vào slide

| Slide | Nội dung | Nguồn |
|---|---|---|
| C1 | Bảng 2 trục drift + bảng ngưỡng đơn vị + 1 dòng kết quả E1 ("naive: 0% nến đúng ở 2026") | Mục C1 ở trên, `results/e1/ablation.csv` |
| C2 | Sơ đồ một hàm, hai tầng + 3 điểm kỹ thuật dạng gạch đầu dòng + bảng tương đương | Mục C2 ở trên |
| A6 (phụ lục) | Bảng lineage 3 dòng thật + vân tay batch_rerun | Mục C6 ở trên |
