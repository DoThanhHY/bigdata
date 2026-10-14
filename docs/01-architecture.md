# 01 · Kiến trúc và quyết định thiết kế

## Luồng dữ liệu

| Tầng | Vào | Xử lý | Ra |
|---|---|---|---|
| Ingest batch | `data.binance.vision` (zip + `.CHECKSUM`) | `ingest/download.py`: kiểm sha256, giải nén, ghi `data/raw/manifest.jsonl` | CSV trong `data/raw/` |
| Batch | CSV aggTrades | `batch/build_batch.py`: canonical → silver parquet → `build_candles` → lineage | `data/lake/gold/candles_batch` + Postgres `source='batch'` |
| Official | CSV klines 1m | `batch/load_klines.py` | `data/lake/gold/candles_official` + Postgres `source='official'` |
| Ingest stream | WS `<sym>@aggTrade` (combined stream) | `ingest/ws_producer.py`: nối lại, backfill REST, lưu aggId cuối | Kafka `binance.aggtrades.live` (key = symbol) |
| Lưu trữ thô | Kafka | `stream/raw_archive.py` | `data/lake/bronze/raw_aggtrades/date=…` |
| Streaming | Kafka | `stream/stream_candles.py`: canonical → watermark → dedup `(symbol, agg_id)` → `build_candles` → upsert trong `foreachBatch` | Postgres `source='stream'` |
| Official trong ngày | REST `/api/v3/klines` | `ingest/rest_klines.py` | Postgres `source='official'` (file_checksum `rest:…`) |
| Replay | bronze hoặc CSV | `ingest/replay.py` (giữ nhịp, tốc độ k×) | Kafka `binance.aggtrades.replay` |
| Đối soát | Postgres `candles` | `reconcile/compare.py` + `taxonomy.py` | `reconcile_results` + `results/reconcile/` |

## Lược đồ chuẩn (C1)

Mọi nguồn trade được đưa về cùng các cột:

| Cột | Kiểu | Ghi chú |
|---|---|---|
| `symbol` | string | CSV không có cột này, lấy từ manifest |
| `agg_id` | long | Khóa tự nhiên; ở E5 bị ẩn đi và chỉ dùng làm nhãn |
| `price`, `qty` | `Decimal(24,8)` | Đọc dưới dạng chuỗi rồi cast thẳng sang Decimal, không qua double |
| `first_trade_id`, `last_trade_id` | long | Dùng để tính số trade |
| `ts_us` | long | Micro-giây, suy ra đơn vị theo độ lớn (`timeutil.to_micros_col`) |
| `event_time` | timestamp | Dùng cho window và watermark |
| `is_buyer_maker`, `origin` | bool, string | `origin` ∈ `batch_csv`, `ws`, `rest_backfill`, `replay` |

Hiện tượng drift xuất hiện ở hai chỗ, không chỉ theo thời gian:

- **Theo thời gian:** CSV spot trước 2025 dùng mili-giây, từ 2025 dùng micro-giây.
- **Giữa các nguồn:** WS JSON vẫn dùng mili-giây theo mặc định (trừ khi đặt `timeUnit=MICROSECOND`), còn CSV 2026 dùng micro-giây. Một hàm suy luận theo độ lớn giá trị xử lý được cả hai trường hợp. Contract test nằm ở `tests/test_timeutil.py`.

## Postgres (`sql/init.sql`)

| Bảng | Vai trò |
|---|---|
| `candles` | Nến của mọi nguồn. PK `(symbol, minute, source)`, có đủ cột lineage và `emitted_at` |
| `candle_versions` | Mọi phiên bản nến stream ở `update` mode, dùng cho C5 (time-to-convergence) |
| `ingest_state` | aggId cuối đã được Kafka xác nhận, theo từng symbol |
| `ws_gaps` | Mỗi lần mất kết nối: thời điểm, lý do, số bản ghi đã backfill |
| `chaos_events` | Mốc inject/recover do các script E4 ghi |
| `reconcile_results` | Kết quả so khớp từng nến kèm nhãn nguyên nhân |
| `anomalies` | Kết quả E7 |

Giá trị `source` gồm: `official`, `batch`, `stream` (live, watermark 5s), `stream:update` (C5), `stream:wm=<W>s` (E3), `stream:e7_x<k>` (E7).

## Các quyết định thiết kế quan trọng

1. **Backfill toàn bộ symbol trước khi đẩy tin live.** Watermark của Spark là toàn cục cho mọi symbol. Nếu đẩy tin live của BTC trước khi backfill xong ETH, watermark sẽ tiến lên và dữ liệu backfill của ETH bị loại vì trễ. `ws_producer._backfill_all()` chạy ngay sau khi kết nối và xong trước khi xử lý tin live đầu tiên. Nhờ đó dữ liệu cũ luôn vào Kafka trước dữ liệu mới.
2. **Producer at-least-once, loại trùng ở hạ nguồn.** State aggId chỉ được lưu sau khi Kafka đã ack. Nếu bị kill, producer có thể gửi lại một đoạn ngắn (≤ 2s). `dropDuplicatesWithinWatermark(symbol, agg_id)` cộng với upsert theo khóa giúp kết quả cuối không bị trùng.
3. **E3 phải phát lại có nhịp, không đặt offset về đầu.** Khi đọc lại cả topic với tốc độ tối đa, dữ liệu dồn vào vài micro-batch rất lớn. Watermark chỉ tiến giữa các micro-batch, nên gần như không bản tin nào bị coi là trễ, và đường cong E3 sẽ sai. `replay.py --from-archive --speed 1` giữ nguyên thứ tự đến và khoảng cách thời gian thật. Vẫn chỉ cần ghi live một lần, đúng ý của plan.
4. **Các job E3 chạy theo nhóm, mỗi nhóm một lần replay cùng đoạn archive.** Cách này công bằng (mọi cấu hình thấy cùng một chuỗi bản tin). `GROUP=5` cho 1 lượt nếu đủ RAM; máy 16 GB đang chạy luồng live nên mặc định 2.
5. **Độ trễ trong replay được quy về đồng hồ thật** bằng `results/replay/<run_id>.json`: `wall(x) = replay_start + (x − origin) / speed`.
6. **E5 dùng `recv_ts_us` cho phía stream.** Trường `T` của WS trùng khít với CSV (cùng một nguồn), nên Δt = 0 và bài toán trở nên tầm thường. Thời điểm *nhận* chứa nhiễu mạng thật, nên mới đúng là ghép mờ.
7. **Không vẽ biểu đồ hai trục y.** Plan mô tả hình E3 có hai trục Y. Code vẽ hai panel dùng chung trục X (watermark): panel trên là % khớp, panel dưới là độ trễ p50/p95. Hình vẫn thể hiện cùng một trade-off nhưng không gây hiểu nhầm về thang đo. E7 stress test làm tương tự.
8. **Ở update mode, Spark không loại trùng.** Spark chỉ cho phép nối nhiều toán tử có trạng thái ở append mode. Luồng `stream:update` (C5) dựa vào việc producer đã lọc trùng cộng với upsert.
