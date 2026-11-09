# 08 · Công việc còn lại của Mảng 2

**Phạm vi:** Streaming, chịu lỗi · E3, E4, C3, C5 · demo · slide 8–11 và 16

**Ngày lập checklist:** 07/11/2026 · **Cập nhật:** 08/11/2026
**Nguyên tắc:** Không chạy lại thí nghiệm chỉ để thay số đẹp. Giữ nguyên bằng chứng gốc, ghi rõ cửa sổ đo, sự cố mạng và tiêu chí loại mẫu nếu có.

---

## 1. Trạng thái hiện tại

### Đã có kết quả, không bắt buộc chạy lại

- [x] **E3 · Watermark trên dữ liệu thật:** cửa sổ Azure 2 giờ, 60.001 bản tin; W = 0/2/5/10/30 giây đều khớp 100%; bằng chứng tại `results/e3-azure-20261108/`.
- [x] **E3 · Jitter:** 5% bản tin trễ tối đa 20 giây; W = 0 giây khớp 12,20%, W = 30 giây khớp 100%; validator hiệu năng chưa đạt vì batch p95 vượt trigger 6–12%.
- [x] **E4 · Fault injection:** `ws_kill`, `ws_netcut`, `spark_kill`, `kafka_restart`; đã có `results/e4/incidents.csv` và các hình timeline.
- [x] **E4 · Sự cố máy ngủ 5,7 giờ:** backfill 260.074 bản ghi; 514 phút × 5 cặp khớp Official.
- [x] **E4 · Batch idempotent:** số dòng và MD5 không đổi khi chạy lại.
- [x] **C3 · Reconciliation và taxonomy:** đã có `CAPTURE_START`; E2/E4 không có nến lệch; chưa quan sát `UNKNOWN`.
- [x] **C5 24 giờ cho đủ 3 nguồn (`stream`, `stream:update`, `batch`) đối soát với `official`:** 7.200 nến/nguồn, 100.826 phiên bản; 100% completeness, 100% consistency, 100% hội tụ, TTC p50 = 0,0 giây và p95 = 2,9 giây.
- [x] **Nạp Batch T+1 hai ngày 07–08/11:** đã nạp 14.400 nến batch, khớp tuyệt đối 100% với Official.
- [x] **E4 reconnect chủ động trước mốc 24 giờ:** 11:08:13–11:08:15 UTC ngày 08/11, backfill 58 bản tin, `reason = 'proactive_24h'`.
- [x] **Demo trực tiếp & video dự phòng:** Đã quay video Full HD tại `results/demo/demo_mang2_90s.mp4`.
- [x] **Báo cáo Mảng 2:** Đã chốt số liệu C5 24h và đối soát 3 nguồn trong `docs/bao-cao-mang-2.md`.

### Việc còn lại duy nhất

- [ ] **Hoàn thiện slide 8, 9, 10, 11, 16** và tập thuyết trình/phản biện.

---

## 2. Trạng thái vận hành sau khi kết thúc cửa sổ C5

Toàn bộ Docker Compose đã được dừng an toàn bằng `docker compose --profile c5 stop`. Container, volume PostgreSQL/Kafka và checkpoint vẫn được giữ; không dùng `down -v`. Khi chuẩn bị demo, chỉ cần bật lại các service cần thiết.

Trong thời gian đo C5, các service bắt buộc phải giữ `Up` gồm:

Các service cần giữ `Up` trong thời gian đo C5:

- `kafka`
- `postgres`
- `ws-producer`
- `raw-archive`
- `stream-candles`
- `stream-candles-update`
- `official-poller`

`stream-candles` ghi nến cuối với `source='stream'`.

`stream-candles-update` ghi `source='stream:update'` và mọi phiên bản vào `candle_versions`; đây là dữ liệu bắt buộc để tính TTC.

### Theo dõi nếu phải bật lại pipeline

- [ ] Kiểm tra container định kỳ:

  ```powershell
  docker compose --profile c5 ps
  ```

- [ ] Kiểm tra nến mới nhất của ba nguồn:

  ```powershell
  docker compose exec -T postgres psql -U bdi -d bdi -c "SELECT source,symbol,max(minute),count(*) FROM candles WHERE source IN ('stream','stream:update','official') GROUP BY source,symbol ORDER BY source,symbol;"
  ```

- [ ] Kiểm tra log lỗi/reconnect/backfill:

  ```powershell
  docker compose logs --since=30m ws-producer stream-candles stream-candles-update official-poller
  ```

- [ ] Kiểm tra `results/progress/live.jsonl` và `live_update.jsonl`:
  - `late_rows_dropped` không tăng ngoài kịch bản có chủ đích;
  - watermark vẫn tiến;
  - `processed_rows_per_second` theo kịp đầu vào về dài hạn;
  - cảnh báo `falling behind` không đi kèm backlog tăng không giới hạn.

- [ ] Theo dõi dung lượng `data/lake/bronze/raw_aggtrades` và ổ đĩa.

### Ghi nhật ký vận hành

Ghi lại theo UTC:

| Sự kiện | Thời điểm UTC | Ghi chú |
|---|---|---|
| Bắt đầu pipeline | | |
| Chuyển LAN → hotspot | | Có/không có reconnect, số bản ghi backfill |
| Chuyển hotspot → LAN | | Có/không có reconnect, số bản ghi backfill |
| Bắt đầu cửa sổ C5 chính thức | | Chỉ ghi sau khi mạng ổn định và backfill xong |
| Kết thúc cửa sổ C5 | | Phải cách mốc bắt đầu ít nhất 24 giờ |

Lấy thời gian UTC hiện tại:

```powershell
[DateTime]::UtcNow.ToString("yyyy-MM-ddTHH:mm:00")
```

---

## 3. Hoàn thiện C5 · Ưu tiên bắt buộc

### 3.1 Chọn cửa sổ đo

- [x] Chọn cửa sổ ổn định 24 giờ: `[2026-11-07T11:34:00Z, 2026-11-08T11:34:00Z)`.
- [x] Ghi chính xác `START` và `END` theo UTC.
- [x] Không xóa dữ liệu thử nghiệm trước đó; kết quả sơ bộ được giữ tại `results/c5-pre24h-20261027/`.
- [ ] Nếu có mất kết nối trong cửa sổ:
  - không sửa/fake TTC hoặc p95;
  - giữ kết quả toàn kỳ;
  - có thể tính thêm `stable-only` theo một quy tắc định trước và công bố rõ.

Quy tắc `stable-only` đề xuất:

> Loại các nến có phút giao với `[disconnected_at, reconnected_at + 2 phút]` trong `ws_gaps`.

### 3.2 Kiểm tra dữ liệu sau 24 giờ

- [x] `stream` và `stream:update` đều có đủ 5 symbol, 7.200 nến/nguồn.
- [x] `candle_versions` có 100.826 phiên bản cho 7.200 nến, trung bình 14,00 phiên bản/nến.
- [x] `official` bao phủ toàn bộ cửa sổ với 7.200 nến.
- [x] Kiểm tra `ws_gaps`: cửa sổ chỉ chứa reconnect chủ động khoảng 2 giây, đã backfill đủ.
- [x] Kiểm tra lần reconnect chủ động:

  ```sql
  SELECT symbol, disconnected_at, reconnected_at, backfilled, reason
  FROM ws_gaps
  WHERE reason = 'proactive_24h'
  ORDER BY disconnected_at;
  ```

### 3.3 Nạp Batch/Official sau T+1

Xác định tất cả ngày UTC giao với cửa sổ 24 giờ. Nếu cửa sổ qua hai ngày, truyền cả hai ngày:

```powershell
python -m bdi.ingest.download --days 2026-11-07 2026-11-08
python -m bdi.batch.load_klines --periods 2026-11-07 2026-11-08
python -m bdi.batch.build_batch --periods 2026-11-07 2026-11-08
```

- [x] File tải về vượt qua kiểm tra checksum.
- [x] Nến `batch` và `official` đã có trong Postgres cho toàn bộ cửa sổ.
- [x] Không dùng thời điểm ghi Batch T+1 để diễn giải như độ trễ thời gian thực.

### 3.4 Chạy phân tích C5

```powershell
python experiments/c5_source_policy.py `
  --start 2026-11-07T11:34:00Z `
  --end 2026-11-08T11:34:00Z `
  --sources stream stream:update batch
```

Đầu ra bắt buộc:

- [x] `results/c5/source_quality.csv` có đủ ba nguồn.
- [x] `results/c5/ttc.csv.gz` có 7.200 nến và `ttc_s`.
- [x] `results/c5/ttc_cdf.png` được dựng lại cho cửa sổ ổn định 24 giờ.
- [x] Ghi lại:
  - completeness từng nguồn;
  - consistency từng nguồn;
  - timeliness p50/p95;
  - tỷ lệ hội tụ;
  - TTC p50/p95;
  - số phút/symbol quan sát;
  - các sự cố nằm trong cửa sổ.

### 3.5 Phân tích kết quả trung thực

- [x] Báo cáo kết quả **toàn kỳ** trước: 100% hội tụ; TTC p50 = 0,0 s, p95 = 2,9 s, p99 = 4,8 s.
- [ ] Nếu có `stable-only`, trình bày như phân tích phụ, kèm quy tắc loại và số mẫu bị loại.
- [x] Không nói “mọi nến hội tụ sau p50”. Cách nói đúng:

  > X% nến cuối cùng hội tụ; trung vị thời gian hội tụ là Y giây, p95 là Z giây.

- [x] Đã giữ và giải thích reconnect chủ động/backfill; không loại mẫu khỏi kết quả toàn kỳ.

---

## 4. E3 · Đã chạy cửa sổ Azure 2 giờ

- [x] Xác nhận `results/e3-azure-20261108/e3/tradeoff.csv` chứa đủ các series chính:
  - dữ liệu thật, trigger 2 giây;
  - jitter 5% tối đa 20 giây.
- [x] Xác nhận `tradeoff.png` là **hai panel dùng chung trục X**, không gọi là “biểu đồ hai trục Y”.
- [ ] Chuẩn bị giải thích:
  - watermark khác trigger;
  - vì sao W = 0 giây vẫn khớp 100% trên dữ liệu thật;
  - vì sao 5% bản tin trễ làm 87,80% nến không khớp ở W = 0 giây;
  - vì sao replay phải giữ nhịp, không đọc Kafka hết tốc lực.
- [x] Đã chạy cửa sổ 2 giờ ở tốc độ thật; kết quả có đủ bảng/hình/log. Validator hiệu năng còn cảnh báo batch p95 vượt trigger 6–12%, đã ghi rõ trong báo cáo.

---

## 5. E4 · Chỉ bổ sung bằng chứng còn thiếu

- [ ] Không chạy lại bốn chaos scenario nếu không có yêu cầu; kết quả cũ đã đầy đủ.
- [x] Đã kiểm tra `proactive_24h`: 5 symbol, 58 bản tin backfill, không thiếu nến trong cửa sổ.
- [ ] Giữ lại log chuyển LAN/hotspot như một tình huống vận hành thực tế, nhưng không thay thế bốn kịch bản E4 đã có.
- [ ] Kiểm tra `results/e4/incidents.csv` khớp tài liệu:
  - `ws_kill`: phục hồi 5,7 giây, backfill 663;
  - `ws_netcut`: phục hồi 6,4 giây, backfill 1.815;
  - `spark_kill`: phục hồi 25,6 giây;
  - `kafka_restart`: phục hồi 8,7 giây;
  - tất cả có 0 nến thiếu/thừa/lệch.
- [ ] Chuẩn bị giải thích **backfill all symbols before live** và watermark toàn cục.
- [ ] Chuẩn bị giải thích ngữ nghĩa giao nhận:
  - producer at-least-once;
  - dedup theo `(symbol, agg_id)`;
  - Spark checkpoint + Kafka offset;
  - Postgres upsert theo `(symbol, minute, source)`;
  - kết quả hiệu quả tương đương exactly-once tại tầng nến, không tuyên bố exactly-once tuyệt đối end-to-end.

---

## 6. C3 · Kiểm tra nội dung báo cáo

- [x] Mô tả đúng grain đối soát: `(symbol, minute)` giữa hai `source`.
- [x] Nêu rõ so khớp tuyệt đối `open`, `high`, `low`, `close`, `volume`, `trades`.
- [x] Giữ đúng thứ tự ưu tiên taxonomy trong code:
  - `EMPTY_MINUTE`;
  - `CAPTURE_START`;
  - `DISCONNECT_GAP`;
  - `DUPLICATE`;
  - `LATE_BEYOND_WATERMARK`;
  - `NUMERIC_PRECISION`;
  - `UNKNOWN`.
- [x] Phân biệt “nhãn có trong taxonomy” với “nhãn đã quan sát ngoài thực tế”.
- [x] Nêu rõ `simulate_late()` chỉ là mô phỏng theo từng bản tin và là cận trên; watermark thật cập nhật giữa các micro-batch.

---

## 7. Chỉnh `docs/bao-cao-mang-2.md`

- [x] Cập nhật C5 bằng số liệu 24 giờ cuối cùng; kết quả sơ bộ chỉ còn trong thư mục lưu riêng.
- [x] Đổi “đồ thị hai trục” thành “hai panel dùng chung trục watermark”.
- [x] Sửa phần rate limit cho đúng code hiện tại:
  - code xử lý HTTP 418/429;
  - chờ theo `Retry-After`;
  - chưa đọc `X-MBX-USED-WEIGHT`;
  - chưa có exponential backoff riêng cho REST backfill;
  - không khẳng định “không bao giờ bị khóa IP”.
- [x] Đổi “Official là căn cứ pháp lý” thành “Official là nguồn tham chiếu do Binance công bố”.
- [x] Tránh phát biểu tuyệt đối “W = 0 không bao giờ loại dữ liệu”; giới hạn kết luận trong cửa sổ và cấu hình đã đo.
- [x] Diễn đạt đúng backfill:
  - WebSocket đã kết nối và có thể đệm tin;
  - ứng dụng hoàn tất backfill trước khi đọc/xử lý tin live.
- [x] Ghi rõ các hạn chế:
  - E3 chính dùng cửa sổ 2 giờ nhưng validator hiệu năng chưa đạt;
  - C5 chạy trên một máy;
  - các lần mất mạng/quá tải trong cửa sổ;
  - chưa thử cụm nhiều worker.

---

## 8. Slide và demo

### Slide cần hoàn thiện

- [x] **Slide 8:** E3 dữ liệu thật; cả 5 watermark đều khớp 100%; p95 tăng 5,70 → 35,09 giây.
- [x] **Slide 9:** E3 jitter; 12,20% → 100%; thông điệp “nhanh thì thiếu, đủ thì chậm”.
- [ ] **Slide 10:** E4; bảng phục hồi và 0 nến sai.
- [ ] **Slide 11:** ba bước demo trực tiếp.
- [ ] **Slide 16:** C5; ma trận chọn Stream/Batch/Official và TTC cuối cùng.

### Demo trực tiếp

- [ ] Pipeline chạy ổn định ít nhất 30–60 phút trước demo.
- [ ] Grafana `/d/bdi-live` mở sẵn, refresh 5–10 giây.
- [ ] Terminal log mở sẵn:

  ```powershell
  docker compose logs -f --tail=30 ws-producer stream-candles
  ```

- [ ] Terminal chaos chuẩn bị sẵn `ws_netcut`.
- [ ] Kiểm tra mạng hội trường; chuẩn bị `--fallback` nếu Binance WS bị chặn.
- [ ] Quay video dự phòng 60–90 giây:
  - Grafana trước sự cố;
  - `ws_netcut`;
  - reconnect và backfill;
  - bảng `ws_gaps` có dòng mới;
  - Stream tiếp tục hội tụ với Official.
- [ ] Tập demo ít nhất hai lần và đo đúng thời lượng 3–5 phút.

---

## 9. Câu hỏi phản biện phải trả lời được

- [ ] Watermark khác trigger như thế nào?
- [ ] Tại sao W = 0 giây vẫn đúng 100% trong E3 dữ liệu thật?
- [ ] Tại sao chỉ 5% bản tin trễ có thể làm 87,80% nến không khớp ở W = 0 giây?
- [ ] Tại sao không đặt watermark 10 phút?
- [ ] Vì sao phải backfill tất cả symbol trước tin live?
- [ ] Nếu Kafka/Spark/Postgres restart thì dữ liệu phục hồi từ đâu?
- [ ] Hệ thống là at-least-once hay exactly-once?
- [ ] TTC khác timeliness của nến cuối như thế nào?
- [ ] Vì sao main stream không đủ để tính TTC?
- [ ] Tại sao `stream:update` không dedup trong Spark?
- [ ] Khi nào dùng Stream, Batch và Official?
- [ ] Kết quả nào là dữ liệu thật, kết quả nào có jitter/chaos nhân tạo?
- [ ] Những hạn chế nào khiến kết quả chưa thể suy rộng cho cụm production?

---

## 10. Hoàn tất và bàn giao

- [x] Chạy test: **22/22 pass** ngày 08/11/2026 (PySpark cần chạy ngoài sandbox Windows để tạo Java gateway):

  ```powershell
  python -m pytest
  ```

- [x] Dashboard JSON đã sinh lại thành công và đã chạy `publish_results.py` cập nhật bảng `exp_c5` (3 nguồn) vào PostgreSQL:

  ```powershell
  python experiments/publish_results.py
  python grafana/build_results_dashboard.py
  ```

- [ ] Kiểm tra Git, không commit `data/`, checkpoint, raw CSV/ZIP hoặc thông tin nhạy cảm.
- [ ] Rà soát các thay đổi chưa commit và chỉ đưa vào commit các file thuộc phạm vi Mảng 2.
- [ ] Sau khi kết thúc thí nghiệm, khôi phục cấu hình đóng nắp/Sleep của Windows.
- [ ] Dừng pipeline an toàn khi không còn cần thu dữ liệu:

  ```powershell
  docker compose --profile c5 stop
  ```

### Tiêu chí Mảng 2 hoàn thành

Mảng 2 chỉ được coi là hoàn thành khi có đủ:

1. Kết quả E3/E4/C3 cũ được kiểm tra và diễn giải đúng;
2. C5 có cửa sổ 24 giờ và TTC; sau khi archive T+1 xuất hiện phải bổ sung dòng Batch để đủ ba nguồn;
3. Báo cáo cập nhật số cuối cùng, không có phát biểu vượt quá code/bằng chứng;
4. Slide 8–11, 16 hoàn chỉnh;
5. Demo trực tiếp đã tập và có video dự phòng;
6. Có thể trả lời các câu hỏi phản biện ở mục 9.
