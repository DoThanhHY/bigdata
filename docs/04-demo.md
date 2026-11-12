# 04 · Kịch bản demo (khoảng 5 phút)

> Ba mươi giây "rút dây mạng" thuyết phục hơn mười slide.

Demo chạy trên **Demo Console** (`make demo` → http://localhost:8088). Đây là một trang web gom mọi thứ cần cho demo vào một màn hình: trạng thái container, độ trễ, biểu đồ giá stream vs official, lưới đối soát 5 cặp × N phút, nút gây sự cố và nhật ký backfill. Grafana (`/d/bdi-live`, `/d/bdi-results`) vẫn dùng được và có link ở góc trên.

## Chuẩn bị (trước giờ demo ≥ 1 giờ)

- [ ] `make up-all`, pipeline ổn định ≥ 30 phút (lưới đối soát xanh kín cửa sổ 30 phút)
  - Nếu pipeline đã tắt **lâu** (vài ngày trở lên), xem mục "Bật lại sau thời gian dài" bên dưới để tránh backfill hàng chục triệu bản ghi qua REST.
- [ ] `make demo` (chạy trên host, cần `.venv` + biến môi trường từ `.env.example`), mở http://localhost:8088 toàn màn hình
- [ ] Chọn "Thời gian sự cố" = 30 s (tổng thời gian pause = 60 s, vừa đủ để thấy luồng dừng mà không làm chậm buổi demo)
- [ ] Mở sẵn các hình: `results/arch/architecture.png`, `results/e3/tradeoff.png`, `results/c5/ttc_cdf.png`, `results/e7/ftx_btc.png`
- [x] Video dự phòng 90 s: `results/demo/demo_mang2_90s.mp4`
- [ ] Phương án dự phòng nếu mạng hội trường chặn Binance: bật `--fallback` (`wss://data-stream.binance.vision`); nếu vẫn không được thì replay từ archive (`replay.py --from-archive --speed 1`) và trỏ `stream-candles` sang topic replay

## Kịch bản

| Thời gian | Màn hình | Hành động | Nói gì |
|---|---|---|---|
| 0:00 | Demo Console | Chỉ vào dải container xanh và 4 ô số | "Năm cặp tiền đang chạy live: WebSocket → Kafka → Spark → Postgres. Nến stream được ghi sau khoảng 10 giây kể từ lúc hết phút, và **mọi nến đều khớp tuyệt đối** với nến chính thức của Binance." |
| 0:30 | Biểu đồ giá + lưới đối soát | Đổi tab BTC → ETH | "Đường xanh là stream, chấm xám là official. Mỗi ô xanh trong lưới là một nến khớp cả 6 trường O·H·L·C·V·số trade." |
| 1:00 | Khung "Gây sự cố" | Bấm **Rút dây mạng** → xác nhận | "Bây giờ tôi cắt mạng bộ nhận WebSocket." |
| 1:15 | Ô độ trễ chuyển cam "dừng … s"; vùng cam xuất hiện trên biểu đồ | Chờ | "Không còn dữ liệu live. Binance sẽ tự ngắt kết nối phía server." |
| 2:00 | Nhật ký sự cố có 5 dòng mới, ô "Bản ghi backfill" nhảy số | Chỉ vào nhật ký | "Bộ nhận tự nối lại và gọi REST `aggTrades?fromId=` để lấp khoảng trống cho **cả 5 cặp trước khi** đẩy tin live, vì watermark của Spark là toàn cục." |
| 2:45 | Lưới đối soát vẫn xanh, ô "Nến thiếu hoặc lệch" = 0 | Chỉ vào vùng sự cố trên lưới | "Các phút nằm trong sự cố vẫn khớp tuyệt đối. Không mất nến, không trùng nến." |
| 3:15 | Hình E3 | | "Đó là khi mạng ổn định. Khi 5% bản tin đến trễ tối đa 20 giây, watermark 0 s chỉ khớp 12,20%; watermark 30 s khớp 100% nhưng p95 tăng lên 35,24 giây." |
| 4:00 | Hình C5 | | "Vì vậy chính sách chọn nguồn: stream cho độ tươi, batch/official cho độ đúng. 24 giờ đo cho thấy 100% nến hội tụ, TTC p95 = 2,9 giây." |

**Lần tập 11/11/2026 16:28 UTC** (`ws_netcut`, pause 50 s): nối lại sau 1 s kể từ khi unpause, backfill **1 244 bản ghi** cho 5 cặp trong khoảng 1,7 s (BTC 649, ETH 276, BNB 139, XRP 120, SOL 60), lưới đối soát **120/120** nến khớp official, kể cả các phút trong vùng sự cố.

## Các nút trên Demo Console

| Nút | Lệnh thật | Kỳ vọng |
|---|---|---|
| Rút dây mạng | `chaos.sh ws_netcut` (docker pause ws-producer, `DOWN + 30` s) | Tự nối lại + backfill, 0 nến lệch |
| Kill bộ nhận WebSocket | `chaos.sh ws_kill` | Khởi động lại, backfill từ `ingest_state` |
| Kill Spark driver | `chaos.sh spark_kill` | Phục hồi từ checkpoint + offset Kafka (mất khoảng 25 s) |
| Restart Kafka broker | `chaos.sh kafka_restart` | Không mất bản tin đã ghi |

Mỗi lần bấm sẽ ghi mốc `inject`/`recover` vào `chaos_events`, giống hệt khi chạy E4 bằng dòng lệnh. Mỗi lúc chỉ chạy được một kịch bản.

## Bật lại sau thời gian dài

`ws_producer` backfill **không giới hạn** từ aggId cuối trong `ingest_state`. Sau vài ngày hoặc vài tuần tắt máy, việc này có thể kéo hàng chục triệu bản ghi qua REST. Nếu khoảng nghỉ là chủ ý (không phải sự cố cần đo), hãy làm như lần bật lại ngày 11/11/2026:

```sql
BEGIN;
CREATE TABLE ingest_state_bak_<ngày> AS SELECT * FROM ingest_state;
INSERT INTO ws_gaps (symbol, disconnected_at, reconnected_at, last_agg_id, backfilled, reason)
  SELECT symbol, updated_at, now(), last_agg_id, 0, 'paused_by_user' FROM ingest_state;
DELETE FROM ingest_state;
COMMIT;
```

Demo Console tự bỏ qua mọi phút trước lần bật lại gần nhất (và phút đầu tiên chỉ bắt được một phần, tức `CAPTURE_START`), nên lưới đối soát không bị đỏ vì khoảng nghỉ chủ ý.
