# E4 · Fault injection (chaos test)

**Câu hỏi:** Hệ thống chịu lỗi ra sao?
**Chứng minh cho:** C3, C5, C6 · **Phụ trách:** Mảng 2 · **Không được cắt**

## Kịch bản

| Kịch bản | Lệnh | Kỳ vọng | Cơ chế trong code |
|---|---|---|---|
| WS ngắt tự nhiên | để producer chạy > 24h | Tự nối lại | `ws_max_session_seconds`, vòng `run()` |
| WS bị kill | `experiments/e4_chaos/chaos.sh ws_kill` | Tự nối lại + backfill, không thiếu nến | `_backfill_all()` + `ingest_state` |
| Rút dây mạng | `chaos.sh ws_netcut` (docker pause) | Như trên | timeout 30s, ping 20s/60s |
| Spark driver chết | `chaos.sh spark_kill` | Phục hồi từ checkpoint, không mất và không trùng | checkpoint + upsert |
| Kafka restart | `chaos.sh kafka_restart` | Không mất bản tin đã ghi | `acks=all`, idempotence, Spark đọc lại offset |
| Chạy lại batch | `chaos.sh batch_rerun` | Số dòng và checksum bảng không đổi | `ON CONFLICT DO UPDATE` |
| Tất cả (cách nhau 10') | `chaos.sh all` | | |

Biến môi trường: `DOWN=60` (số giây gây lỗi), `SYMBOL`/`PERIOD` cho `batch_rerun`.

## Chạy

```bash
make up-all                                # pipeline live phải đang chạy
experiments/e4_chaos/chaos.sh all          # khoảng 45 phút
experiments/e4_chaos/chaos.sh batch_rerun
# chờ Binance công bố klines ngày đó (T+1)
python -m bdi.ingest.download --kind klines --days <ngày>
python -m bdi.batch.load_klines --periods <ngày>
python experiments/e4_analyze.py
```

## Đầu ra

- `results/e4/incidents.csv`: `recovery_s`, `missing_minutes`, `extra_trades_minutes`, `wrong_minutes`, `backfilled_records`
- `results/e4/timeline_*.png`: số trade/phút của stream và official, vùng tô là sự cố
- `results/e4/batch_rerun_{1,2}.txt`: (số dòng, md5) phải giống nhau

## Lưu ý

- Lấp khoảng trống: `GET /api/v3/aggTrades?fromId=<aggId cuối + 1>` (`ingest/backfill.py`)
- Nếu `missing_minutes > 0` sau `ws_kill`: kiểm tra `ws_gaps.backfilled` và log producer. Nếu backfill có chạy mà nến vẫn thiếu, khả năng cao là dữ liệu backfill bị coi là trễ. Dùng `compare --with-late 5` để xác nhận.
- Ghi lại một sự cố **trước khi sửa** (nếu có) làm ví dụ "trước/sau" cho báo cáo

## Kết quả: sự cố chủ động (14/10/2026, `chaos.sh all` + `batch_rerun`)

Đối soát mỗi sự cố trong cửa sổ [inject − 5 phút, recover + 10 phút] với official (REST klines):

| Kịch bản | Thời gian lỗi | Phục hồi (`recovery_s`) | Backfill | Phút official | Thiếu | Thừa | Lệch |
|---|---|---|---|---|---|---|---|
| `ws_kill` (docker kill) | 64 s | 5,7 s | 663 | 80 | **0** | **0** | **0** |
| `ws_netcut` (docker pause) | 91 s | 6,4 s | 1 815 | 80 | **0** | **0** | **0** |
| `spark_kill` | 66 s | 25,6 s (gồm khởi động JVM + tải checkpoint) | không cần (Spark đọc lại offset Kafka) | 85 | **0** | **0** | **0** |
| `kafka_restart` | 3 s | 8,7 s | không cần | 65 | **0** | **0** | **0** |
| `batch_rerun` (XRPUSDT 2026-08 × 2) | | | | | 175 680 dòng, md5 `407f1b6e…` **không đổi** | | |

- `recovery_s` là thời gian từ lúc khôi phục đến khi có nến stream mới đầu tiên được ghi.
- Ngoài kịch bản, lúc 02:34:46 có thêm một lần `recv_timeout` (30 s không có tin, cả 5 cặp). Producer tự nối lại và backfill 430 bản ghi.
- Hình: `results/e4/timeline_*.png`. Số trade/phút của stream trùng khít official, kể cả trong vùng sự cố.

## Kết quả: sự cố tự nhiên (13–14/10/2026)

**Máy Mac ngủ ~5,7 giờ** (khoảng 19:12 → 00:53 UTC): mọi tiến trình và VM Docker bị treo. Khi máy thức dậy:

| Bước | Quan sát |
|---|---|
| Phát hiện | 30 s không có tin → `recv_timeout`, nối lại sau 1 s |
| Backfill | 260 074 bản ghi qua REST `fromId` (BTC 107 084, ETH 78 768, SOL 36 162, BNB 22 092, XRP 15 968), xong trong khoảng 1,5 phút |
| Ngắt tiếp theo | 3 lần Binance đóng kết nối vì "Pong timeout" (1008) khi máy quá tải (load ~50); mỗi lần tự nối lại và backfill vài chục bản ghi |
| **Đối soát** | `compare --left stream --start 2026-10-13T16:40 --end 2026-10-14T01:15 --run-id e4_sleep`: **514/514 phút × 5 cặp khớp tuyệt đối official**, 0 nến thiếu |

Kết luận: backfill chạy trước tin live (P1) giữ watermark đứng yên trong lúc mất kết nối, nên toàn bộ dữ liệu bù được dựng nến đúng, kể cả sau khoảng trống 6 giờ.

## Checklist

- [x] 4 kịch bản streaming + batch_rerun: 0 nến thiếu/thừa/lệch; batch idempotent
- [x] Bảng incidents + timeline
- [x] Sự cố tự nhiên: máy ngủ 5,7 giờ + 3 lần Pong timeout. Phục hồi 100% (xem trên)
- [ ] Ít nhất một lần ngắt tự nhiên do mốc 24h (`reason = proactive_24h`) trong `ws_gaps`
