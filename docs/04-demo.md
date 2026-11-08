# 04 · Kịch bản demo (khoảng 5 phút)

> Ba mươi giây "rút dây mạng" thuyết phục hơn mười slide.

## Chuẩn bị (trước giờ demo ≥ 1h)

- [ ] Bật `kafka postgres grafana ws-producer stream-candles official-poller`; pipeline ổn định ≥ 30 phút, Grafana ở khung "Last 30 minutes", refresh 5s
- [ ] Terminal 1: `make logs`
- [ ] Terminal 2: sẵn lệnh `experiments/e4_chaos/chaos.sh ws_netcut`
- [ ] Mở sẵn các hình: `results/e2/heatmap_all_match.png`, `results/e3/tradeoff.png`, `results/c5/ttc_cdf.png`, `results/e7/ftx_btc.png`
- [ ] Quay video dự phòng 60–90 giây: dashboard trước sự cố → `ws_netcut` → reconnect/backfill → bảng `ws_gaps` cập nhật
- [ ] Phương án dự phòng nếu mạng hội trường chặn Binance: bật `--fallback`; nếu vẫn không được thì replay từ archive (`replay.py --from-archive --speed 1`) và trỏ `stream-candles` sang topic replay

## Kịch bản

| Thời gian | Hành động | Nói gì |
|---|---|---|
| 0:00 | Grafana: Candlestick, giá Stream/Official và độ tươi | "Một hàm `build_candles` dựng nến OHLCV một phút; Stream được đối soát với Official" |
| 0:45 | Chạy `chaos.sh ws_netcut` | "Giờ cắt mạng bộ nhận" |
| 1:00 | Panel độ tươi tăng dần, số trade/phút của stream tụt | "Không còn dữ liệu live" |
| 2:00 | Container được unpause, log hiện `backfill BTCUSDT: aggId …` | "WebSocket đã nối lại; ứng dụng hoàn tất REST backfill trước khi xử lý phần tin live đang được đệm" |
| 2:30 | Bảng "Khoảng trống WS + backfill" có dòng mới; % khớp official vẫn giữ | "Không mất nến nào" |
| 3:00 | Hình E3 Azure | "Với jitter 5% tối đa 20 giây, W=0 chỉ khớp 12,20%; W=30 khớp 100% nhưng p95 tăng lên 35,24 giây" |
| 4:00 | Hình C5/E7 | "C5 24 giờ cho thấy 100% nến hội tụ, TTC p95 2,9 giây; Batch C5 được bổ sung sau T+1" |

Không nói C5 đã đủ ba nguồn cho tới khi Batch ngày 07–08/11 được nạp và phân tích lại.
