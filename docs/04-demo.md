# 04 · Kịch bản demo (khoảng 5 phút)

> Ba mươi giây "rút dây mạng" thuyết phục hơn mười slide.

## Chuẩn bị (trước giờ demo ≥ 1h)

- [ ] `make up-all`, pipeline live đã chạy ≥ 1h, Grafana mở sẵn dashboard **BDI** ở khung "Last 30 minutes", refresh 5s
- [ ] Terminal 1: `make logs`
- [ ] Terminal 2: sẵn lệnh `experiments/e4_chaos/chaos.sh ws_netcut`
- [ ] Mở sẵn các hình: `results/e2/heatmap_all_match.png`, `results/e3/tradeoff.png`, `results/e7/ftx_btc.png`
- [ ] Phương án dự phòng nếu mạng hội trường chặn Binance: bật `--fallback`; nếu vẫn không được thì replay từ archive (`replay.py --from-archive --speed 1`) và trỏ `stream-candles` sang topic replay

## Kịch bản

| Thời gian | Hành động | Nói gì |
|---|---|---|
| 0:00 | Grafana: giá 3 nguồn trùng nhau, độ tươi vài giây | "Một hàm `build_candles` chạy cho cả hai tầng" |
| 0:45 | Chạy `chaos.sh ws_netcut` | "Giờ cắt mạng bộ nhận" |
| 1:00 | Panel độ tươi tăng dần, số trade/phút của stream tụt | "Không còn dữ liệu live" |
| 2:00 | Container được unpause, log hiện `backfill BTCUSDT: aggId …` | "Tự nối lại, lấy phần thiếu qua REST `fromId`" |
| 2:30 | Bảng "Khoảng trống WS + backfill" có dòng mới; % khớp official vẫn giữ | "Không mất nến nào" |
| 3:00 | Hình E3 | "Watermark nhanh thì thiếu, chậm thì đủ: đây là bản chất tích hợp batch–stream" |
| 4:00 | Hình E7 FTX | "Bộ phát hiện bắt được 8–9/11/2022 khi replay qua stream" |
