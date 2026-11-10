# Demo Mảng 2 · Streaming, Fault-tolerance, C3/C5 & E3/E4

Tài liệu và video demo cho Mảng 2.

## 1. Video demo (`demo_mang2_90s.mp4`)

- **Tên file:** `results/demo/demo_mang2_90s.mp4`
- **Thời lượng:** 01:25 (khoảng 90 giây), độ phân giải Full HD 1080p (1920×1080), 30 fps, dung lượng ~12 MB.
- **Nội dung kịch bản trọng tâm:**
  - `00:00–00:15`: Giới thiệu đề tài và kiểm tra các dịch vụ đang chạy trên Docker (`docker compose ps`).
  - `00:15–00:35`: Dashboard Grafana Live (Candlestick, giá đóng cửa, số giao dịch/phút, độ tươi nến).
  - `00:35–00:50`: Bơm lỗi ngắt mạng bộ nhận WebSocket (`chaos.sh ws_netcut` - pause container 90 giây), quan sát độ tươi tăng dần do không có dữ liệu mới.
  - `00:50–01:10`: Unpause bộ nhận, quan sát log tự động kết nối lại và kích hoạt cơ chế REST Backfill bù đắp toàn bộ giao dịch bị thiếu theo `aggId` trước khi phát tin live.
  - `01:10–01:25`: Dashboard Grafana tự động phục hồi, ghi nhận khoảng trống trong bảng `ws_gaps`, nến tiếp tục khớp 100% với Official.

## 2. Kịch bản trình diễn trước Hội đồng

Xem chi tiết kịch bản thuyết trình và ứng đáp câu hỏi tại [docs/04-demo.md](../../docs/04-demo.md) và [docs/bao-cao-mang-2.md](../../docs/bao-cao-mang-2.md).
