# C5 · Cửa sổ 24 giờ ngày 07–08/11/2026

- Cửa sổ UTC: `[2026-11-07T11:34:00Z, 2026-11-08T11:34:00Z)`.
- 5 symbol, 1.440 phút, 7.200 nến cho mỗi nguồn `stream`, `stream:update`, `official`.
- 100.826 phiên bản trong `candle_versions`, trung bình 14,00 phiên bản/nến.
- Reconnect chủ động: 11:08:13–11:08:15 UTC ngày 08/11, backfill 58 bản tin, `reason=proactive_24h`.
- `stream`: completeness 100%, consistency 100%, timeliness p50 11,36 s, p95 16,73 s.
- `stream:update`: completeness 100%, consistency 100%, timeliness p50 0,00 s, p95 2,22 s.
- TTC: 100% hội tụ; 64,44% có TTC = 0; p50 0,0 s, p95 2,9 s, p99 4,8 s, lớn nhất 8,2 s.

TTC/timeliness được chặn dưới tại 0 giây vì định nghĩa bắt đầu từ mốc hết phút. Phiên bản đã ổn định trước mốc đó được tính là 0 giây.

## Trạng thái

Đây là kết quả 24 giờ của Stream/Official. Chưa phải bảng ba nguồn cuối cùng vì archive Batch ngày 08/11 chỉ có sau T+1. Khi Batch có sẵn, nạp hai ngày 07–08/11 và chạy lại đúng cửa sổ trên.

Sau khi cửa sổ kết thúc, một lần chạy Spark test nặng lúc khoảng 12:17 UTC làm checkpoint trên bind mount Windows phát sinh lỗi I/O. Sự cố xảy ra sau `END=11:34 UTC`, không thuộc mẫu C5 ở trên. Toàn bộ Docker Compose sau đó đã được dừng an toàn bằng `stop`; container, volume và checkpoint vẫn được giữ để phục vụ phân tích Batch T+1 và demo.
