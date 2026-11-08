# E3 Azure · Bản lưu kết quả ngày 08/11/2026

## Phạm vi

- Nguồn: VM Azure `bdi-e3`, thư mục chạy `/mnt/e3-work/e3-azure`.
- Dữ liệu replay: 60.001 bản tin, 08:00–10:00 UTC ngày 07/11/2026, tốc độ `1x`.
- Watermark: 0, 2, 5, 10 và 30 giây.
- Hai series: baseline và jitter 5% bản tin trễ tối đa 20 giây.
- Trigger: 2 giây; tối đa hai Spark job chạy song song.

## Nội dung bản lưu gốc

- `e3/`: bảng `tradeoff.csv`, hình `tradeoff.png`, cấu hình và log.
- `progress/`: 10 file tiến độ micro-batch, một file cho mỗi cấu hình.
- `replay/`: metadata của 6 lượt replay và bản sao metadata theo từng watermark.

Bundle tải từ Azure có tổng cộng 55 file, 12.144.884 byte. Source branch chỉ giữ bảng/hình kết quả, cấu hình và metadata replay cần thiết; log container và progress JSONL dung lượng lớn được giữ local, không commit.

## Kiểm tra chất lượng

- Replay lag tối đa: 0,010 giây.
- Batch p95: 2.123–2.232 ms, vượt trigger 2.000 ms khoảng 6–12%.
- Validator mặc định kết luận `[invalid]` chỉ vì điều kiện batch p95; không thiếu cấu hình, log hay kết quả đối soát.
- Báo cáo phải giữ nguyên hạn chế này, không đổi ngưỡng validator để hợp thức hóa lượt chạy.

## SHA-256

```text
F5965226B0E10C6F9CA17E617CA40FBDCD57181D6A054D274D3352B776083E11  e3/tradeoff.csv
EF19CD0D97E4C3A57D2917C1E5D088BB5D25387B2232AB4E5068C867B41B310D  e3/tradeoff.png
```
