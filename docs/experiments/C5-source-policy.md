# C5 · Chính sách chọn nguồn dựa trên độ hội tụ

**Phụ trách:** Mảng 2 + viết báo cáo (tuần 4) · Dùng số liệu từ E3 và E4

## Định nghĩa

- **Time-to-convergence (TTC):** thời gian từ lúc một phút kết thúc đến khi nến stream trùng với official *và không đổi nữa*. Tính bằng `metrics.time_to_convergence()` trên bảng `candle_versions`.
- **Completeness:** % phút official mà nguồn có nến.
- **Consistency:** % phút khớp tuyệt đối official.
- **Timeliness:** độ trễ p50/p95 từ lúc phút kết thúc đến lúc nến được ghi.

## Chạy

```bash
docker compose --profile c5 up -d stream-candles-update    # update mode, watermark 30s, chạy ≥ 24h
# ngày hôm sau, khi đã có official + batch của ngày đó:
python experiments/c5_source_policy.py --start 2026-10-14 --end 2026-10-15
```

## Đầu ra

- `results/c5/source_quality.csv`: bảng 3 tiêu chí cho `stream`, `stream:update`, `batch`
- `results/c5/ttc_cdf.png`: CDF của TTC

## Kết quả sơ bộ (14/10/2026, 02:45–05:00 UTC, 5 cặp, official qua REST)

| Nguồn | Completeness | Consistency | Timeliness p50 | Timeliness p95 |
|---|---|---|---|---|
| `stream` (append, W = 5 s) | 100% | 100% | 24,2 s | 359 s |
| `stream:update` (update, W = 30 s) | 100% | 100% | 10,0 s | 303 s |

Time-to-convergence (`stream:update`): **100% nến hội tụ**, p50 = 13,0 s, p95 = 306,6 s (`results/c5/ttc_cdf.png`).

Lưu ý: p95 bị đội lên vì khoảng đo trùng với E4 (kill Spark lúc 02:40, restart Kafka lúc 02:51) và với lúc máy quá tải do replay FTX 100× (load 20–48). Cần đo lại trên 24h live khi máy rảnh. p50 phản ánh hành vi bình thường. Batch của ngày 14/10 chỉ có sau T+1, nên chưa có cột batch.

## Quy tắc chọn nguồn (điền số liệu thật)

| Nhu cầu | Nguồn | Căn cứ |
|---|---|---|
| Dashboard, cảnh báo | Streaming | timeliness p50 = 10–24 s; consistency = 100% (sơ bộ). Có thể xem nến từ ~10 s sau khi phút kết thúc |
| Phân tích sau T+1 | Batch / official | consistency = 100% (E2: 878 400 nến) |
| Ba nguồn xung đột | Official là chuẩn | |
| Sau sự cố WS | Stream vẫn dùng được nhờ backfill; batch là nguồn kiểm chứng | E4: 0 nến thiếu/lệch ở cả 4 kịch bản và khi máy ngủ 5,7 giờ |
| Mạng xấu, nhiều bản tin trễ | Không tin stream cho tới khi qua watermark đủ lớn; dùng batch | E3 jitter: 5% bản tin trễ ≤ 20 s làm khớp chỉ còn 2% ở W = 0 s, 100% ở W = 30 s |
