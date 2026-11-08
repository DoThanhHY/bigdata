# C5 · Chính sách chọn nguồn dựa trên độ hội tụ

**Phụ trách:** Mảng 2 + viết báo cáo (tuần 4) · Dùng số liệu từ E3 và E4

## Định nghĩa

- **Time-to-convergence (TTC):** thời gian từ lúc một phút kết thúc đến khi nến stream trùng với official *và không đổi nữa*. Tính bằng `metrics.time_to_convergence()` trên bảng `candle_versions`.
- **Completeness:** % phút official mà nguồn có nến.
- **Consistency:** % phút khớp tuyệt đối official.
- **Timeliness:** độ trễ p50/p95 từ lúc phút kết thúc đến lúc nến được ghi.

## Chạy

```powershell
# Thu thập đã hoàn thành; các lệnh dưới chỉ dùng để tái lập một cửa sổ mới.
docker compose --profile c5 up -d stream-candles-update

# Sau T+1, nạp Batch rồi phân tích lại cửa sổ đã thu; không chạy lại live 24 giờ.
python -m bdi.ingest.download --days 2026-11-07 2026-11-08
python -m bdi.batch.load_klines --periods 2026-11-07 2026-11-08
python -m bdi.batch.build_batch --periods 2026-11-07 2026-11-08
python experiments/c5_source_policy.py `
  --start 2026-11-07T11:34:00Z `
  --end 2026-11-08T11:34:00Z `
  --sources stream stream:update batch
```

## Đầu ra

- `results/c5/source_quality.csv`: bảng completeness/consistency/timeliness; hiện có `stream`, `stream:update`, `official`, sau T+1 sẽ thay bằng lượt phân tích có thêm `batch`
- `results/c5/ttc_cdf.png`: CDF của TTC

## Kết quả 24 giờ (07–08/11/2026, 11:34–11:34 UTC, 5 cặp)

Cửa sổ có đúng 7.200 nến cho mỗi nguồn hiện có và 100.826 phiên bản `stream:update`. Một reconnect chủ động diễn ra lúc 11:08:13 UTC ngày 08/11, kéo dài khoảng 2 giây và backfill đủ 58 bản tin.

| Nguồn | Completeness | Consistency | Timeliness p50 | Timeliness p95 |
|---|---|---|---|---|
| `stream` (append, W = 5 s) | 100% | 100% | 11,36 s | 16,73 s |
| `stream:update` (update, W = 30 s) | 100% | 100% | 0,00 s | 2,22 s |
| `official` (REST poller) | 100% | 100% | 486,11 s | 486,87 s |

Timeliness của `official` phản ánh chu kỳ REST poller dùng để đối soát, không phải SLA của một kênh giao dịch thời gian thực.

Time-to-convergence (`stream:update`): **100% nến hội tụ**; 64,44% có TTC = 0; p50 = 0,0 s, p95 = 2,9 s, p99 = 4,8 s và lớn nhất 8,2 s (`results/c5/ttc_cdf.png`). TTC được chặn dưới tại 0 giây: nến đã ổn định trước hoặc đúng mốc hết phút được tính là hội tụ tại mốc hết phút, không biểu diễn bằng thời gian âm.

Kết quả hiện đã chốt cho Stream/Official. Batch của ngày 08/11 chỉ có sau T+1 nên chưa có cột Batch; khi archive xuất hiện cần nạp hai ngày 07–08/11 và chạy lại cùng cửa sổ trước khi gọi đây là bảng ba nguồn hoàn chỉnh.

## Quy tắc chọn nguồn

| Nhu cầu | Nguồn | Căn cứ |
|---|---|---|
| Dashboard, cảnh báo | Streaming | Cửa sổ 24 giờ: timeliness p50 = 11,36 s, p95 = 16,73 s; consistency = 100% |
| Phân tích sau T+1 | Batch / official | consistency = 100% (E2: 878 400 nến) |
| Ba nguồn xung đột | Official là nguồn tham chiếu do Binance công bố | Không gọi là căn cứ pháp lý |
| Sau sự cố WS | Stream vẫn dùng được nhờ backfill; batch là nguồn kiểm chứng | E4: 0 nến thiếu/lệch ở cả 4 kịch bản và khi máy ngủ 5,7 giờ |
| Mạng xấu, nhiều bản tin trễ | Không tin stream cho tới khi qua watermark đủ lớn; dùng batch | E3 jitter Azure: W = 0 s khớp 12,20%; W = 30 s khớp 100% |
