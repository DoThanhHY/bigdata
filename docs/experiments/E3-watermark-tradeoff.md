# E3 · Đường cong trade-off watermark

**Câu hỏi:** Watermark đánh đổi giữa độ đúng và độ trễ như thế nào?
**Chứng minh cho:** C2, C3, C5 · **Phụ trách:** Mảng 2 · **Không được cắt**

## Thiết kế

- Luồng live đã ghi raw vào `data/lake/bronze/raw_aggtrades/` (service `raw-archive`), nên chỉ cần ghi một lần.
- `replay.py --from-archive --speed 1` phát lại **giữ nguyên thứ tự đến và khoảng cách thời gian thật** vào topic `binance.aggtrades.replay`.
- Các job streaming với watermark ∈ {0, 2, 5, 10, 30}s đọc topic đó (theo nhóm `GROUP` job mỗi lượt replay; mọi lượt phát lại đúng cùng một đoạn archive), mỗi job ghi `source = stream:wm=<W>s`.

> **Không** dùng cách "đặt offset về đầu rồi chạy hết tốc lực", xem [03-pitfalls.md](../03-pitfalls.md) P2.

## Chạy

```bash
# 1) cần official của ngày được replay
python -m bdi.ingest.download --kind klines --days 2026-10-14
python -m bdi.batch.load_klines --periods 2026-10-14

# 2) replay. GROUP = số job song song mỗi lượt (mặc định 2, mỗi job 1GB RAM)
#    5 watermark / GROUP=2 => 3 lượt; mỗi lượt dài bằng cửa sổ replay + ~3 phút
experiments/e3_watermark_sweep.sh 2026-10-14 2026-10-14T13:30 2026-10-14T14:30
#    máy dư RAM (≥ 6GB trống): GROUP=5 => 1 lượt

# 3) phân tích
python experiments/e3_analyze.py
```

Chọn khung giờ **sôi động** (giờ mở cửa Mỹ, khoảng 13:30–15:30 UTC) để có đủ bản tin đến trễ.

## Đầu ra

- `results/e3-azure-20261108/e3/tradeoff.csv`: `watermark_s`, `pct_match`, `pct_match_trades`, `latency_p50_s`, `latency_p95_s`, `late_rows_dropped`
- `results/e3-azure-20261108/e3/tradeoff.png`: hai panel chung trục X (watermark). Panel trên là % khớp, panel dưới là độ trễ p95

**Kỳ vọng:** % khớp tăng rồi bão hòa khi W tăng, độ trễ tăng gần tuyến tính theo W. Đó là trade-off "càng nhanh thì càng thiếu".

## Tiêm độ trễ có kiểm soát

Dữ liệu thật gần như **không** có bản tin trễ (xem kết quả bên dưới), nên để thấy vế "nhanh thì thiếu" của trade-off, replay hỗ trợ làm trễ ngẫu nhiên một phần bản tin:

```bash
JITTER_PCT=5 JITTER_MAX=20 GROUP=3 experiments/e3_watermark_sweep.sh 2026-10-13 2026-10-13T15:15 2026-10-13T15:45
TRIGGER=0 WMS="0 2 5" GROUP=3 experiments/e3_watermark_sweep.sh ...   # micro-batch liên tục
```

Mỗi cấu hình ghi `results/e3/configs/<run_id>.json`; `e3_analyze.py` tự gom thành các series trên hình.

## Kết quả Azure (07–08/11/2026, cửa sổ 08:00–10:00 UTC ngày 07/11, 60.001 bản tin, 5 cặp)

Replay chạy ở tốc độ `1x` trên VM Azure 4 vCPU, 16 GiB RAM. Mỗi cấu hình baseline đối soát 585 nến hoàn chỉnh sau khi bỏ phút biên đầu/cuối.

**Trade-off watermark trên dữ liệu thật:**

| Trigger | Watermark | % khớp | Độ trễ p50 | Độ trễ p95 |
|---|---|---|---|---|
| 2 s | 0 s | 100% | 4,84 s | 5,70 s |
| 2 s | 2 s | 100% | 6,92 s | 7,51 s |
| 2 s | 5 s | 100% | 9,23 s | 11,25 s |
| 2 s | 10 s | 100% | 15,24 s | 15,90 s |
| 2 s | 30 s | 100% | 34,44 s | 35,09 s |

**Diễn giải cho báo cáo:**

1. Watermark chỉ tiến **giữa** các micro-batch. Trên series baseline, cả năm cấu hình đều có `late_rows_dropped = 0`, nên watermark 0 s vẫn khớp 100%.
2. Kiến trúc góp phần: Kafka key theo symbol giữ thứ tự từng cặp, và backfill chạy trước tin live (P1) nên dữ liệu bù không bị coi là trễ.
3. Trên dữ liệu thật, trade-off chỉ còn **độ trễ tăng tuyến tính theo watermark**. Chọn watermark nhỏ (0–2 s) là đủ an toàn.
4. Vế "nhanh thì thiếu" chỉ xuất hiện khi có độ trễ lớn (mạng xấu). Xem kết quả jitter bên dưới.

**Có tiêm độ trễ: 5% bản tin đến muộn ngẫu nhiên tối đa 20 s** (trigger 2 s):

| Watermark | % khớp | Bản tin bị loại vì trễ | Độ trễ p50 | Độ trễ p95 |
|---|---|---|---|---|
| 0 s | **12,20%** | 2.476 | 5,31 s | 6,14 s |
| 2 s | 14,75% | 2.157 | 7,24 s | 8,06 s |
| 5 s | 18,64% | 1.740 | 9,62 s | 10,43 s |
| 10 s | 33,05% | 1.008 | 15,50 s | 16,17 s |
| 30 s | **100%** | 0 | 34,44 s | 35,24 s |

Mỗi phút có nhiều bản tin, nên 5% bản tin bị trễ có thể làm phần lớn nến lệch Official khi watermark nhỏ. Tỷ lệ khớp chỉ hồi phục hoàn toàn khi watermark lớn hơn độ trễ tiêm tối đa 20 giây. Đây là hình ảnh đầy đủ của trade-off "nhanh thì thiếu, đủ thì chậm" và là lý do tầng Batch/Official cần làm nguồn chuẩn sau T+1.

**Giới hạn lượt chạy:** replay lag tối đa chỉ 0,010 giây, nhưng batch p95 nằm trong khoảng 2.123–2.232 ms, cao hơn trigger 2.000 ms khoảng 6–12%. Validator mặc định ghi `[invalid]` vì điều kiện hiệu năng này; bảng vẫn được giữ làm bằng chứng trade-off và không được trình bày như một lượt đã vượt qua cổng hiệu năng.

Kết quả gốc được lưu tại `results/e3-azure-20261108/`; xem `e3/tradeoff.csv`, `e3/tradeoff.png`, 10 file `progress` và 16 file metadata `replay`.

## Phân tích thêm (C3)

```bash
python -m bdi.reconcile.compare --left "stream:wm=0s" --start 2026-10-14T08:00 --end 2026-10-14T10:00 \
    --run-id e3_wm0 --with-late 0
```

Lệnh này cho phân bố nhãn `LATE_BEYOND_WATERMARK` ở watermark 0.

## Checklist

- [x] Raw archive có dữ liệu của ngày chọn
- [x] Official của ngày đó đã nạp (qua `rest_klines`)
- [x] 10 file progress (5 baseline + 5 jitter) đều có dữ liệu trong bản lưu thực nghiệm; log/progress lớn được giữ ngoài source branch
- [x] Hình trade-off + bảng Azure 2 giờ (trigger 2 s)
- [x] Kết quả jitter (5% bản tin trễ ≤ 20 s), xem bảng trên
- [x] Chạy trên cửa sổ dài 2 giờ với replay tốc độ thật
- [x] Đọc ra watermark đạt 100% trong kịch bản jitter: W = 30 s, p95 = 35,24 s
- [ ] Tối ưu micro-batch và chạy lại nếu bắt buộc phải đạt validator `[valid]`
