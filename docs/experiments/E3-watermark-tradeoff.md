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

- `results/e3/tradeoff.csv`: `watermark_s`, `pct_match`, `pct_match_trades`, `latency_p50_s`, `latency_p95_s`, `late_rows_dropped`
- `results/e3/tradeoff.png`: hai panel chung trục X (watermark). Panel trên là % khớp, panel dưới là độ trễ p50/p95

**Kỳ vọng:** % khớp tăng rồi bão hòa khi W tăng, độ trễ tăng gần tuyến tính theo W. Đó là trade-off "càng nhanh thì càng thiếu".

## Tiêm độ trễ có kiểm soát

Dữ liệu thật gần như **không** có bản tin trễ (xem kết quả bên dưới), nên để thấy vế "nhanh thì thiếu" của trade-off, replay hỗ trợ làm trễ ngẫu nhiên một phần bản tin:

```bash
JITTER_PCT=5 JITTER_MAX=20 GROUP=3 experiments/e3_watermark_sweep.sh 2026-10-13 2026-10-13T15:15 2026-10-13T15:45
TRIGGER=0 WMS="0 2 5" GROUP=3 experiments/e3_watermark_sweep.sh ...   # micro-batch liên tục
```

Mỗi cấu hình ghi `results/e3/configs/<run_id>.json`; `e3_analyze.py` tự gom thành các series trên hình.

## Kết quả (13–14/10/2026, cửa sổ 15:15–15:45 UTC ngày 13/10, 52 407 bản tin, 5 cặp)

**Độ đảo thứ tự thật của luồng Binance** (đo trên raw archive, chỉ tính bản tin `origin=ws`):

| Chỉ số | Giá trị |
|---|---|
| Độ trễ mạng `recv − T` | p50 = 11 ms, p95 = 329 ms, p99 = 893 ms, max = 4,5 s |
| % bản tin đến sau một bản tin mới hơn | 4,12% |
| Mức "lùi" so với event time lớn nhất đã thấy | p99 = 41 ms, p99.9 = 334 ms, **max = 777 ms**; không bản tin nào > 1 s |

**Trade-off watermark trên dữ liệu thật** (140 nến official / cấu hình):

| Trigger | Watermark | % khớp | Độ trễ p50 | Độ trễ p95 |
|---|---|---|---|---|
| 2 s | 0 s | 100% | 6,2 s | 11,9 s |
| 2 s | 2 s | 100% | 8,4 s | 12,5 s |
| 2 s | 5 s | 100% | 13,2 s | 15,1 s |
| 2 s | 10 s | 100% | 17,2 s | 22,1 s |
| 2 s | 30 s | 100% | 35,0 s | 36,1 s |
| 0 (liên tục) | 0 / 2 / 5 s | 100% | 12,1 / 13,6 / 16,0 s | 15,9 / 18,7 / 28,1 s |

**Diễn giải cho báo cáo:**

1. Watermark chỉ tiến **giữa** các micro-batch, nên độ đảo thứ tự nhỏ hơn thời lượng một micro-batch (~0,5–2 s) không bao giờ bị loại. Luồng Binance lùi tối đa 0,78 s, nên watermark 0 s vẫn khớp 100%.
2. Kiến trúc góp phần: Kafka key theo symbol giữ thứ tự từng cặp, và backfill chạy trước tin live (P1) nên dữ liệu bù không bị coi là trễ.
3. Trên dữ liệu thật, trade-off chỉ còn **độ trễ tăng tuyến tính theo watermark**. Chọn watermark nhỏ (0–2 s) là đủ an toàn.
4. Trigger 0 (liên tục) không nhanh hơn trên máy 16 GB đang chạy song song nhiều job, vì chi phí cố định mỗi micro-batch và tranh CPU.
5. Vế "nhanh thì thiếu" chỉ xuất hiện khi có độ trễ lớn (mạng xấu). Xem kết quả jitter bên dưới.

**Có tiêm độ trễ: 5% bản tin đến muộn ngẫu nhiên tối đa 20 s** (trigger 2 s):

| Watermark | % khớp | Bản tin bị loại vì trễ | Độ trễ p50 | Độ trễ p95 |
|---|---|---|---|---|
| 0 s | **2,1%** | 1 936 | 7,8 s | 15,9 s |
| 2 s | 2,1% | 1 633 | 10,4 s | 17,4 s |
| 5 s | 2,9% | 1 265 | 13,6 s | 18,0 s |
| 10 s | 5,0% | 787 | 14,8 s | 17,5 s |
| 30 s | **100%** | 0 | 35,2 s | 41,1 s |

Mỗi phút có hàng trăm bản tin, nên chỉ cần 5% bị trễ là gần như phút nào cũng mất ít nhất một bản tin, và nến lệch official. % khớp chỉ hồi phục khi watermark lớn hơn độ trễ tối đa (20 s). Đây là hình ảnh đầy đủ của trade-off "nhanh thì thiếu, đủ thì chậm". Nó cũng là lý do tầng batch (C5) cần làm nguồn chuẩn sau T+1.

Lưu ý: lượt jitter W = 0/2/5 s chạy lần đầu bị hỏng vì máy ngủ giữa chừng (P12), nên đã chạy lại. Nhịp replay của các lượt hợp lệ lệch tối đa 0,06 s.

## Phân tích thêm (C3)

```bash
python -m bdi.reconcile.compare --left "stream:wm=0s" --start 2026-10-14T08:00 --end 2026-10-14T10:00 \
    --run-id e3_wm0 --with-late 0
```

Lệnh này cho phân bố nhãn `LATE_BEYOND_WATERMARK` ở watermark 0.

## Checklist

- [x] Raw archive có dữ liệu của ngày chọn
- [x] Official của ngày đó đã nạp (qua `rest_klines`)
- [x] 5 file `results/progress/e3_wm*.jsonl` đều có dữ liệu
- [x] Hình trade-off + bảng (dữ liệu thật, trigger 2 s và 0)
- [x] Kết quả jitter (5% bản tin trễ ≤ 20 s), xem bảng trên
- [ ] Chạy lại trên cửa sổ dài hơn (≥ 2 giờ sôi động, 13:30–15:30 UTC) khi đã có ≥ 24h archive
- [ ] Đọc ra watermark "đủ tốt" (ví dụ ≥ 99.9% khớp) và độ trễ tương ứng, đưa vào kết luận C5
