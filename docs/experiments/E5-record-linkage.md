# E5 · Record linkage mờ

**Câu hỏi:** Có ghép được bản ghi stream với batch khi không dùng khóa không?
**Chứng minh cho:** C4 · **Phụ trách:** Mảng 3

## Thiết kế

- Phía stream: bản tin từ raw archive, thời gian là **`recv_ts_us`** (lúc nhận, có nhiễu mạng).
- Phía batch: aggTrades file theo ngày, lấy từ silver parquet.
- Ẩn `agg_id`, ghép theo `price ==`, `qty ==`, `|Δt| ≤ δ`. Ràng buộc 1-1, chọn tham lam theo |Δt| tăng dần.
- `agg_id` chỉ dùng làm nhãn đúng để tính P, R, F1.
- δ ∈ {10, 50, 100, 500, 1000} ms.
- Chiến lược blocking:

| Chiến lược | Khóa | Ghi chú |
|---|---|---|
| `none` | symbol | Tích Đề-các; số cặp tính giải tích |
| `window` | symbol, ts // δ | |
| `second` | symbol, giây | |
| `second_price` | symbol, giây, ⌊price / bucket⌋ | `--price-bucket` |

## Chạy

```bash
python -m bdi.ingest.download --kind aggTrades --days 2026-10-14
python -m bdi.batch.build_batch --periods 2026-10-14 --no-db
python experiments/e5_linkage.py --date 2026-10-14 --start 2026-10-14T09:00 --minutes 30
```

## Đầu ra

- `results/e5/linkage.csv`: `delta_ms`, `strategy`, `pairs_compared`, `seconds`, `tp`, `fp`, `fn`, `precision`, `recall`, `f1`
- `results/e5/pr_curve.png`: đường Precision–Recall theo δ
- `results/e5/blocking_cost.png`: số cặp so sánh theo δ cho từng chiến lược (thang log)

**Kỳ vọng:** δ nhỏ thì recall thấp (độ trễ mạng lớn hơn δ). δ lớn thì precision giảm ở các giây sôi động, nơi nhiều trade cùng giá và cùng khối lượng. Blocking giảm số cặp nhiều bậc mà kết quả ghép không đổi.

## Kết quả (14/10/2026, BTC + ETH, 15:15–15:45 UTC ngày 13/10)

Phía stream có **37 009/37 009** bản ghi của batch (không mất bản ghi nào trong 30 phút).

| δ | Tham lam (P / R / F1) | Giữ thứ tự (P / R / F1) |
|---|---|---|
| 10 ms | 0,943 / 0,300 / 0,456 | 0,984 / 0,356 / 0,522 |
| 50 ms | 0,948 / 0,574 / 0,715 | 0,995 / 0,689 / 0,814 |
| 100 ms | 0,951 / 0,669 / 0,785 | 0,995 / 0,802 / 0,888 |
| 500 ms | 0,954 / 0,814 / 0,878 | 0,997 / 0,975 / 0,986 |
| **1 s** | 0,954 / 0,825 / 0,885 | **0,999 / 0,990 / 0,995** |

Chi phí so sánh (số cặp ứng viên), cùng kết quả ghép với mọi chiến lược blocking:

| Blocking | δ = 10 ms | δ = 1 s |
|---|---|---|
| `none` (tích Đề-các theo symbol) | 684 870 853 | 684 870 853 |
| `window` (ts // δ) | 248 720 | 2 667 016 |
| `second` | 2 667 016 | 2 667 016 |
| `second_price` | 1 147 799 | 1 147 799 |

Blocking giảm số cặp phải so **250–2 750 lần** mà không đổi kết quả ghép.

**Phân tích lỗi:**

1. **Độ trễ nhận** `recv − trade time`: p50 = 13 ms, p90 = 203 ms, p99 = 972 ms, max = 4,5 s. Vì vậy δ nhỏ thì recall thấp.
2. **24,6% Δt âm**: máy nhận bản tin "trước" thời điểm giao dịch, tức đồng hồ máy (VM Docker) chậm hơn Binance vài chục ms. Đây là bằng chứng cho P6 (lệch đồng hồ).
3. **Bản ghi trùng hệt**: 18,2% bản ghi có bản sao cùng giây, cùng giá, cùng khối lượng (lệnh nhỏ lặp lại). Ghép tham lam theo |Δt| bị nhầm trong các nhóm này, nên recall dừng ở 82,5% và FP khoảng 1 500.
4. **Cải tiến "giữ thứ tự"** (`method="ordered"`): trong mỗi nhóm (symbol, giá, khối lượng), đi hai con trỏ theo thời gian. Cách này dựa vào việc Kafka key theo symbol giữ thứ tự giao dịch. FP giảm từ 1 482 xuống 29, recall tăng từ 82,5% lên 99,0%.

## Checklist

- [x] Chạy cho BTCUSDT + ETHUSDT, 30 phút
- [x] Hình P–R + bảng chi phí
- [x] Phân tích các ca FP: trade trùng giá và khối lượng trong cùng giây (18,2%); đã khắc phục bằng ghép giữ thứ tự
- [ ] (tuỳ chọn) `--stream-time event` để cho thấy bài toán thành tầm thường, chứng minh vì sao phải dùng `recv`
