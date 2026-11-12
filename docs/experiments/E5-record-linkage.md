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

## Biến thể dùng thời điểm giao dịch (`--stream-time event`, 11/11/2026)

Cùng cửa sổ và cùng 37 009 bản ghi, nhưng phía stream dùng trường `T` của WebSocket (thời điểm giao dịch) thay cho thời điểm nhận. Kết quả lưu ở `results/e5/linkage_event.csv` và `results/e5/pr_curve_event.png`.

| δ | Tham lam (P / R / F1) | Giữ thứ tự (P / R / F1) |
|---|---|---|
| 10 ms … 1 s (mọi δ) | 0,967 / 0,869 / **0,916** | 0,931 / 0,931 / **0,931** |

**Kỳ vọng ban đầu sai.** Tài liệu cũ dự đoán P = R = 1, tức bài toán thành tầm thường. Thực tế:

1. **Kết quả phẳng theo δ:** Δt giữa `T` của WebSocket và timestamp của CSV luôn dưới 1 ms, nên chiều thời gian quả thật không còn thông tin phân biệt. Phần này đúng như dự đoán.
2. **Nhưng F1 lại thấp hơn bản dùng thời điểm nhận (0,931 so với 0,995).** Trường `T` của WebSocket chỉ có độ phân giải **mili-giây**, nên **12,87%** bản ghi có bản sao trùng hệt cả bốn trường (cặp, giá, khối lượng, `T`). Trong các nhóm này, thứ tự giữa các bản ghi bị mất, và ghép giữ thứ tự chỉ còn đoán. Với `recv_ts_us` (micro-giây, mỗi bản tin một thời điểm nhận riêng), tỷ lệ trùng là **0%**.
3. **Bài học cho C4:** thời điểm nhận "nhiễu" hơn nhưng mang theo **thứ tự đến**. Vì Kafka key theo symbol giữ đúng thứ tự giao dịch, chính thứ tự này giúp ghép giữ thứ tự đạt F1 0,995. Trong ghép bản ghi, một thuộc tính nhiễu nhưng phân biệt được các bản ghi có thể có giá trị hơn một thuộc tính chính xác nhưng hay trùng.

## Checklist

- [x] Chạy cho BTCUSDT + ETHUSDT, 30 phút
- [x] Hình P–R + bảng chi phí
- [x] Phân tích các ca FP: trade trùng giá và khối lượng trong cùng giây (18,2%); đã khắc phục bằng ghép giữ thứ tự
- [x] (tuỳ chọn) `--stream-time event`: phẳng theo δ nhưng F1 chỉ 0,931 vì 12,87% bản ghi trùng hệt ở độ phân giải ms. `recv` tốt hơn vì giữ thứ tự đến (xem mục trên)
