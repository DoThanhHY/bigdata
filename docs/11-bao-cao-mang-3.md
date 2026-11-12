# 11 · Báo cáo Mảng 3: C4 (ghép mờ) và E6 (truy vấn xấp xỉ)

**Phạm vi:** Mảng 3 · Đóng góp C4, thí nghiệm E5 và E6
**Bằng chứng:** `src/bdi/linkage/fuzzy.py`, `experiments/e5_linkage.py`, `experiments/e6_approx.py`, `results/e5/`, `results/e6/`
**Chi tiết số liệu:** [experiments/E5-record-linkage.md](experiments/E5-record-linkage.md), [experiments/E6-approx-query.md](experiments/E6-approx-query.md)

---

## C4 · Record linkage mờ với ground truth miễn phí

### 1. Câu hỏi

Nếu không có khóa chung, có ghép được bản ghi từ luồng WebSocket với bản ghi trong file lịch sử không? Ghép đúng đến mức nào, và tốn bao nhiêu phép so sánh?

### 2. Thiết kế thí nghiệm

Điểm đặc biệt của bài toán: hai phía **đều có** `agg_id` (mã giao dịch gộp do Binance cấp). Nhóm cố ý **giấu** `agg_id` khi ghép, rồi dùng chính nó để chấm điểm. Nhờ vậy có nhãn đúng 100%, điều hiếm có trong các bài toán record linkage thực tế.

| Thành phần | Lựa chọn |
|---|---|
| Phía stream | Raw archive (bronze) của Kafka, thời gian là `recv_ts_us` (lúc **nhận**, có nhiễu mạng thật) |
| Phía batch | aggTrades file ngày 13/10/2026 (silver parquet) |
| Phạm vi | BTCUSDT + ETHUSDT, 30 phút (15:15–15:45 UTC), 37 009 bản ghi mỗi phía |
| Luật so khớp | `price` bằng nhau, `qty` bằng nhau, `|Δt| ≤ δ`, ràng buộc 1–1 |
| δ | 10, 50, 100, 500, 1 000 ms |
| Blocking | `none` (theo symbol), `window` (ts // δ), `second` (theo giây), `second_price` (giây + bucket giá) |
| Thuật toán | **Tham lam:** chọn cặp theo `|Δt|` tăng dần · **Giữ thứ tự:** trong từng nhóm (symbol, giá, khối lượng), đi hai con trỏ theo thời gian |

Lý do dùng thời điểm **nhận** thay vì thời điểm giao dịch: trường `T` của WebSocket trùng khít với CSV (cùng một nguồn gốc), nên nếu dùng `T` thì Δt ≈ 0. Thời điểm nhận chứa độ trễ mạng và lệch đồng hồ thật, nên mới đúng là ghép mờ.

### 3. Kết quả

| δ | Tham lam P / R / F1 | Giữ thứ tự P / R / F1 |
|---|---|---|
| 10 ms | 0,943 / 0,300 / 0,456 | 0,984 / 0,356 / 0,522 |
| 50 ms | 0,948 / 0,574 / 0,715 | 0,995 / 0,689 / 0,814 |
| 100 ms | 0,951 / 0,669 / 0,785 | 0,995 / 0,802 / 0,888 |
| 500 ms | 0,954 / 0,814 / 0,878 | 0,997 / 0,975 / 0,986 |
| **1 s** | 0,954 / 0,825 / 0,885 | **0,999 / 0,990 / 0,995** |

| Blocking | Số cặp so sánh (δ = 1 s) | Giảm so với `none` |
|---|---|---|
| `none` | 684 870 853 | — |
| `second` | 2 667 016 | 257 lần |
| `second_price` | 1 147 799 | 597 lần |

Mọi chiến lược blocking cho **cùng một kết quả ghép**; chỉ khác số phép so sánh.

**Biến thể dùng thời điểm giao dịch** (`--stream-time event`): F1 phẳng theo δ ở mức 0,916 (tham lam) và 0,931 (giữ thứ tự), **thấp hơn** bản dùng thời điểm nhận.

Hình: `results/e5/pr_curve.png`, `results/e5/blocking_cost.png`, `results/e5/pr_curve_event.png`.

### 4. Diễn giải

Ba nguồn lỗi, mỗi nguồn giải thích một phần của bảng kết quả:

1. **Độ trễ nhận** (`recv − T`): p50 = 13 ms, p90 = 203 ms, p99 = 972 ms. δ nhỏ hơn độ trễ thì bản ghi không tìm được cặp, nên recall thấp ở δ = 10 ms và chỉ đạt 99% khi δ = 1 s.
2. **Lệch đồng hồ:** 24,6% Δt **âm**, tức máy "nhận" bản tin trước cả lúc giao dịch xảy ra. Đồng hồ máy chậm hơn Binance vài chục ms. Vì vậy luật so khớp phải dùng `|Δt|`, không được giả định Δt ≥ 0.
3. **Bản ghi trùng hệt:** 18,2% bản ghi có bản sao cùng giây, cùng giá, cùng khối lượng (lệnh nhỏ lặp lại, thường do bot). Ghép tham lam theo `|Δt|` nhầm trong các nhóm này, nên recall kẹt ở 82,5% và có khoảng 1 500 cặp sai.

**Ghép giữ thứ tự** (`_ordered` trong `src/bdi/linkage/fuzzy.py`) khai thác một tính chất của pipeline: Kafka key theo symbol nên thứ tự bản tin của mỗi cặp được giữ nguyên. Trong mỗi nhóm (symbol, giá, khối lượng), bản ghi thứ k bên stream phải ghép với bản ghi thứ k bên batch. Số cặp sai giảm từ 1 482 xuống 29, recall tăng từ 82,5% lên 99,0%.

**Vì sao thời điểm "nhiễu" lại tốt hơn thời điểm "chính xác"?** Trường `T` của WebSocket chỉ có độ phân giải mili-giây, nên 12,87% bản ghi trùng hệt cả bốn trường (cặp, giá, khối lượng, `T`); trong các nhóm này thứ tự bị mất. `recv_ts_us` có độ phân giải micro-giây và mỗi bản tin một thời điểm nhận riêng, nên tỷ lệ trùng là 0%: nó mang theo **thứ tự đến**. Bài học chung cho record linkage: một thuộc tính nhiễu nhưng phân biệt được các bản ghi có thể có giá trị hơn một thuộc tính chính xác nhưng hay trùng.

### 5. Hạn chế

- Chỉ 30 phút, 2 cặp, trong giờ thị trường yên. Ở giai đoạn biến động mạnh (như FTX), mật độ bản ghi trùng hệt và độ trễ nhận đều cao hơn, nên F1 có thể thấp hơn.
- Ghép giữ thứ tự dựa vào giả định Kafka giữ thứ tự theo symbol. Nếu đổi key hoặc tăng partition không theo symbol, giả định này mất.
- Ground truth "miễn phí" là đặc thù của Binance. Trong bài toán thật không có `agg_id`, phải ước lượng P/R bằng mẫu gán nhãn tay.

---

## E6 · Truy vấn xấp xỉ

### 1. Câu hỏi

Trên 420 triệu dòng, truy vấn xấp xỉ (HyperLogLog++ cho đếm phân biệt, Greenwald–Khanna cho phân vị) có đáng dùng không? Đánh đổi giữa sai số và thời gian ra sao?

### 2. Thiết kế thí nghiệm

| Phép tính | Bản chính xác | Bản xấp xỉ | Tham số |
|---|---|---|---|
| Đếm phân biệt theo symbol, cột ít giá trị (`price`, 3,0 triệu) | `countDistinct` | `approx_count_distinct` | rsd ∈ {0,01; 0,05; 0,1} |
| Đếm phân biệt theo symbol, cột nhiều giá trị (`agg_id`, 419,8 triệu) | `countDistinct` | `approx_count_distinct` | rsd ∈ {0,01; 0,05; 0,1} |
| Phân vị [0,5; 0,95; 0,99] của notional = price × qty (USDT), theo symbol | `percentile` | `approxQuantile` | relErr ∈ {0,001; 0,01; 0,05} |

419 835 967 dòng silver trades, `local[*]` trên máy 10 lõi, 16 GB, driver 6 GB. Mỗi phép chạy 3 lần, lấy thời gian tốt nhất.

Thiết kế đã sửa so với plan: plan đếm `price` và lấy phân vị `qty` gộp 5 cặp. Nhưng `price` có ít giá trị phân biệt (đếm chính xác vốn rẻ), và `qty` của các cặp khác đơn vị nên không được gộp. Bản đã sửa thêm `agg_id` (nhiều giá trị phân biệt) và dùng notional theo từng symbol.

### 3. Kết quả (đo lại trên máy rảnh, 12/11/2026)

Lượt đo lại chạy khi đã tắt pipeline live và mọi job khác (`results/e6/approx_idle.csv`, `results/e6/error_vs_time_idle.png`, log tải máy `results/e6/logs/`). Load trung vị 26 lúc đo là do chính job Spark dùng hết 10 lõi, không phải tải từ việc khác. Lượt cũ (máy bận, load 25–30 từ việc khác) giữ ở `results/e6/approx_local.csv` để so.

**Đếm phân biệt:**

| Cột | Số phân biệt | Chính xác | rsd = 0,01 | rsd = 0,05 | rsd = 0,1 |
|---|---|---|---|---|---|
| `price` | 3,0 triệu | **11,3 s** | 14,6 s · sai 1,6% | 15,4 s · 8,5% | 17,1 s · 14,6% |
| `agg_id` | 419,8 triệu | 122,7 s | **14,1 s · sai 0,76%** | 8,2 s · 7,1% | 8,0 s · 11,0% |

**Phân vị notional:**

| Cấu hình | Thời gian | Sai số thứ hạng lớn nhất | Sai số giá trị so với relErr = 0,001 |
|---|---|---|---|
| `percentile` chính xác | **OutOfMemoryError** (lượt cũ, heap 6 GB) | | |
| relErr = 0,001 | 40,4 s | 0,047% | (tham chiếu) |
| relErr = 0,01 | 55,5 s | 1,00% | 59 042% |
| relErr = 0,05 | 39,1 s | 5,00% | 203 712% |

**So với lượt đo khi máy bận:**

| Phép tính | Máy bận | Máy rảnh |
|---|---|---|
| Đếm `agg_id` chính xác → rsd 0,01 | 234,7 s → 18,4 s (**12,7 lần**) | 122,7 s → 14,1 s (**8,7 lần**) |
| Đếm `price` chính xác → rsd 0,01 | 90,0 s → 93,3 s | 11,3 s → 14,6 s |
| Phân vị relErr 0,001 / 0,01 / 0,05 | 101,8 / 316,9 / 334,2 s | 40,4 / 55,5 / 39,1 s |

Sai số không đổi giữa hai lượt (cùng dữ liệu, cùng thuật toán); chỉ thời gian thay đổi.

### 4. Diễn giải

1. **Xấp xỉ chỉ đáng dùng khi số giá trị phân biệt lớn.** Với `agg_id` (420 triệu giá trị), bản chính xác phải giữ cả tập giá trị qua shuffle, còn HyperLogLog++ chỉ giữ một sketch vài KB, nên nhanh hơn **8,7 lần** với sai số 0,76%. Với `price` (3 triệu giá trị), đếm chính xác chỉ là một phép gộp hash rẻ, và **mọi cấu hình xấp xỉ đều chậm hơn** bản chính xác. Lượt đo trên máy rảnh làm kết luận này rõ hơn lượt cũ.
2. **Hệ số tăng tốc thật là 8,7 lần, không phải 12,7 lần.** Con số 12,7 lần của lượt cũ bị phóng đại vì bản chính xác (nặng shuffle) chịu ảnh hưởng của tải máy nhiều hơn bản xấp xỉ. Báo cáo và slide nên dùng **8,7 lần**.
3. **Sai số thực tế so với rsd:** sai số lớn nhất trên 5 cặp bằng 0,76–1,7 lần `rsd`. Điều này đúng tinh thần của rsd: nó là độ lệch chuẩn tương đối, không phải cận cứng.
4. **Phân vị: relErr gần như không ảnh hưởng thời gian.** Ba cấu hình đều mất 39–56 giây, không theo thứ tự relErr. Kỳ vọng trong kế hoạch đo lại ("relErr lớn thì nhanh hơn") **không đạt**: chi phí chủ yếu nằm ở việc quét 420 triệu dòng và tính notional, còn kích thước sketch thì nhỏ ở mọi cấu hình. Vì vậy chọn relErr nhỏ (0,001) gần như **không tốn thêm** thời gian.
5. **Sai số thứ hạng khác sai số giá trị.** Sai số thứ hạng luôn nằm trong cam kết relErr. Nhưng notional có đuôi cực dày: lệch 1% thứ hạng quanh p99 làm giá trị lệch hàng trăm lần (59 042%). Kết luận: với dữ liệu đuôi dày, dùng relErr ≤ 0,001.
6. **Phân vị chính xác không chạy được** trên notional (hết bộ nhớ heap 6 GB). Đây là lý do thực tế, không chỉ lý thuyết, để cần xấp xỉ.

### 5. Hạn chế

- Một máy, `local[*]`. Phần mở rộng so sánh 1 worker và 2 worker (B3.3) chưa làm.
- Lượt OOM của phân vị chính xác là ở lượt cũ; lượt đo lại không chạy `--exact-quantile` để tránh làm treo máy.
- Mỗi cấu hình lấy thời gian tốt nhất trong 3 lần, chưa báo cáo độ phân tán.
