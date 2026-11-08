# 05 · Tổng hợp kết quả

**Nhóm 3 · IT5427** · Cập nhật ngày 08/11/2026. Chi tiết từng thí nghiệm nằm trong [docs/experiments/](experiments/); số liệu gốc (CSV) và hình (PNG) nằm trong `results/`.

Xem trực quan:
- **Trang kết quả (web):** https://claude.ai/artifact/HCZmyQry8Uk5BDzHwDsUCP
- **Grafana:** `/d/bdi-results` (kết quả thí nghiệm) và `/d/bdi-live` (luồng live)

## Tóm tắt một trang

| # | Câu hỏi | Kết quả chính | Trạng thái |
|---|---|---|---|
| E1 | Bỏ qua schema drift thì hỏng đến đâu? | Bản naive hỏng **100%** dữ liệu 2026 (0% nến đúng); bản drift-aware đúng **100%** | ✅ (SOL, XRP; chưa đủ 5 cặp) |
| E2 | Batch có khớp official không? | **878 400/878 400** nến khớp tuyệt đối O/H/L/C/V + số trades | ✅ |
| E3 | Watermark đánh đổi thế nào? | Replay 1× trên Azure: 100% khớp ở mọi watermark, p95 tăng 5,70 → 35,09 s. Với jitter 5% tối đa 20 s: khớp **12,20%** ở W = 0 s và **100%** ở W = 30 s | ⚠️ có kết quả; validator hiệu năng chưa đạt |
| E4 | Hệ thống chịu lỗi ra sao? | 4 kịch bản chủ động + máy ngủ 5,7 giờ: **0 nến thiếu/thừa/lệch**; phục hồi 5,7–25,6 s | ✅ |
| E5 | Ghép mờ được không? | Ghép giữ thứ tự: **F1 = 0,995** (P 99,9%, R 99,0%) ở δ = 1 s; blocking giảm số cặp **250–2 750 lần** | ✅ |
| E6 | Truy vấn xấp xỉ có đáng không? | Đếm phân biệt trên 420M giá trị: **nhanh hơn 12,7 lần, sai 0,76%**; phân vị chính xác hết RAM | ✅ (thời gian đo khi máy tải cao) |
| E7 | Phát hiện được FTX không? | 8/11 và 9/11/2022 là **top-2 ngày** của tháng; stream trùng batch **111/111** báo động | ✅ (cần kiểm lại trên LUNA) |
| C5 | Chọn nguồn thế nào? | Cửa sổ 24 giờ: Stream đầy đủ/khớp 100%; 100% nến hội tụ, TTC p50 = **0,0 s**, p95 = **2,9 s** | ⚠️ còn thiếu Batch T+1 |

## Quy mô dữ liệu và hệ thống

| Hạng mục | Giá trị |
|---|---|
| Cặp giao dịch | BTCUSDT, ETHUSDT, BNBUSDT, SOLUSDT, XRPUSDT |
| Kỳ batch | 2022-11 (FTX, timestamp ms) + 2026-06, 2026-07, 2026-08 (timestamp µs) |
| aggTrades batch | **419 835 967 dòng** (418,2M tháng + 1,7M ngày 13/10), 40 file, 39 GB thô, kiểm sha256 100% |
| Nến official | 878 400 phút (+ REST cho các ngày live) |
| Luồng live | 5 cặp WebSocket → Kafka → Spark Structured Streaming → Postgres, chạy từ 13/10 15:07 UTC |
| Lưu lượng live | trung bình 18,6 bản tin/s, p95 58/s |
| Raw archive | 665 255+ bản tin, offset Kafka liên tục, không thiếu/trùng |
| Công nghệ | PySpark 4.0.4, Kafka 3.9 (KRaft), Postgres 16, Grafana 11, Docker Compose |
| Test | **22/22 xanh** ngày 08/11 (contract C1, batch = streaming C2, taxonomy C3, TTC/latency C5, linkage C4, mzscore E7) |

---

## E1 · Ablation schema drift — chứng minh C1

| Cặp | Kỳ | Phiên bản | Bản ghi | % mốc vô lý | % nến đúng |
|---|---|---|---|---|---|
| SOLUSDT | 2022-11 | naive | 7 077 107 | 0% | 100% |
| SOLUSDT | 2022-11 | drift-aware | 7 077 107 | 0% | 100% |
| SOLUSDT | 2026-08 | naive | 4 977 119 | **100%** | **0%** |
| SOLUSDT | 2026-08 | drift-aware | 4 977 119 | 0% | 100% |
| XRPUSDT | 2022-11 | naive | 4 776 769 | 0% | 100% |
| XRPUSDT | 2022-11 | drift-aware | 4 776 769 | 0% | 100% |
| XRPUSDT | 2026-08 | naive | 6 016 728 | **100%** | **0%** |
| XRPUSDT | 2026-08 | drift-aware | 6 016 728 | 0% | 100% |

- Drift xảy ra ở hai chỗ: theo thời gian (CSV 2022 dùng ms, CSV 2026 dùng µs) và **giữa các nguồn** (WS JSON vẫn dùng ms, CSV 2026 dùng µs).
- Một hàm suy đơn vị theo độ lớn giá trị xử lý được cả hai; contract test gồm 3 nguồn × 2 giai đoạn + giây/ns (13 case).
- File: `results/e1/ablation.csv`. Chưa có hình; dùng bảng trên.

## E2 · Batch vs official — chứng minh C2, C3

| Phạm vi | Nến | % khớp O | H | L | C | V | trades | Thiếu |
|---|---|---|---|---|---|---|---|---|
| 5 cặp × 4 tháng | 878 400 | 100 | 100 | 100 | 100 | 100 | 100 | 0 |

- Thời gian dựng batch: ~70 phút cho 418M dòng trên 1 máy 16 GB (local[*]).
- Không nến nào lệch, nên không có nhãn nguyên nhân.
- Câu hỏi mở của C3: official của 5 cặp × 4 tháng **không có phút `trades = 0`**, nên `EMPTY_MINUTE` không xảy ra với các cặp thanh khoản cao này.
- Hình: `results/e2/heatmap_all_match.png` (mọi ô 100%; trên slide nên dùng bảng hoặc con số lớn).

## E3 · Trade-off watermark — chứng minh C2, C3, C5

Lượt chính chạy trên VM Azure 4 vCPU, 16 GiB RAM. Cùng 60.001 bản tin của cửa sổ 08:00–10:00 UTC ngày 07/11/2026 được replay ở tốc độ `1×` cho mọi cấu hình; trigger là 2 giây.

**Watermark trên dữ liệu không tiêm jitter** (585 nến hoàn chỉnh/cấu hình):

| Watermark | % khớp | Độ trễ p50 | Độ trễ p95 |
|---|---|---|---|
| 0 s | 100% | 4,84 s | 5,70 s |
| 2 s | 100% | 6,92 s | 7,51 s |
| 5 s | 100% | 9,23 s | 11,25 s |
| 10 s | 100% | 15,24 s | 15,90 s |
| 30 s | 100% | 34,44 s | 35,09 s |

**Có tiêm độ trễ: 5% bản tin trễ ngẫu nhiên tối đa 20 giây** (590 nến hoàn chỉnh/cấu hình):

| Watermark | % khớp | Bản tin bị loại | Độ trễ p95 |
|---|---|---|---|
| 0 s | **12,20%** | 2.476 | 6,14 s |
| 2 s | 14,75% | 2.157 | 8,06 s |
| 5 s | 18,64% | 1.740 | 10,43 s |
| 10 s | 33,05% | 1.008 | 16,17 s |
| 30 s | **100%** | 0 | 35,24 s |

Diễn giải:
1. Watermark chỉ tiến giữa các micro-batch. Trong lượt baseline, `late_rows_dropped = 0` ở cả năm cấu hình nên mọi watermark đều khớp 100%; khi W tăng, độ trễ tăng gần tuyến tính.
2. Một nến chứa nhiều giao dịch, nên 5% bản tin trễ có thể làm phần lớn nến lệch. Tỷ lệ khớp chỉ trở lại 100% khi watermark lớn hơn mức jitter tối đa 20 giây.
3. Replay lag tối đa chỉ 0,010 giây, nhưng micro-batch p95 là 2.123–2.232 ms, cao hơn trigger 2.000 ms khoảng 6–12%. Validator vì vậy ghi `[invalid]`; kết quả vẫn chứng minh trade-off nhưng không được trình bày như một lượt vượt cổng hiệu năng.

Hình và bảng canonical: `results/e3/tradeoff.png`, `results/e3/tradeoff.csv`. Bản lưu có provenance nằm tại `results/e3-azure-20261108/`.

## E4 · Fault injection — chứng minh C3, C5, C6

| Kịch bản | Thời gian lỗi | Phục hồi | Backfill | Thiếu | Thừa | Lệch |
|---|---|---|---|---|---|---|
| Kill bộ nhận WS | 64 s | 5,7 s | 663 | 0 | 0 | 0 |
| Rút mạng (docker pause) | 91 s | 6,4 s | 1 815 | 0 | 0 | 0 |
| Kill Spark driver | 66 s | 25,6 s | không cần (đọc lại offset Kafka) | 0 | 0 | 0 |
| Restart Kafka | 3 s | 8,7 s | không cần | 0 | 0 | 0 |
| **Sự cố tự nhiên: máy ngủ** | **5,7 giờ** | ~1,5 phút | **260 074** | **0 / 514 phút × 5 cặp** | 0 | 0 |
| Chạy lại batch (XRP 2026-08) | | | | 175 680 dòng, md5 `407f1b6e…` không đổi | | |

- Thiết kế then chốt: sau khi WebSocket kết nối lại, ứng dụng hoàn tất backfill toàn bộ symbol trước khi đọc/xử lý phần tin live đang được thư viện đệm. Watermark của Spark là toàn cục, nên nếu đưa tin live mới vào Kafka trước thì dữ liệu bù cũ có thể bị loại vì trễ.
- Thêm các sự cố ngoài kế hoạch, đều tự phục hồi: 3 lần Binance ngắt vì "Pong timeout" khi máy quá tải; Postgres tự khởi động lại; `raw-archive` chết vì timeout heartbeat.
- Hình: `results/e4/timeline_*.png` (đường stream trùng khít official qua vùng sự cố).

## E5 · Record linkage mờ — chứng minh C4

BTC + ETH, 30 phút, 37 009 bản ghi mỗi phía (stream không mất bản ghi nào). Ghép theo (giá, khối lượng, |Δt| ≤ δ), ẩn `agg_id` và chỉ dùng nó làm nhãn đúng.

| δ | Tham lam P / R / F1 | Giữ thứ tự P / R / F1 |
|---|---|---|
| 10 ms | 0,943 / 0,300 / 0,456 | 0,984 / 0,356 / 0,522 |
| 50 ms | 0,948 / 0,574 / 0,715 | 0,995 / 0,689 / 0,814 |
| 100 ms | 0,951 / 0,669 / 0,785 | 0,995 / 0,802 / 0,888 |
| 500 ms | 0,954 / 0,814 / 0,878 | 0,997 / 0,975 / 0,986 |
| **1 s** | 0,954 / 0,825 / 0,885 | **0,999 / 0,990 / 0,995** |

| Blocking | Số cặp so sánh (δ = 1 s) |
|---|---|
| Không blocking (theo symbol) | 684 870 853 |
| (symbol, giây) | 2 667 016 |
| (symbol, giây, bucket giá) | 1 147 799 |

Phân tích lỗi:
- Độ trễ nhận p50 13 ms, p99 972 ms, nên δ nhỏ thì recall thấp.
- 24,6% Δt âm, tức đồng hồ máy chậm hơn Binance vài chục ms.
- 18,2% bản ghi có bản sao cùng giây, giá và khối lượng; ghép tham lam bị nhầm trong các nhóm này. Ghép giữ thứ tự giảm FP từ 1 482 xuống 29.

Hình: `results/e5/pr_curve.png`, `results/e5/blocking_cost.png`.

## E6 · Truy vấn xấp xỉ

419,8M dòng, theo symbol (máy đang tải cao, load 25–30):

| Phép tính | Chính xác | Xấp xỉ tốt nhất | Sai số |
|---|---|---|---|
| Đếm phân biệt `agg_id` (420M giá trị) | 234,7 s | **18,4 s** (rsd 0,01) | 0,76% |
| Đếm phân biệt `price` (3M giá trị) | 90,0 s | 93,3 s (rsd 0,01) | 1,6% |
| Phân vị notional (USDT) | **OutOfMemoryError** (6 GB) | 101,8 s (relErr 0,001) | thứ hạng 0,047% |

- Xấp xỉ chỉ đáng dùng khi số giá trị phân biệt lớn; với cột ít giá trị phân biệt, đếm chính xác đã rẻ.
- Sai số thứ hạng luôn nằm trong cam kết `relErr`, nhưng sai số **giá trị** có thể rất lớn với dữ liệu đuôi dày (59 042% ở relErr 0,01). Nên dùng relErr ≤ 0,001.

Hình: `results/e6/error_vs_time_local.png`.

## E7 · Phát hiện bất thường + replay FTX

Phương pháp: modified z-score (Iglewicz–Hoaglin), |M| > 3,5, baseline 1 ngày, log(1 + khối lượng), sàn |log-return| ≥ 0,5%.

| Cấu hình | Báo động 8/11 | Báo động 9/11 | Hạng 8/11 | Hạng 9/11 | /ngày 11/2022 | /ngày yên 2026 |
|---|---|---|---|---|---|---|
| Theo plan: 60 phút, không sàn | 197 | 86 | 4 | 29 | 144,2 | 201,8 |
| **Chọn: 1 ngày, sàn 0,5%** | **576** | **207** | **1** | **2** | 60,1 | **9,3** |
| 1 ngày, sàn 1% | 235 | 116 | 1 | 2 | 18,3 | 0,9 |

**Replay FTX qua streaming** (BTCUSDT 6–10/11/2022, 100×, ~41 triệu bản tin):

| Chỉ số | Giá trị |
|---|---|
| Nến stream so với official | **5 758/5 758** khớp tuyệt đối |
| Báo động stream so với batch | **111/111** trùng (0 chỉ có ở stream, 0 chỉ có ở batch) |
| Throughput xử lý (lô 500k) | trung vị 13 879 · tối đa 25 454 bản tin/s, khoảng **750–1 370 lần** lưu lượng live |
| Tải đưa vào tối đa | 26 964 bản tin/s, vượt khả năng xử lý của máy nên job tụt lại |

- Lưu ý trung thực: cửa sổ và sàn được chọn sau khi xem dữ liệu tháng 11/2022, nên cần kiểm lại trên sự kiện khác (LUNA 5/2022).
- Hình: `results/e7/ftx_btc.png`, `results/e7/alerts_per_day_nov22.png`.

## C5 · Chính sách chọn nguồn

Cửa sổ `[2026-11-07T11:34:00Z, 2026-11-08T11:34:00Z)`, 5 cặp × 1.440 phút = 7.200 nến cho mỗi nguồn hiện có và 100.826 phiên bản `stream:update`:

| Nguồn | Completeness | Consistency | Timeliness p50 | p95 |
|---|---|---|---|---|
| `stream` (append, W = 5 s) | 100% | 100% | 11,36 s | 16,73 s |
| `stream:update` (update, W = 30 s) | 100% | 100% | 0,00 s | 2,22 s |
| `official` | 100% | 100% | 486,11 s | 486,87 s |

`official` được REST poller ghi theo chu kỳ đối soát, nên timeliness của nó không đại diện cho kênh thời gian thực. Time-to-convergence của `stream:update`: 100% nến hội tụ; 64,44% có TTC = 0; p50 = 0,0 giây, p95 = 2,9 giây, p99 = 4,8 giây, lớn nhất 8,2 giây. TTC được chặn dưới tại 0 vì định nghĩa bắt đầu từ mốc hết phút.

Kết quả trên đã chốt cho Stream/Official. Batch của ngày 08/11 chỉ có sau T+1; cần nạp hai ngày 07–08/11 và chạy lại đúng cửa sổ trước khi gọi đây là bảng ba nguồn hoàn chỉnh.

| Nhu cầu | Nguồn | Căn cứ |
|---|---|---|
| Dashboard, cảnh báo | Streaming | C5 24 giờ: completeness/consistency 100%, timeliness p50 11,36 s, p95 16,73 s |
| Phân tích, báo cáo sau T+1 | Batch / official | 878 400/878 400 nến khớp (E2) |
| Xung đột 3 nguồn | Official làm nguồn tham chiếu | Nguồn klines do Binance công bố; không gọi là căn cứ pháp lý |
| Sau sự cố WS | Stream (đã backfill), batch kiểm chứng | E4: 0 nến lệch |
| Mạng xấu, nhiều bản tin trễ | Batch | E3 Azure: 5% trễ làm W=0 chỉ còn 12,20%; W=30 đạt 100% |

## C3, C6 · Taxonomy và lineage

- **Taxonomy** (`reconcile/taxonomy.py`): `EMPTY_MINUTE`, `CAPTURE_START` (nhãn mới, phát hiện khi chạy thật), `DISCONNECT_GAP`, `DUPLICATE`, `LATE_BEYOND_WATERMARK`, `NUMERIC_PRECISION`, `UNKNOWN`. Các nến lệch quan sát được:
  - live 13/10: 5 nến `CAPTURE_START`, 0 `UNKNOWN`;
  - E3 jitter: lệch do bản tin trễ, được Spark đếm trong `late_rows_dropped`;
  - E2, E4: không có nến lệch.
- **Lineage**: mỗi nến mang `source`, `file_checksum` (sha256 file zip hoặc `kafka:<topic>` hoặc `rest:/api/v3/klines`), `ingested_at`, `watermark_config`, `pipeline_version`. Khóa idempotent là `(symbol, minute, source)`.

## Các bẫy kỹ thuật phát hiện khi chạy thật

16 bẫy được ghi trong [03-pitfalls.md](03-pitfalls.md). Những bẫy đáng nêu trên slide:

| Bẫy | Hệ quả nếu bỏ qua |
|---|---|
| Backfill xen kẽ với tin live | Dữ liệu bù bị loại vì trễ, nến vẫn thiếu sau sự cố |
| Replay E3 bằng "offset về đầu" | Mọi watermark đều 100%, kết luận sai |
| PySpark trả timestamp theo giờ máy | Nến lệch đúng 7 giờ trong Postgres |
| E5 dùng trade time cho stream | Δt luôn = 0, bài toán thành tầm thường |
| Producer Kafka không có backpressure | `BufferError`, mất 100 000 bản tin |
| Máy ngủ khi chạy thí nghiệm | Replay đứng 5,7 giờ, số đo độ trễ sai |

## Hạn chế cần nêu trong báo cáo

1. E1 mới chạy 2/5 cặp (kết luận khó thay đổi).
2. E3 dùng cửa sổ 2 giờ trên một VM Azure; validator hiệu năng chưa đạt vì micro-batch p95 vượt trigger 6–12%.
3. E6 và stress test đo khi máy tải cao (load 20–48), nên con số thời gian chỉ mang tính tương đối.
4. Tham số E7 được chọn trên chính dữ liệu FTX; chưa kiểm trên sự kiện khác.
5. C5 đã đo 24 giờ cho Stream/Official nhưng chưa có Batch cùng cửa sổ; mọi job chạy trên một máy nên chưa đại diện cho cụm production.
6. Mọi thứ chạy trên **một máy** 16 GB (local[*]); chưa thử cụm nhiều worker.
