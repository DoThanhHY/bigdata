# 10 · Nội dung slide chi tiết

**Dùng cùng:** [06-dan-y-slide.md](06-dan-y-slide.md) (dàn ý + quy ước trình bày), [05-ket-qua.md](05-ket-qua.md) (số liệu gốc), [04-demo.md](04-demo.md) (kịch bản demo).
**Cập nhật:** 12/11/2026 · E1 đủ 5 cặp × 4 tháng · E6 đo lại máy rảnh · sơ đồ kiến trúc `results/arch/architecture.png` · Demo Console `make demo`.

Mỗi slide gồm 4 phần:

- **Trên slide:** chữ đặt đúng như trên slide (tiêu đề là một câu khẳng định).
- **Hình:** file trong `results/` hoặc mô tả hình cần dựng.
- **Lời nói:** đoạn để đọc hoặc nói theo, khoảng 120–150 từ/phút.
- **Thời gian** và người nói.

Tổng: 19 slide chính (≈ 17 phút) + demo 4 phút + 6 slide phụ lục.

---

## Slide 1 · Tiêu đề

**Trên slide**

> # Tích hợp batch và streaming dữ liệu giao dịch Binance
> IT5427 · Tích hợp và xử lý dữ liệu lớn · Nhóm 3
> Vũ Tuấn Minh · Đỗ Xuân Thanh · Đào Quang Huy · Trần Anh Vũ
>
> `420 triệu giao dịch · 3 nguồn · 5 cặp · 878 400 nến · 7 thí nghiệm`

**Lời nói**
"Kính chào hội đồng. Nhóm 3 trình bày đề tài tích hợp batch và streaming trên dữ liệu giao dịch Binance. Điểm chính của nhóm: mỗi đóng góp đều có một thí nghiệm chứng minh bằng số liệu."

**Thời gian:** 20 s · Mảng 1

---

## Slide 2 · Bài toán tích hợp dữ liệu

**Trên slide**

> ## Một sự thật, ba nguồn, không nguồn nào giống nguồn nào
>
> | | CSV aggTrades | CSV klines (official) | WebSocket |
> |---|---|---|---|
> | Đến | T+1, theo file tháng/ngày | T+1 | Real-time, ~19 bản tin/s |
> | Mức chi tiết | Từng giao dịch gộp | Nến 1 phút | Từng giao dịch gộp |
> | Định dạng | CSV không header | CSV không header | JSON, tên trường 1 chữ |
> | Timestamp | ms (2022) → **µs (2026)** | ms → **µs** | **ms** |
> | Lỗi đặc trưng | — | — | trễ, mất, trùng, ngắt sau 24 h |
>
> **Câu hỏi:** dựng cùng một nến OHLCV 1 phút từ cả ba nguồn, **khớp tuyệt đối từng chữ số**, và biết **khi nào tin nguồn nào**.

**Hình:** 3 khối nguồn bên trái → 1 khối "nến 1 phút (symbol, minute)" bên phải.

**Lời nói**
"Bài toán tích hợp ở đây là: cùng một sự thật — giá và khối lượng của từng phút — đến từ ba nguồn khác nhau về thời điểm đến, mức chi tiết, định dạng và cả đơn vị thời gian. Nguồn lịch sử đến sau một ngày nhưng đầy đủ. Nguồn WebSocket đến ngay nhưng có thể trễ, mất hoặc trùng. Nhóm muốn hợp nhất chúng thành một bảng nến duy nhất, và khớp ở đây là khớp tuyệt đối từng chữ số, không phải gần đúng."

**Thời gian:** 1 phút · Mảng 1

---

## Slide 3 · Sáu vấn đề tích hợp cụ thể

**Trên slide**

> ## Bài toán tách thành 6 vấn đề kinh điển của tích hợp dữ liệu
>
> | Vấn đề tích hợp | Biểu hiện trong project | Lời giải | Bằng chứng |
> |---|---|---|---|
> | **Dị thể lược đồ & ngữ nghĩa** | 3 định dạng; timestamp ms/µs đổi theo thời gian *và* giữa nguồn | Lược đồ chuẩn, suy đơn vị theo độ lớn (C1) | E1 |
> | **Hợp nhất xử lý batch + stream** | Hai tầng dễ cho hai kết quả | Một hàm `build_candles` cho cả hai tầng (C2) | E2, E7 |
> | **Đối soát & xung đột dữ liệu** | Stream lệch official thì vì sao? | So 3 nguồn + nhãn nguyên nhân (C3) | E2–E4 |
> | **Ghép bản ghi (entity resolution)** | Không có khóa chung thì ghép thế nào? | Ghép mờ (giá, khối lượng, Δt) có ground truth (C4) | E5 |
> | **Chất lượng & chọn nguồn** | Tươi hay đúng? | Chỉ số time-to-convergence (C5) | E3, C5 |
> | **Lineage & chịu lỗi** | Chạy lại, sự cố, trùng lặp | Lineage từng nến + upsert idempotent (C6) | E4 |

**Lời nói**
"Nếu tách ra, bài toán gồm sáu vấn đề quen thuộc của môn học. Một: dị thể lược đồ và ngữ nghĩa, nặng nhất là timestamp đổi đơn vị. Hai: hợp nhất hai tầng xử lý batch và stream. Ba: khi các nguồn xung đột thì giải thích vì sao. Bốn: ghép bản ghi khi không có khóa. Năm: chọn nguồn theo chất lượng. Sáu: truy vết và chịu lỗi. Mỗi vấn đề ứng với một đóng góp từ C1 đến C6, và mỗi đóng góp có ít nhất một thí nghiệm."

**Thời gian:** 1 phút · Mảng 1

---

## Slide 4 · Dữ liệu & thống kê mô tả

**Trên slide**

> ## 420 triệu giao dịch gộp, hai giai đoạn đối lập
>
> **419 835 967** dòng aggTrades · **1,049 tỷ** giao dịch khớp lệnh · **400 tỷ USDT** · **39 GB** CSV, kiểm sha256 100%
>
> | | 11/2022 (FTX, ms) | 6–8/2026 (yên, µs) |
> |---|---|---|
> | aggTrades | 214,3 triệu (1 tháng) | 203,9 triệu (3 tháng) |
> | Khối lượng | 209,7 tỷ USDT | 190,6 tỷ USDT |
> | BTC trade/phút (p50) | 3 718 | 1 577 |
>
> Live: 5 cặp WebSocket · ~18,6 bản tin/s · p95 58/s

**Hình:** `results/desc/daily_quote_volume.png` (nhỏ, góc phải).

**Lời nói**
"Dữ liệu batch gồm 5 cặp trong 4 tháng: tháng 11/2022 có vụ sụp đổ FTX, và ba tháng hè 2026 thị trường yên. Một tháng FTX có số giao dịch bằng cả ba tháng 2026 cộng lại. Tổng 420 triệu dòng, gấp 2,3 lần ước tính ban đầu, và 39 GB, vượt RAM của máy nên Spark phải xử lý ngoài bộ nhớ."

**Thời gian:** 45 s · Mảng 4

---

## Slide 5 · Kiến trúc

**Trên slide**

> ## Một hàm dựng nến, hai tầng xử lý

**Hình:** `results/arch/architecture.png` (toàn slide). Khối cam `build_candles()` là nơi hai tầng gặp nhau.

Chân slide: PySpark 4.0 · Kafka 3.9 KRaft · Postgres 16 · Grafana 11 · Docker Compose

**Lời nói**
"Tầng batch tải file từ data.binance.vision, kiểm checksum, đưa về lược đồ chuẩn rồi dựng nến. Tầng streaming nhận WebSocket, đẩy vào Kafka, Spark Structured Streaming áp watermark và loại trùng, rồi dựng nến. Điểm mấu chốt là khối màu cam: cả hai tầng gọi **cùng một hàm** `build_candles`. Mọi nến được upsert vào Postgres theo khóa symbol, phút, nguồn, kèm thông tin truy vết. Từ đó nhóm đối soát ba nguồn, đo độ hội tụ và hiển thị trên Grafana."

**Thời gian:** 1 phút · Mảng 1

---

## Slide 6 · Sáu đóng góp

**Trên slide**

> ## Mỗi đóng góp có thí nghiệm chứng minh bằng số
>
> | Đóng góp | Thí nghiệm | Con số |
> |---|---|---|
> | C1 Lược đồ nhận biết drift | E1 | naive: **0%** nến 2026 đúng · drift-aware: **100%** |
> | C2 Viết một lần, chạy hai tầng | E2, E7 | **878 400/878 400** nến · **111/111** báo động |
> | C3 Đối soát + nhãn nguyên nhân | E2–E4 | 7 nhãn · **0** `UNKNOWN` |
> | C4 Ghép mờ có ground truth | E5 | **F1 = 0,995** |
> | C5 Chọn nguồn theo độ hội tụ | E3, C5 | **100%** hội tụ · TTC p95 **2,9 s** |
> | C6 Lineage + idempotent | E4 | **0** nến lệch qua 5 sự cố |

**Lời nói**
"Sáu đóng góp và con số chứng minh. Nhóm sẽ đi lần lượt qua từng thí nghiệm."

**Thời gian:** 40 s · Mảng 1

---

## Slide 7 · E1: Schema drift

**Trên slide**

> ## Bỏ qua schema drift thì mất toàn bộ dữ liệu 2026
>
> | | 2022 (ms) | 2026 (µs) |
> |---|---|---|
> | Naive (luôn coi là ms) | 100% nến đúng | **0%** · 100% mốc thời gian vô lý (năm ~58 000) |
> | Drift-aware (suy theo độ lớn) | 100% | **100%** |
>
> `~1,6·10¹² → ms   ~1,6·10¹⁵ → µs   ~1,6·10¹⁸ → ns`
> 5 cặp × 4 tháng · 418 triệu bản ghi · contract test 3 nguồn × 2 giai đoạn
>
> *Chứng minh cho C1*

**Hình:** `results/e1/ablation.png` (2 panel: % nến khớp, % mốc vô lý).

**Lời nói**
"Binance đổi timestamp từ mili-giây sang micro-giây từ năm 2025. Nếu code viết cứng là mili-giây, dữ liệu 2022 vẫn đúng, nên kiểm thử trên dữ liệu cũ sẽ không phát hiện lỗi; nhưng toàn bộ dữ liệu 2026 nhảy lên năm 58 nghìn và không nến nào đúng. Điểm thú vị hơn: drift còn xảy ra **giữa các nguồn** — CSV 2026 dùng micro-giây, còn WebSocket vẫn mili-giây. Vì vậy nhóm không viết cứng theo năm, mà suy đơn vị từ độ lớn giá trị. Kết quả: đúng 100% trên cả 5 cặp, 4 tháng."

**Thời gian:** 1 phút · Mảng 1

---

## Slide 8 · E2: Batch vs official

**Trên slide**

> ## Batch tái tạo chính xác 100% nến chính thức của Binance
>
> # 878 400 / 878 400
> nến khớp tuyệt đối O·H·L·C·V·trades · 5 cặp × 4 tháng
>
> Ba điểm kỹ thuật:
> 1. `min_by / max_by(price, struct(ts, agg_id))` thay `first / last`
> 2. `Decimal` thay `double`
> 3. `trades = Σ(last_id − first_id + 1)`
>
> *Chứng minh cho C2*

**Lời nói**
"Batch dựng nến từ aggTrades, official là file klines do Binance công bố riêng. Cả 878 400 nến khớp tuyệt đối. Con số này đạt được nhờ ba điểm. `first` và `last` không đảm bảo thứ tự sau shuffle, nên nhóm dùng `min_by`, `max_by` theo thời gian rồi đến agg_id. `double` làm tổng khối lượng lệch ở chữ số cuối, nên nhóm dùng Decimal. Và một aggTrade gộp nhiều giao dịch lẻ, nên số trade phải tính bằng hiệu ID cộng một. Test `test_candles.py` cố ý xáo trộn thứ tự dòng để kiểm chứng."

**Thời gian:** 1 phút · Mảng 1

---

## Slide 9 · E3: Dữ liệu thật, không tiêm jitter

**Trên slide**

> ## Mạng ổn định: watermark chỉ đổi độ trễ, không đổi độ đúng
>
> Replay 1× · 60 001 bản tin thật · 585 nến/cấu hình · `late_rows_dropped = 0`
>
> | Watermark | % khớp official | Độ trễ p95 |
> |---|---|---|
> | 0 s | 100% | 5,70 s |
> | 2 s | 100% | 7,51 s |
> | 5 s | 100% | 11,25 s |
> | 10 s | 100% | 15,90 s |
> | 30 s | 100% | 35,09 s |

**Lời nói**
"Thí nghiệm E3 phát lại cùng 2 giờ dữ liệu thật với 5 cấu hình watermark trên VM Azure. Khi mạng ổn định, không bản tin nào bị loại, nên mọi cấu hình đều khớp 100%, và watermark chỉ làm tăng độ trễ gần như tuyến tính. Lưu ý: validator hiệu năng ghi nhận micro-batch p95 vượt trigger 2 giây khoảng 6–12%, nên nhóm không tuyên bố lượt này đạt cổng hiệu năng."

**Thời gian:** 1 phút · Mảng 2

---

## Slide 10 · E3: Khi mạng xấu

**Trên slide**

> ## Chỉ 5% bản tin trễ làm 87,8% nến sai: nhanh thì thiếu, đủ thì chậm
>
> 5% bản tin trễ ngẫu nhiên tối đa 20 s
> W = 0 s → **12,20%** khớp · p95 6,14 s
> W = 30 s → **100%** khớp · p95 35,24 s
>
> *Chứng minh cho C2, C3, C5*

**Hình:** `results/e3/tradeoff.png` (2 panel chung trục watermark).

**Lời nói**
"Dữ liệu thật quá sạch để thấy trade-off, nên nhóm tiêm độ trễ có kiểm soát vào bản phát lại: 5% bản tin trễ tối đa 20 giây. Vì mỗi nến chứa hàng trăm giao dịch, chỉ cần một bản tin trễ là nến sai, nên ở watermark 0 giây chỉ 12% nến khớp. Phải đến 30 giây, lớn hơn mức trễ tối đa, mới khớp 100%, nhưng độ trễ tăng lên 35 giây. Đây là bản chất của tích hợp batch–stream: càng nhanh thì càng thiếu. Và đó là lý do batch phải là nguồn chuẩn sau T+1."

**Thời gian:** 1,5 phút · Mảng 2

---

## Slide 11 · E4: Chịu lỗi

**Trên slide**

> ## 5 sự cố, 0 nến sai
>
> | Kịch bản | Lỗi | Phục hồi | Backfill | Thiếu/thừa/lệch |
> |---|---|---|---|---|
> | Kill bộ nhận WS | 64 s | 5,7 s | 663 | **0 / 0 / 0** |
> | Rút mạng | 91 s | 6,4 s | 1 815 | **0 / 0 / 0** |
> | Kill Spark driver | 66 s | 25,6 s | offset Kafka | **0 / 0 / 0** |
> | Restart Kafka | 3 s | 8,7 s | không cần | **0 / 0 / 0** |
> | **Máy ngủ 5,7 giờ** | 5,7 h | ~1,5 phút | **260 074** | **0** / 514 phút × 5 cặp |
>
> Backfill **tất cả** cặp trước khi đẩy tin live, vì watermark là toàn cục.
>
> *Chứng minh cho C3, C6*

**Hình:** `results/e4/timeline_01_ws_netcut.png` (nhỏ).

**Lời nói**
"Nhóm gây bốn loại sự cố có chủ đích, và gặp thêm một sự cố thật: máy ngủ 5,7 giờ. Trong mọi trường hợp: không thiếu, không thừa, không lệch nến nào. Một quyết định thiết kế quan trọng: khi nối lại, bộ nhận phải backfill **tất cả** cặp trước khi đẩy tin live. Watermark của Spark là toàn cục; nếu tin live của BTC vào trước, watermark tiến lên và dữ liệu bù của ETH bị loại vì trễ. Lỗi này chỉ lộ ra khi chạy thật."

**Thời gian:** 1,5 phút · Mảng 2

---

## Slide 12 · DEMO

**Trên slide**

> ## Rút dây mạng ngay bây giờ
> 1. Live: 5 cặp, lưới đối soát xanh
> 2. Bấm **Rút dây mạng** → luồng dừng
> 3. Tự nối lại · backfill · nến vẫn khớp official

**Màn hình:** Demo Console http://localhost:8088 (`make demo`). Kịch bản từng giây: [04-demo.md](04-demo.md).

**Lời nói (tóm tắt)**
"Đây là hệ thống đang chạy live. Mỗi ô xanh là một nến stream khớp tuyệt đối với official. Bây giờ tôi cắt mạng bộ nhận… độ trễ chuyển cam, luồng dừng… bộ nhận đã tự nối lại và backfill cho cả 5 cặp… và các ô trong vùng sự cố vẫn xanh: không mất nến nào."

**Kết quả lần tập 11/11:** pause 50 s → backfill 1 244 bản ghi trong ~1,7 s → 120/120 nến khớp.
**Dự phòng:** video `results/demo/demo_mang2_90s.mp4`.

**Thời gian:** 3–4 phút · Mảng 2

---

## Slide 13 · E5: Ghép mờ

**Trên slide**

> ## Không cần khóa vẫn ghép đúng 99,9%
>
> Ẩn `agg_id`, ghép stream ↔ batch theo (giá, khối lượng, |Δt| ≤ δ); `agg_id` chỉ dùng để chấm điểm
>
> | Cách ghép (δ = 1 s) | P | R | F1 |
> |---|---|---|---|
> | Tham lam | 0,954 | 0,825 | 0,885 |
> | **Giữ thứ tự** | **0,999** | **0,990** | **0,995** |
>
> Blocking (symbol, giây, bucket giá): **685 triệu → 1,1 triệu** cặp so sánh, kết quả không đổi
>
> *Chứng minh cho C4*

**Hình:** `results/e5/pr_curve.png`.

**Lời nói**
"Hiếm bài toán ghép bản ghi nào có nhãn đúng sạch như ở đây: nhóm cố ý giấu agg_id rồi ghép mờ, sau đó dùng agg_id để chấm điểm. Ghép tham lam bị kẹt ở F1 0,885 vì 18% giao dịch trùng hệt nhau về giây, giá và khối lượng. Ghép giữ thứ tự đạt F1 0,995. Blocking giảm số cặp so sánh 600 lần mà không đổi kết quả. Phát hiện phụ: 25% Δt âm, nghĩa là đồng hồ máy lệch so với Binance."

**Thời gian:** 1,5 phút · Mảng 3

---

## Slide 14 · E6: Truy vấn xấp xỉ

**Trên slide**

> ## Xấp xỉ nhanh gấp 8,7 lần, nhưng chỉ khi số giá trị phân biệt lớn
>
> 420 triệu dòng · máy rảnh · tốt nhất trong 3 lần
>
> | Phép tính | Chính xác | Xấp xỉ | Sai số |
> |---|---|---|---|
> | Đếm phân biệt `agg_id` (420M giá trị) | 122,7 s | **14,1 s** | 0,76% |
> | Đếm phân biệt `price` (3M giá trị) | **11,3 s** | 14,6 s | 1,6% |
> | Phân vị notional | **hết RAM** | 40,4 s | thứ hạng 0,047% |
>
> Sai số thứ hạng ≤ relErr, nhưng sai số **giá trị** tới 59 042% ở đuôi dày → dùng relErr ≤ 0,001 (gần như miễn phí)

**Hình:** `results/e6/error_vs_time_idle.png` (tuỳ chọn).

**Lời nói**
"Với cột có 420 triệu giá trị phân biệt, HyperLogLog nhanh gấp 8,7 lần với sai số dưới 1%. Với cột chỉ 3 triệu giá trị, đếm chính xác lại nhanh hơn mọi cấu hình xấp xỉ. Phân vị chính xác hết RAM, nên bắt buộc dùng xấp xỉ. Và vì thời gian gần như không đổi theo relErr, nhóm chọn relErr nhỏ nhất: notional có đuôi rất dày, lệch 1% thứ hạng quanh p99 làm giá trị lệch hàng trăm lần. Lần đo đầu khi máy bận cho con số 12,7 lần; nhóm đo lại trên máy rảnh và dùng 8,7 lần."

**Thời gian:** 1 phút · Mảng 3

---

## Slide 15 · E7: Phát hiện FTX

**Trên slide**

> ## Ngày FTX sụp đổ là ngày nhiều báo động nhất tháng
>
> Modified z-score (Iglewicz–Hoaglin), |M| > 3,5, baseline 1 ngày, sàn 0,5%
> 8/11 và 9/11/2022: hạng **1** và **2** · mùa yên 2026: **9,3** báo động/ngày (cấu hình theo plan: 202)
> Kiểm chứng ngoài mẫu LUNA 5/2022, giữ nguyên tham số: 9–12/5 chiếm trọn **top 1–4**

**Hình:** `results/e7/alerts_per_day_nov22.png` (chính), `results/e7/ftx_btc.png` (phụ).

**Lời nói**
"Cấu hình ban đầu theo plan, cửa sổ 60 phút, chỉ xếp ngày FTX ở hạng 4 và 29, và báo động 200 lần mỗi ngày khi thị trường yên. Sau khi đổi baseline 1 ngày và thêm sàn biến động 0,5%, hai ngày FTX lên hạng 1 và 2, còn báo động mùa yên giảm xuống 9 mỗi ngày. Để tránh chọn tham số theo đúng sự kiện cần tìm, nhóm giữ nguyên tham số và chạy trên sự kiện LUNA tháng 5/2022: bốn ngày sự kiện chiếm trọn top 4."

**Thời gian:** 1,5 phút · Mảng 4

---

## Slide 16 · E7: Replay FTX qua streaming + stress test

**Trên slide**

> ## Cùng bộ phát hiện, hai tầng giống hệt; chịu tải 23,8 nghìn bản tin/s
>
> **111 / 111** báo động stream trùng batch · **5 758 / 5 758** nến khớp official
> Stress test 1× → 1 000×: đỉnh **23 766 bản tin/s** ở 200× (p95 7,75 s) = **1 278 ×** lưu lượng live
>
> *Chứng minh cho C2 (mở rộng) · chữ V velocity*

**Hình:** `results/e7/stress.png`.

**Lời nói**
"Phát lại aggTrades của FTX qua toàn bộ pipeline streaming ở tốc độ 100 lần: nến stream khớp official tuyệt đối, và 111 báo động trên stream trùng khít 111 báo động trên batch. Nguyên tắc 'viết một lần, chạy hai tầng' mở rộng được sang cả bộ phát hiện bất thường. Khi tăng tốc dần, Spark trên một máy xử lý tối đa khoảng 24 nghìn bản tin mỗi giây, gấp 1 278 lần lưu lượng live."

**Thời gian:** 1,5 phút · Mảng 4

---

## Slide 17 · C5: Chọn nguồn nào?

**Trên slide**

> ## Stream cho độ tươi, batch cho độ đúng, official để phân xử
>
> 24 giờ · 7 200 nến/nguồn · 100 826 phiên bản nến
>
> | Nguồn | Đầy đủ | Nhất quán | Độ tươi p50 / p95 |
> |---|---|---|---|
> | Stream (W = 5 s) | 100% | 100% | 11,4 s / 16,7 s |
> | Stream update (W = 30 s) | 100% | 100% | 0,0 s / 2,2 s |
> | Batch (T+1) | 100% | 100% | ~36,5 h |
>
> Time-to-convergence: **100%** nến hội tụ · p50 **0,0 s** · p95 **2,9 s**
>
> | Nhu cầu | Nguồn |
> |---|---|
> | Dashboard, cảnh báo | Stream |
> | Báo cáo sau T+1 | Batch / official |
> | Xung đột ba nguồn | Official làm tham chiếu |
> | Mạng xấu, nhiều bản tin trễ | Batch |
>
> *Chứng minh cho C5*

**Hình:** `results/c5/ttc_cdf.png`.

**Lời nói**
"Để trả lời 'khi nào tin nguồn nào', nhóm định nghĩa time-to-convergence: từ lúc hết phút đến lúc nến stream trùng nến official. Trong 24 giờ đo đủ ba nguồn, 100% nến hội tụ, p95 là 2,9 giây. Từ đó rút ra chính sách: stream cho dashboard và cảnh báo; batch hoặc official cho báo cáo sau T+1; official làm tham chiếu khi các nguồn xung đột."

**Thời gian:** 1 phút · Mảng 2

---

## Slide 18 · Bài học kỹ thuật

**Trên slide**

> ## Những lỗi chỉ lộ ra khi chạy thật
>
> | Bẫy | Hệ quả nếu bỏ qua |
> |---|---|
> | Backfill xen kẽ với tin live | Dữ liệu bù bị loại vì trễ |
> | Replay E3 bằng "offset về đầu" | Mọi watermark đều 100%, kết luận sai |
> | PySpark trả timestamp theo giờ máy | Nến lệch đúng 7 giờ |
> | E5 dùng trade time cho stream | Δt luôn = 0, bài toán thành tầm thường |
> | Producer Kafka không có backpressure | Mất 100 000 bản tin |
> | Bật lại sau 4 tuần tắt máy | Backfill không giới hạn → hàng chục triệu request REST |

**Lời nói**
"Nhóm ghi lại 16 bẫy, đây là 6 bẫy đáng nhớ nhất. Hai bẫy nguy hiểm nhất là bẫy cho ra kết luận sai mà không báo lỗi: phát lại E3 bằng cách đặt offset về đầu khiến mọi watermark đều 100%, và timestamp bị lệch đúng 7 giờ vì múi giờ của máy."

**Thời gian:** 1 phút · Cả nhóm

---

## Slide 19 · Kết luận và hạn chế

**Trên slide**

> ## Tích hợp batch–stream khớp tuyệt đối được, nếu thiết kế đúng
>
> **RQ1 · Lược đồ:** suy đơn vị theo độ lớn xử lý được drift theo thời gian và giữa nguồn (E1: 0% → 100%)
> **RQ2 · Tương đương:** batch = stream = official khi kiến trúc đúng (E2, E4, E7); khi lệch, taxonomy chỉ ra vì sao (E3)
> **RQ3 · Chọn nguồn:** stream cho độ tươi (~11 s), batch sau T+1, official làm tham chiếu (C5)
>
> **Hạn chế:** 1 máy 16 GB · E3 chưa đạt cổng hiệu năng · E7 không có nhãn chuẩn · ghép mờ chỉ 30 phút, 2 cặp
> **Hướng tiếp:** cụm nhiều worker · mô hình học máy đa biến cho E7 · Delta Lake time-travel cho lineage

**Lời nói**
"Kết luận cho ba câu hỏi nghiên cứu. Một: lược đồ chuẩn nhận biết drift giải quyết được drift theo thời gian và giữa các nguồn. Hai: batch, stream và official khớp tuyệt đối khi kiến trúc đúng; khi chúng lệch, taxonomy chỉ ra nguyên nhân. Ba: stream dùng cho độ tươi, batch cho độ đúng, official để phân xử. Hạn chế lớn nhất: mọi thứ chạy trên một máy. Xin cảm ơn hội đồng."

**Thời gian:** 1 phút · Cả nhóm

---

## Phụ lục

| Slide | Nội dung | Nguồn |
|---|---|---|
| A1 | Bảng E1 đầy đủ 40 dòng (5 cặp × 4 tháng × 2 phiên bản) | `results/e1/ablation.csv` |
| A2 | Bảng E3 Azure đầy đủ: baseline + jitter | `results/e3/tradeoff.csv` |
| A3 | Bảng E4 + 4 timeline sự cố | `results/e4/` |
| A4 | Bảng E5 P/R/F1 theo δ + chi phí blocking | `results/e5/` |
| A5 | E7: quét tham số, LUNA, stress test, so sánh học máy | `results/e7/` |
| A6 | Lược đồ Postgres + một dòng `candles` thật với cột lineage | `sql/init.sql` |

## Câu hỏi hội đồng có thể hỏi

1. **"Vấn đề tích hợp dữ liệu của đề tài là gì?"** Hợp nhất một thực thể (nến 1 phút theo `symbol, minute`) từ ba nguồn dị thể về lược đồ, đơn vị, mức chi tiết và thời điểm đến, thành một bảng duy nhất khớp tuyệt đối; sau đó trả lời khi các nguồn xung đột thì vì sao và nên tin nguồn nào (slide 2–3).
2. **"100% khớp có phải do so cùng một nguồn không?"** Không. Batch dựng từ aggTrades, official là klines; hai file khác nhau do Binance công bố riêng, có `file_checksum` khác nhau trong lineage. Stream đến từ WebSocket, một kênh hoàn toàn khác.
3. **"Sao phải tiêm độ trễ giả vào E3?"** Dữ liệu thật trong cửa sổ đo không có bản tin nào bị watermark loại. Phần jitter có kiểm soát trả lời câu hỏi "nếu mạng xấu thì sao" và luôn được trình bày tách riêng.
4. **"Tham số E7 có bị overfit không?"** Đã kiểm chứng trên LUNA 5/2022 với tham số giữ nguyên: 4 ngày sự kiện chiếm trọn top 4 của tháng.
5. **"Exactly-once thật không?"** Không theo nghĩa giao dịch phân tán. Đây là hiệu quả tương đương: producer at-least-once, loại trùng theo `agg_id` trong watermark, upsert theo khóa. E4 chứng minh bằng 0 nến thừa.
6. **"Máy 16 GB có đại diện cho big data không?"** 420 triệu dòng / 39 GB vượt RAM, nên Spark phải xử lý ngoài bộ nhớ. Cụm nhiều worker nằm ở hướng tiếp.
7. **"Demo có phải dựng sẵn không?"** Không. Demo Console gọi đúng `chaos.sh` dùng trong E4, đọc trực tiếp Postgres, và phép so khớp là so bằng tuyệt đối từng trường với nến official lấy qua REST của Binance.
