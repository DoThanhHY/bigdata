# 06 · Dàn ý slide thuyết trình

**Mục tiêu:** khoảng 18 slide chính + 6 slide phụ lục, nói 15–18 phút, demo 3–5 phút.
**Số liệu:** lấy từ [05-ket-qua.md](05-ket-qua.md). Hình lấy từ `results/` (PNG 160 dpi, dán thẳng vào PowerPoint).

## Quy ước trình bày

- **Mỗi slide một thông điệp**, viết thành câu khẳng định ở tiêu đề (ví dụ "Batch khớp official 100%", không phải "Kết quả E2").
- Số quan trọng để **cỡ chữ lớn** (con số anh hùng), chú thích nhỏ bên dưới.
- Màu: xanh `#2a78d6` cho "stream/của nhóm", cam `#eb6834` cho "điểm nhấn/sự cố", xám cho phần phụ. Khớp với màu các hình trong `results/`.
- Bảng tối đa 5–6 dòng trên slide; bảng đầy đủ để ở phụ lục.
- Mỗi slide kết quả có dòng nhỏ ở chân: *"Chứng minh cho C…"* để hội đồng thấy đóng góp nào được bảo vệ.

## Phân vai gợi ý

| Phần | Slide | Người nói |
|---|---|---|
| Mở đầu, kiến trúc | 1–5 | Mảng 1 |
| Batch: E1, E2 | 6–7 | Mảng 1 |
| Streaming: E3, E4, demo | 8–11 | Mảng 2 |
| E5, E6 | 12–13 | Mảng 3 |
| E7, C5, kết luận | 14–18 | Mảng 4 |

---

## Slide 1 · Tiêu đề

- **Tiêu đề:** Tích hợp batch và streaming dữ liệu giao dịch Binance
- Phụ đề: IT5427 · Nhóm 3 · Vũ Tuấn Minh, Đỗ Xuân Thanh, Đào Quang Huy, Trần Anh Vũ
- Một dải số nhỏ dưới tiêu đề: `420 triệu giao dịch · 5 cặp · 878 400 nến · 7 thí nghiệm`
- **Thời gian:** 20 s

## Slide 2 · Bài toán

- **Thông điệp:** Cùng một dữ liệu đến từ 3 nguồn khác nhau. Câu hỏi là chúng có khớp nhau không, và khi nào tin nguồn nào.
- Hình: 3 khối nguồn nối vào 1 câu hỏi.
  - CSV aggTrades (lịch sử)
  - CSV klines (official)
  - WebSocket (real-time)
- 3 câu hỏi nghiên cứu (RQ1–RQ3) mỗi câu 1 dòng:
  - **RQ1:** Làm sao hợp nhất lược đồ khi chính nguồn đổi định dạng (schema drift)?
  - **RQ2:** Batch và streaming có cho cùng một kết quả không? Nếu không thì vì sao?
  - **RQ3:** Nên chọn nguồn nào cho mục đích nào?
- **Ghi chú người nói:** nhấn rằng "khớp" ở đây là khớp **tuyệt đối** từng chữ số, không phải gần đúng.
- **Thời gian:** 1 phút

## Slide 3 · Dữ liệu & Thống kê mô tả

- **Thông điệp:** 420 triệu dòng aggTrades (1,05 tỷ giao dịch), hai giai đoạn đối lập, có cả sự kiện cực đoan.
- Con số lớn: **419 835 967** dòng aggTrades · **1,049 tỷ** giao dịch khớp lệnh · **400,35 tỷ USDT** · **39 GB**
- Bảng nhỏ 2 cột (từ `results/desc/trades_by_month.csv`):
  - **2022-11 (FTX, ms):** 214,3M dòng aggTrades (51,2%) · 209,7 tỷ USDT (52,4%) · BTC p50 = 3 718 trade/phút
  - **2026-06..08 (thị trường yên, µs):** 203,9M dòng aggTrades (3 tháng) · 190,6 tỷ USDT · BTC p50 = 1 577 trade/phút
- Hình nhỏ kèm theo: `results/desc/trades_per_minute.png` hoặc `results/desc/daily_quote_volume.png`
- Luồng live: 5 cặp, chạy liên tục từ 13/10, ~18,6 bản tin/s
- **Ghi chú:** con số thực gấp 2,3 lần ước tính 185M trong đề xuất.
- **Thời gian:** 45 s

## Slide 4 · Kiến trúc

- **Thông điệp:** Một hàm dựng nến dùng chung cho cả hai tầng.
- **Hình cần vẽ:** sơ đồ kiến trúc (vẽ lại từ README, dạng khối):
  - Tầng batch: `data.binance.vision → download (sha256) → CSV → silver parquet → build_candles()`
  - Tầng streaming: `WebSocket → ws_producer (+REST backfill) → Kafka → Spark Streaming (watermark, dedup) → build_candles()`
  - Cả hai nhánh cùng ghi upsert vào `Postgres candles (symbol, minute, source)`, rồi tới đối soát và Grafana.
  - Tô **cam** khối `build_candles()` ở giữa, nơi hai tầng gặp nhau.
- Chân slide: PySpark 4.0 · Kafka 3.9 KRaft · Postgres 16 · Grafana · Docker Compose
- **Thời gian:** 1 phút

## Slide 5 · 6 đóng góp

- **Thông điệp:** Mỗi đóng góp đều có thí nghiệm chứng minh bằng số.
- Bảng 3 cột (Đóng góp → Chứng minh bằng → Con số chính):

| Đóng góp | Thí nghiệm | Con số |
|---|---|---|
| C1 Lược đồ nhận biết drift | E1 | naive hỏng 100% · drift-aware đúng 100% |
| C2 Viết một lần, chạy hai tầng | E2, E7 | 878 400/878 400 nến · 111/111 báo động |
| C3 Đối soát + nhãn nguyên nhân | E2–E4 | 7 nhãn, 0 `UNKNOWN` |
| C4 Ghép mờ có ground truth | E5 | F1 = 0,995 |
| C5 Chọn nguồn theo độ hội tụ | E3, C5 | hội tụ p50 13 s |
| C6 Lineage + idempotent | E4 | 0 nến lệch qua 5 sự cố |

- **Thời gian:** 45 s

## Slide 6 · E1: Schema drift

- **Tiêu đề:** Bỏ qua schema drift thì mất toàn bộ dữ liệu 2026
- **Hình:** `results/e1/ablation.png` (cột nhóm theo 4 kỳ, naive vs drift-aware, 5 cặp, 878 400 nến). Trên slide nên dùng bảng 4 ô làm chính, hình làm phụ:

| | 2022 (ms) | 2026 (µs) |
|---|---|---|
| Naive (luôn coi là ms) | 100% | **0%** |
| Drift-aware (suy theo độ lớn) | 100% | **100%** |

- Một dòng code minh họa: `~1,6×10¹² → ms · ~1,6×10¹⁵ → µs`
- Điểm thú vị: drift còn xảy ra **giữa các nguồn** (WS dùng ms, CSV 2026 dùng µs), và cùng một hàm xử lý được cả hai.
- Chân slide: *Chứng minh cho C1 · contract test 3 nguồn × 2 giai đoạn*
- **Thời gian:** 1 phút

## Slide 7 · E2: Batch vs official

- **Tiêu đề:** Batch tái tạo chính xác 100% nến chính thức của Binance
- Con số anh hùng: **878 400 / 878 400**, kèm dòng phụ "nến khớp tuyệt đối O·H·L·C·V·trades · 5 cặp × 4 tháng"
- Bên phải: 3 điểm kỹ thuật làm nên con số này:
  1. `min_by / max_by(price, struct(ts, agg_id))` thay cho `first / last`
  2. `Decimal` thay cho `double`
  3. `trades = Σ(last_id − first_id + 1)`
- Hình (tùy chọn, nhỏ): `results/e2/heatmap_all_match.png`
- **Ghi chú:** nếu hội đồng hỏi "sao không có lỗi nào", trả lời rằng chính ba điểm kỹ thuật trên loại bỏ mọi nguồn sai số; test `test_candles.py` cố tình xáo trộn thứ tự dòng để chứng minh.
- Chân slide: *Chứng minh cho C2*
- **Thời gian:** 1 phút

## Slide 8 · E3: Luồng thật gần như đúng thứ tự

- **Tiêu đề:** Binance gần như không có bản tin trễ: watermark nhỏ là đủ
- Con số: lệch thứ tự tối đa **0,78 s** · 4,1% bản tin đến sau bản tin mới hơn
- Bảng 5 dòng (watermark → % khớp → p95 độ trễ): 0 s 100% 11,9 s ··· 30 s 100% 36,1 s
- Thông điệp phụ: trên dữ liệu thật, watermark chỉ **đổi độ trễ**, không đổi độ đúng.
- **Ghi chú:** giải thích ngắn rằng watermark chỉ tiến giữa các micro-batch, nên độ lệch dưới ~1 s không bao giờ bị loại.
- **Thời gian:** 1 phút

## Slide 9 · E3: Khi mạng xấu

- **Tiêu đề:** Chỉ 5% bản tin trễ làm hỏng 98% nến: nhanh thì thiếu, đủ thì chậm
- Hình chính: `results/e3/tradeoff.png` (2 panel chung trục watermark)
- Chú thích: đường xanh lá (5% bản tin trễ ≤ 20 s) đi từ **2,1%** ở W = 0 s lên **100%** ở W = 30 s; độ trễ p95 tăng lên 41 s
- Câu chốt: đây là lý do **batch phải là nguồn chuẩn sau T+1**, dẫn sang C5.
- **Ghi chú:** nói rõ độ trễ được *tiêm có kiểm soát* vào replay, vì dữ liệu thật quá sạch để thấy trade-off. Không giấu điều này.
- Chân slide: *Chứng minh cho C2, C3, C5*
- **Thời gian:** 1,5 phút

## Slide 10 · E4: Chịu lỗi

- **Tiêu đề:** 5 sự cố, 0 nến sai
- Bảng 5 dòng: kịch bản · thời gian lỗi · phục hồi · backfill · nến lệch (cột cuối toàn **0**, tô xanh)
- Dòng nổi bật (tô cam): **Máy ngủ 5,7 giờ → backfill 260 074 bản ghi → 514/514 phút khớp**
- Hình nhỏ: `results/e4/timeline_01_ws_netcut.png`
- Ý thiết kế then chốt (1 dòng): backfill **tất cả** cặp trước khi đẩy tin live, nếu không watermark toàn cục sẽ loại dữ liệu bù.
- Chân slide: *Chứng minh cho C3, C6*
- **Thời gian:** 1,5 phút

## Slide 11 · DEMO

- **Tiêu đề:** Rút dây mạng ngay bây giờ
- Nội dung trên slide chỉ 3 bước to:
  1. Grafana live: giá 3 nguồn trùng nhau
  2. `chaos.sh ws_netcut`: luồng dừng
  3. Tự nối lại, backfill, nến vẫn khớp official
- Kịch bản chi tiết: [04-demo.md](04-demo.md)
- **Phương án dự phòng:** quay sẵn video 60 s trước buổi bảo vệ, phòng khi mạng hội trường chặn Binance.
- **Thời gian:** 3–5 phút

## Slide 12 · E5: Ghép mờ

- **Tiêu đề:** Không cần khóa vẫn ghép đúng 99,9%
- Hình: `results/e5/pr_curve.png`
- 3 điểm:
  - Ground truth miễn phí: `agg_id` bị ẩn khi ghép, chỉ dùng để chấm điểm
  - Ghép tham lam bị kẹt ở F1 0,885 vì **18% giao dịch trùng hệt** (cùng giây, giá, khối lượng)
  - Ghép **giữ thứ tự** đạt **F1 0,995**: FP giảm từ 1 482 xuống 29
- Con số phụ: blocking giảm số cặp từ **685 triệu xuống 1,1 triệu** mà không đổi kết quả
- Phát hiện phụ: 24,6% Δt âm, tức đồng hồ máy lệch so với Binance
- Chân slide: *Chứng minh cho C4*
- **Thời gian:** 1,5 phút

## Slide 13 · E6: Truy vấn xấp xỉ

- **Tiêu đề:** Xấp xỉ nhanh gấp 12,7 lần, nhưng chỉ khi số giá trị phân biệt lớn
- Bảng 3 dòng:
  - `agg_id` 420M giá trị: 234,7 s → **18,4 s**, sai 0,76%
  - `price` 3M giá trị: 90 s → 93 s, xấp xỉ **không** đáng dùng
  - Phân vị chính xác: **hết RAM**; xấp xỉ 102 s
- Bài học: sai số thứ hạng luôn trong cam kết, nhưng sai số giá trị có thể rất lớn với dữ liệu đuôi dày.
- Hình (tùy chọn): `results/e6/error_vs_time_local.png`
- **Thời gian:** 1 phút

## Slide 14 · E7: Phát hiện FTX (batch)

- **Tiêu đề:** Ngày FTX sụp đổ là ngày nhiều báo động nhất tháng
- Hình chính: `results/e7/alerts_per_day_nov22.png` (cột cam 8/11 vượt hẳn)
- Hình phụ: `results/e7/ftx_btc.png`
- Hộp "bài học phương pháp":
  - cửa sổ 60 phút → FTX đứng hạng 4 và hạng 29;
  - cửa sổ 1 ngày + sàn 0,5% → hạng **1 và 2**; báo động mùa yên giảm từ 202 xuống 9,3/ngày.
- **Kiểm chứng ngoài mẫu (LUNA 5/2022, giữ nguyên tham số):** cả 4 ngày sự kiện (9–12/5/2022) đều chiếm trọn **hạng 1, 2, 3, 4** của tháng (384, 237, 106, 82 báo động; chiếm 56,2% cả tháng).
- **Thời gian:** 1,5 phút

## Slide 15 · E7: Replay FTX qua streaming & Stress test velocity

- **Tiêu đề:** Cùng một bộ phát hiện, hai tầng cho kết quả giống hệt; chịu tải tới 23,8 nghìn bản tin/s
- Hai con số lớn cạnh nhau:
  - **111 / 111** báo động stream trùng batch
  - **5 758 / 5 758** nến stream khớp official
- Hình phụ / bên phải: `results/e7/stress.png` (Stress test 7 bậc tốc độ `1×` → `1 000×` trên 140 423 bản tin cao điểm FTX):
  - Đỉnh xử lý của Spark đạt **23 766 bản tin/s** ở `200×` (độ trễ p95 = 7,75 s), gấp **1 278 lần** lưu lượng live trung bình (18,6 bản tin/s).
  - Độ trễ p95 tạo đường cong chữ U: từ 9,86 s (`1×`, chờ watermark) xuống đáy **4,83 s** (`50×`), rồi tăng lại lên 9,32 s (`500×–1 000×`) khi tải đưa vào (36k–47,5k/s) vượt công suất.
- Chân slide: *Chứng minh cho C2 (mở rộng sang phát hiện bất thường) · chữ V velocity*
- **Thời gian:** 1,5 phút

## Slide 16 · C5: Chọn nguồn nào?

- **Tiêu đề:** Stream cho độ tươi, batch cho độ đúng, official để phân xử
- Bảng quyết định 5 dòng (lấy từ 05-ket-qua.md, mục C5)
- Hình nhỏ: `results/c5/ttc_cdf.png` (nến stream hội tụ về official sau p50 = 13 s)
- 3 tiêu chí: completeness · timeliness · consistency
- **Thời gian:** 1 phút

## Slide 17 · Bài học kỹ thuật

- **Tiêu đề:** Những lỗi chỉ lộ ra khi chạy thật
- 6 bẫy, mỗi bẫy 1 dòng "bẫy → hệ quả" (lấy bảng trong 05-ket-qua.md)
- Nhấn mạnh 2 bẫy:
  - lệch 7 giờ do múi giờ;
  - "replay bằng offset về đầu" cho kết luận sai.
- **Thời gian:** 1 phút

## Slide 18 · Kết luận và hạn chế

- **Kết luận (3 dòng)**, trả lời 3 RQ:
  - **RQ1:** suy đơn vị theo độ lớn xử lý được drift theo thời gian và giữa nguồn (E1).
  - **RQ2:** batch và stream khớp tuyệt đối khi kiến trúc đúng (E2, E4, E7). Khi lệch, taxonomy chỉ ra vì sao (E3).
  - **RQ3:** stream cho độ tươi (~10–24 s), batch là chuẩn sau T+1, official phân xử (C5).
- **Hạn chế (trung thực):**
  - chạy trên 1 máy 16 GB;
  - E3 cửa sổ 30 phút;
  - E7 không có nhãn chuẩn nên chỉ đánh giá theo sự kiện (FTX 11/2022 và LUNA 5/2022);
  - C5 mới đo ~2 giờ.
- **Hướng tiếp:**
  - cụm nhiều worker;
  - mở rộng bộ phát hiện sang mô hình học máy đa biến (Isolation Forest);
  - Delta Lake để làm time-travel cho lineage.
- **Thời gian:** 1 phút

---

## Phụ lục (để trả lời câu hỏi)

| Slide | Nội dung | Nguồn |
|---|---|---|
| A1 | Bảng E1 gộp theo kỳ (8 dòng; bảng đủ 40 dòng nếu bị hỏi) + Thống kê mô tả (`results/desc/`) | 05-ket-qua.md · E1, experiments/E1-drift-ablation.md |
| A2 | Bảng E3 đầy đủ: trigger 2 s, trigger 0, jitter | `results/e3/tradeoff.csv` |
| A3 | Bảng E4 sự cố + 4 timeline | `results/e4/` |
| A4 | Bảng E5 P/R/F1 đầy đủ + `blocking_cost.png` | `results/e5/` |
| A5 | Bảng quét tham số E7 + kiểm chứng LUNA + Stress test | `results/e7/sweep.csv`, `sweep_luna.csv`, `stress.csv` |
| A6 | Lược đồ Postgres + cột lineage, ví dụ 1 dòng `candles` thật | `sql/init.sql` |

## Câu hỏi hội đồng có thể hỏi (chuẩn bị sẵn)

1. **"100% khớp có phải do so nhầm cùng một nguồn không?"** Không. Batch dựng từ aggTrades, official là klines, hai file khác nhau do Binance công bố riêng; có `file_checksum` khác nhau trong lineage.
2. **"Sao phải tiêm độ trễ giả vào E3?"** Vì dữ liệu thật chỉ lệch tối đa 0,78 s, nhỏ hơn một micro-batch. Đó cũng là một kết quả. Phần tiêm độ trễ trả lời câu hỏi "nếu mạng xấu thì sao".
3. **"Tham số E7 có bị overfit không?"** Nhóm đã kiểm chứng độc lập trên sự kiện LUNA (5/2022) với tham số giữ nguyên hoàn toàn (`W=1440`, `|M|>3,5`, sàn `0,5%`): 4 ngày 9–12/5/2022 chiếm trọn top 1–4 của tháng (11/5 hạng 1 với 384 báo động, 9/5 hạng 2 với 237, 12/5 hạng 3 với 106, 10/5 hạng 4 với 82).
4. **"Exactly-once thật không?"** Không phải exactly-once theo nghĩa giao dịch phân tán. Đây là *hiệu quả tương đương*: producer at-least-once, dedup theo `agg_id` trong watermark, upsert theo khóa. E4 chứng minh bằng 0 nến thừa.
5. **"Máy 16 GB có đại diện cho big data không?"** Dữ liệu 420M dòng / 39 GB vượt RAM, nên Spark phải xử lý ngoài bộ nhớ. Phần mở rộng cụm nhiều worker nằm ở hướng tiếp.

## Việc cần làm để hoàn thiện slide

- [ ] Vẽ sơ đồ kiến trúc (slide 4)
- [x] Tạo biểu đồ cột E1 từ `results/e1/ablation.csv` (slide 6): `results/e1/ablation.png`
- [ ] Quay video demo dự phòng 60 s (slide 11)
- [ ] Cập nhật số liệu nếu chạy thêm E7 trên LUNA, C5 đủ 24h (E1 đủ 5 cặp đã chạy 25/10: kết luận không đổi)
