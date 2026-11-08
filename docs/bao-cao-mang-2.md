# Báo cáo chuyên sâu & Kịch bản bảo vệ: Mảng 2
## Streaming, Chịu lỗi (Chaos Testing) & Chính sách chọn nguồn

**Môn học:** IT5427 · Tích hợp và xử lý dữ liệu lớn

**Nhóm 3:** Vũ Tuấn Minh, Đỗ Xuân Thanh, Đào Quang Huy, Trần Anh Vũ

**Phụ trách Mảng 2:** Đóng góp **C3, C5** · Thí nghiệm **E3, E4** · Slide **8, 9, 10, 11, 16** · Kịch bản **DEMO hội đồng**

---

# PHẦN 1 · BÁO CÁO HỌC THUẬT (Sẵn sàng đưa vào Báo cáo môn học)

## 1. Thí nghiệm E3 · Đánh đổi Watermark: Giữa độ tươi và độ đúng
*Chứng minh cho đóng góp C2, C3, C5*

### 1.1. Câu hỏi nghiên cứu
Trong xử lý luồng sự kiện (Stream Processing), cơ chế Watermark đánh đổi giữa **độ chính xác (completeness/consistency)** và **độ trễ xử lý (timeliness)** như thế nào? Cụ thể: Khi hạ thấp watermark để có nến nhanh hơn, hệ thống phải trả giá bằng bao nhiêu phần trăm dữ liệu sai lệch?

### 1.2. Thiết kế thực nghiệm
- **Nguyên lý tái hiện luồng (Replay):** Dữ liệu live từ Binance WebSocket được lưu nguyên bản vào tầng Bronze (`data/lake/bronze/raw_aggtrades/` qua service `raw-archive`). Script `replay.py --from-archive --speed 1` phát lại toàn bộ giao dịch vào Kafka topic `binance.aggtrades.replay`, **giữ nguyên tuyệt đối thứ tự đến và khoảng cách thời gian vật lý thực tế**.
- **Không đặt offset về đầu rồi chạy hết tốc lực:** Vì nếu chạy dồn toa, toàn bộ bản tin nằm gọn trong 1–2 micro-batch, watermark không kịp tiến theo thời gian vật lý, làm sai lệch bản chất đánh đổi (Bẫy kỹ thuật P2).
- **Bộ thông số khảo sát:**
  - Cửa sổ kiểm thử chính thức: **08:00–10:00 UTC ngày 07/11/2026**, gồm **60.001 bản tin** của 5 cặp BTCUSDT, ETHUSDT, BNBUSDT, SOLUSDT và XRPUSDT.
  - Watermark khảo sát: $W \in \{0, 2, 5, 10, 30\}$ giây.
  - Micro-batch trigger: 2 giây (`ProcessingTime('2 seconds')`).
  - Mỗi lượt replay chạy ở tốc độ thật `speed=1`; tối đa hai Spark job chạy đồng thời trên VM Azure 4 vCPU, 16 GiB RAM.
- **Thử nghiệm tiêm độ trễ có kiểm soát (Network Jitter Injection):** Do luồng mạng thực tế của Binance tới máy chủ thu thập rất tốt, nhóm tiến hành tiêm trễ nhân tạo: **5% tổng số bản tin bị làm trễ ngẫu nhiên từ 0 đến 20 giây** (`JITTER_PCT=5, JITTER_MAX=20`) để mô phỏng môi trường mạng internet công cộng kém ổn định.

### 1.3. Kết quả thực nghiệm

#### A. Đánh đổi trên luồng dữ liệu Binance thật
| Watermark ($W$) | Tỷ lệ khớp nến Official | Độ trễ xử lý $p50$ | Độ trễ xử lý $p95$ | Bản tin bị loại (Late Dropped) |
|---|---|---|---|---|
| **0 s** | 100% | 4,84 s | 5,70 s | 0 |
| **2 s** | 100% | 6,92 s | 7,51 s | 0 |
| **5 s** | 100% | 9,23 s | 11,25 s | 0 |
| **10 s** | 100% | 15,24 s | 15,90 s | 0 |
| **30 s** | 100% | 34,44 s | 35,09 s | 0 |

Mỗi cấu hình baseline được đối soát trên 585 nến hoàn chỉnh sau khi bỏ phút biên đầu/cuối của cửa sổ replay. Cả năm watermark đều có 585 nến stream, khớp 100% với Official và không loại bản tin trễ.

#### B. Đánh đổi khi mạng xấu (Tiêm trễ 5% bản tin $\le 20\text{ s}$)
| Watermark ($W$) | Tỷ lệ khớp nến Official | Số bản tin bị loại | Độ trễ xử lý $p95$ |
|---|---|---|---|
| **0 s** | **12,20%** | 2.476 | 6,14 s |
| **2 s** | 14,75% | 2.157 | 8,06 s |
| **5 s** | 18,64% | 1.740 | 10,43 s |
| **10 s** | 33,05% | 1.008 | 16,17 s |
| **30 s** | **100,0%** | **0** | **35,24 s** |

Mỗi cấu hình jitter được đối soát trên 590 nến hoàn chỉnh. Chênh lệch 5 nến so với baseline đến từ quy tắc loại phút biên theo thời lượng replay thực tế; mọi watermark trong cùng series jitter vẫn dùng cùng một cửa sổ so sánh.

*(Đồ thị gồm hai panel dùng chung trục watermark: [tradeoff.png](../results/e3-azure-20261108/e3/tradeoff.png); số liệu gốc: [tradeoff.csv](../results/e3-azure-20261108/e3/tradeoff.csv).)*

#### C. Kiểm tra chất lượng lượt chạy

- Replay giữ nhịp tốt: sai lệch tối đa so với nhịp mục tiêu chỉ **0,010 giây**.
- Toàn bộ 10 cấu hình đều có progress, log và kết quả đối soát.
- Tuy nhiên, thời gian micro-batch $p95$ nằm trong khoảng **2,123–2,232 giây**, cao hơn trigger 2 giây khoảng **6–12%**. Validator nghiêm ngặt của dự án vì vậy ghi `[invalid]` với tiêu chí “batch p95 không được vượt trigger”. Kết quả trade-off vẫn được giữ làm bằng chứng thực nghiệm, nhưng độ trễ báo cáo phải được hiểu là độ trễ end-to-end trên VM thử nghiệm, bao gồm cả chi phí xử lý Spark.

### 1.4. Diễn giải bản chất kỹ thuật
1. **Tại sao dữ liệu thật $W = 0\text{s}$ vẫn khớp 100%?**
   Trong Spark Structured Streaming, watermark chỉ tiến ở ranh giới micro-batch. Trên cửa sổ baseline, không có bản tin nào bị Spark loại vì watermark và cả 585 nến đều khớp Official, kể cả khi $W=0\text{s}$.
2. **Vì sao chỉ 5% bản tin trễ làm sai phần lớn số nến?**
   Một nến 1 phút được tổng hợp từ nhiều giao dịch; chỉ cần một giao dịch quan trọng rơi ngoài watermark thì Open, Close, High, Low, Volume hoặc số trade có thể lệch. Ở $W=0\text{s}$, tỷ lệ khớp chỉ còn **12,20%**, tức **87,80%** nến không khớp hoàn toàn.
3. **Ý nghĩa kiến trúc:** Muốn có nến streaming nhanh (độ trễ ~10s), ta phải chấp nhận rủi ro khi mạng chập chờn. Để đảm bảo độ đúng tuyệt đối phục vụ kiểm toán tài chính, **tầng Batch sau T+1 là bắt buộc** để sửa chữa các sai lệch do tầng streaming gây ra.

---

## 2. Thí nghiệm E4 · Kiểm thử khả năng chịu lỗi (Chaos Testing & Fault Injection)
*Chứng minh cho đóng góp C3, C5, C6*

### 2.1. Câu hỏi nghiên cứu
Hệ thống tích hợp streaming có khả năng tự phục hồi (self-healing) khi các thành phần phân tán bị sự cố hay không? Liệu việc ngắt mạng hay crash container có làm mất dữ liệu, sinh nến trùng hoặc làm lệch nến so với sàn Binance không?

### 2.2. Thiết kế thực nghiệm
Nhóm xây dựng kịch bản kiểm thử hỗn loạn tự động bằng script `experiments/e4_chaos/chaos.sh`:
1. **WS Kill (`ws_kill`):** `docker kill` đột ngột container `ws-producer`.
2. **WS Netcut (`ws_netcut`):** `docker pause` container `ws-producer` (mô phỏng đứt cáp mạng, treo socket).
3. **Spark Kill (`spark_kill`):** `docker kill` tiến trình Spark Streaming Driver.
4. **Kafka Restart (`kafka_restart`):** Khởi động lại Kafka Broker.
5. **Batch Rerun (`batch_rerun`):** Chạy lại toàn bộ lô dữ liệu batch của 1 tháng để kiểm chứng tính Idempotent.
6. **Sự cố tự nhiên ngoại cảnh:** Máy trạm ngủ đông (Sleep) liên tục **5,7 giờ** trong quá trình chạy luồng live thật.

### 2.3. Kết quả thực nghiệm

| Kịch bản sự cố | Thời gian gián đoạn | Thời gian phục hồi ($T_{\text{rec}}$) | Số trade bù đắp (Backfill) | Số phút đối soát | Nến thiếu | Nến thừa | Nến lệch |
|---|---|---|---|---|---|---|---|
| **Kill WS Producer** | 64 s | 5,7 s | 663 | 80 | **0** | **0** | **0** |
| **Cắt mạng (Pause WS)** | 91 s | 6,4 s | 1.815 | 80 | **0** | **0** | **0** |
| **Kill Spark Driver** | 66 s | 25,6 s | 0 (đọc lại offset Kafka) | 85 | **0** | **0** | **0** |
| **Restart Kafka Broker** | 3 s | 8,7 s | 0 (Kafka lưu trên đĩa) | 65 | **0** | **0** | **0** |
| **Chạy lại Batch (XRP 08/2026)** | N/A | N/A | N/A | 175.680 dòng | **0** | **0** | MD5 không đổi |
| **SỰ CỐ MÁY NGỦ THẬT** | **5,7 giờ** | **~1,5 phút** | **260.074** | **514 phút × 5 cặp** | **0** | **0** | **0** |

*(Đồ thị timeline các sự cố: xem file `results/e4/timeline_*.png`)*

### 2.4. Điểm đột phá kiến trúc: Backfill Prior to Live (Bẫy kỹ thuật P1)
- **Vấn đề tiềm ẩn:** Trong Spark Streaming, watermark thời gian sự kiện là **toàn cục (global)** trên toàn bộ stream. Nếu khi mạng có lại, bộ nhận đẩy ngay các bản tin live của thời điểm hiện tại vào Kafka trước, watermark của Spark sẽ lập tức nhảy vọt tới thời điểm hiện tại. Khi đó, các bản tin backfill (vốn thuộc về 5–10 phút trước) sẽ bị Spark coi là "quá trễ" và âm thầm vứt bỏ toàn bộ!
- **Giải pháp của nhóm trong `ws_producer.py`:** Khi kết nối WebSocket được thiết lập lại, thư viện có thể đệm các tin live đến trong lúc ứng dụng đối chiếu `last_agg_id` từ bảng `ingest_state`. Ứng dụng hoàn tất REST backfill cho toàn bộ symbol và đẩy dữ liệu bù vào Kafka trước, sau đó mới đọc/xử lý phần tin live đang được đệm; các `agg_id` cũ được bỏ qua.
- **Bằng chứng thành công:** Ngay cả khi máy ngủ suốt 5,7 giờ, sau khi mở máy, 260.074 bản ghi được kéo về bù đắp và toàn bộ 514 phút nến của cả 5 cặp tiền đều khớp tuyệt đối 100% với nến chính thức của Binance.

---

## 3. Đóng góp C3 · Khung đối soát 3 nguồn và Taxonomy phân loại sai lệch

Khung đối soát ghép hai nguồn theo grain `(symbol, minute)` và so khớp tuyệt đối sáu trường `open`, `high`, `low`, `close`, `volume`, `trades`. Khi một nến lệch, `src/bdi/reconcile/taxonomy.py` gắn nhãn theo thứ tự ưu tiên dưới đây:

| Mã nhãn (Taxonomy Label) | Định nghĩa & Nguyên nhân gốc | Minh chứng thực tế trong đề tài |
|---|---|---|
| `EMPTY_MINUTE` | Phút thị trường không có giao dịch nào phát sinh. | Kiểm tra 878.400 phút của 5 cặp top Binance: không xảy ra phút nào có trade = 0. |
| `CAPTURE_START` | Phút đầu tiên khi hệ thống khởi động giữa chừng phút, dẫn tới thiếu một phần trade đầu phút. | Quan sát 5 nến trong ngày đầu chạy live lúc 15:07 UTC. Sau đó khớp 100%. |
| `DISCONNECT_GAP` | Mất bản tin do mất kết nối mạng và không được backfill. | Không quan sát trong các lượt E4 vì REST backfill đã bù đủ. |
| `DUPLICATE` | Bản tin bị gửi lặp làm phồng khối lượng Volume. | Không quan sát ở tầng nến; producer/dedup/upsert đã loại tác động của bản tin lặp. |
| `LATE_BEYOND_WATERMARK` | Bản tin có $t_{\text{event}} < \text{Watermark}$ nên bị Spark loại bỏ. | Được tạo có kiểm soát trong E3 jitter 5%; đây không phải sự cố mạng tự nhiên. |
| `NUMERIC_PRECISION` | Lệch vài số thập phân cuối do dùng kiểu `double` dấu phẩy động. | Bị triệt tiêu hoàn toàn nhờ quy chuẩn toàn bộ sang kiểu `DecimalType(24, 8)`. |
| `UNKNOWN` | Sai lệch chưa thể giải thích được bằng các nguyên nhân trên. | 0 trường hợp trong các lượt đối soát đã chạy. |

Các nhãn trên là khả năng mà taxonomy hỗ trợ, không phải tất cả đều đã xuất hiện ngoài thực tế. Hàm `simulate_late()` mô phỏng loại bản tin theo từng event để hỗ trợ phân loại E3 và được xem là cận trên; watermark thật của Spark chỉ tiến giữa các micro-batch.

---

## 4. Đóng góp C5 · Chính sách chọn nguồn dựa trên độ hội tụ (TTC)

### 4.1. Khái niệm Time-to-Convergence (TTC)
Nhóm định nghĩa **Time-to-Convergence (TTC)** là khoảng thời gian tính từ khi một phút kết thúc ($t = mm:00$) cho đến khi nến Streaming hội tụ về giá trị trùng khớp tuyệt đối với nến chính thức của sàn Binance và **không thay đổi nữa**.

### 4.2. Số liệu đo đạc thực tế
Đo trên cửa sổ ổn định 24 giờ `2026-11-07T11:34:00Z`–`2026-11-08T11:34:00Z`, gồm 7.200 nến của 5 symbol và 100.826 phiên bản trong `candle_versions`:

- **Độ phủ:** `stream`, `stream:update` và `official` đều có đủ 7.200/7.200 nến.
- **Tỷ lệ hội tụ:** **100% nến `stream:update` hội tụ về nến Official**.
- **TTC:** 64,44% nến đã ổn định tại mốc hết phút; $p50 = 0,0\text{ giây}$, $p95 = 2,9\text{ giây}$, $p99 = 4,8\text{ giây}$ và lớn nhất 8,2 giây. TTC bằng 0 nghĩa là giá trị cuối cùng đã ổn định trước hoặc đúng mốc hết phút; phép đo được chặn dưới tại 0 giây.
- **Luồng append chính:** completeness 100%, consistency 100%, timeliness $p50 = 11,36\text{ giây}$ và $p95 = 16,73\text{ giây}$.
- **Sự kiện trong cửa sổ:** một lần reconnect chủ động lúc `2026-11-08T11:08:13Z`, kéo dài khoảng 2 giây và backfill đủ 58 bản tin; không thiếu nến sau đối soát.

Các số trên là kết quả 24 giờ của Stream/Official. Cột Batch sẽ được bổ sung sau khi archive ngày 08/11 được Binance phát hành T+1; không dùng thời điểm nạp Batch để suy diễn độ trễ thời gian thực.

### 4.3. Bảng quy tắc chọn nguồn tổng hợp (Ma trận quyết định)

| Nhu cầu sử dụng | Nguồn dữ liệu tối ưu | Căn cứ định lượng từ thực nghiệm |
|---|---|---|
| **Dashboard giám sát, Cảnh báo thời gian thực** | **Streaming** (`source = 'stream'`) | Cửa sổ 24 giờ: completeness 100%, consistency 100%, timeliness $p50 = 11,36\text{ s}$ và $p95 = 16,73\text{ s}$. |
| **Báo cáo tài chính, Kiểm toán, Backtest thuật toán** | **Batch / Official** (sau T+1) | Độ đúng tuyệt đối 100% trên 878.400/878.400 nến (E2), loại bỏ hoàn toàn các rủi ro do mạng rớt hay jitter. |
| **Xung đột dữ liệu giữa 3 nguồn** | **Official** làm chuẩn phân xử | Official là nguồn tham chiếu do Binance công bố; không gọi đây là căn cứ pháp lý. |
| **Khi đường truyền WebSocket gặp sự cố đứt kết nối** | **Streaming** (đã qua cơ chế REST Backfill) | E4 chứng minh: Cơ chế backfill tự động khôi phục dữ liệu không thiếu nến nào. |
| **Môi trường mạng bất ổn định, nhiều bản tin đến trễ** | **Batch** (không tin tưởng Stream tức thời) | E3 chứng minh: với 5% bản tin bị trễ tối đa 20 giây, watermark 0 giây chỉ còn 12,20% nến khớp hoàn toàn. |

### 4.4. Giới hạn của kết quả

- E3 dùng một cửa sổ 2 giờ trên VM Azure 4 vCPU/16 GiB; validator hiệu năng ghi `[invalid]` vì micro-batch p95 vượt trigger 2 giây khoảng 6–12%. Kết quả vẫn dùng để phân tích trade-off, không dùng để tuyên bố cấu hình đã đạt cổng hiệu năng.
- C5 chạy trên một máy và đã hoàn chỉnh cho Stream/Official trong 24 giờ; Batch cùng cửa sổ chỉ được bổ sung sau khi archive T+1 xuất hiện.
- Các kết quả chưa được kiểm chứng trên cụm Spark nhiều worker, nhiều partition Kafka hoặc tải production lớn hơn.
- Một reconnect chủ động khoảng 2 giây nằm trong cửa sổ C5 và đã được giữ nguyên trong kết quả toàn kỳ; không loại mẫu để làm đẹp số liệu.

---

# PHẦN 2 · KỊCH BẢN DEMO TRỰC TIẾP TRƯỚC HỘI ĐỒNG (5 PHÚT)
*> "Ba mươi giây rút dây mạng thuyết phục hơn mười slide lý thuyết"*

### 1. Công tác chuẩn bị (Trước giờ bảo vệ 30 phút)
1. Mở sẵn trình duyệt:
   - Tab 1: Grafana Dashboard **BDI · Live** (`http://localhost:3000/d/bdi-live`), chọn khung thời gian `Last 15 minutes`, auto-refresh `5s`.
   - Tab 2: Sẵn sàng các hình ảnh kết quả: `results/e3-azure-20261108/e3/tradeoff.png`, `results/e4/timeline_01_ws_netcut.png`.
2. Mở sẵn 2 cửa sổ Terminal:
   - **Terminal 1 (Xem log):** `docker compose logs -f --tail=20 ws-producer stream-candles`
   - **Terminal 2 (Ra lệnh Chaos):** Đặt sẵn câu lệnh `bash experiments/e4_chaos/chaos.sh ws_netcut`
3. Chuẩn bị file video backup 60 giây (đề phòng mạng trường chặn WebSocket).

---

### 2. Kịch bản trình diễn từng giây

| Thời gian | Hành động trên màn hình | Lời thoại thuyết trình (Nói dõng dạc, tự tin) |
|---|---|---|
| **0:00 – 0:45** | Chiếu Dashboard Grafana. Chỉ chuột vào panel "Giá đóng cửa theo nguồn" và "Độ tươi dữ liệu". | *"Kính thưa hội đồng, trên màn hình là hệ thống streaming thời gian thực đang chạy nến 1-phút cho 5 cặp coin lớn nhất Binance. Quý thầy cô có thể thấy đường giá màu xanh của Stream và đường màu vàng của Official trùng khít lên nhau từng giây, độ tươi dữ liệu chỉ lệch khoảng 10 giây so với thực tế sàn."* |
| **0:45 – 1:15** | Chuyển sang Terminal 2, bấm Enter chạy lệnh `chaos.sh ws_netcut`. Quay lại Grafana. | *"Bây giờ, em sẽ tiến hành ngắt kết nối mạng cưỡng bức của bộ nhận WebSocket ngay lập tức. Trên Grafana, số trade/phút bắt đầu tụt dốc về 0, độ trễ dữ liệu tăng dần. Bộ nhận hoàn toàn mất tín hiệu từ sàn Binance."* |
| **1:15 – 2:00** | Lệnh unpause chạy tự động trong script. Terminal 1 hiện dòng log vàng `backfill BTCUSDT: aggId ...`. | *"Sau khi mạng được khôi phục, bộ nhận phát hiện ngay khoảng trống ID giao dịch. Thay vì nhận tiếp tin live làm nhảy watermark, hệ thống kích hoạt cơ chế Backfill qua REST API để kéo toàn bộ các giao dịch bị rớt trong lúc mất mạng về Kafka."* |
| **2:00 – 3:00** | Grafana nhảy nến mới. Bảng "Khoảng trống WS + backfill" xuất hiện dòng ghi nhận mới. Tỷ lệ lệch nến vẫn bằng 0. | *"Như hội đồng thấy trên bảng đối soát trực tiếp: 100% nến trong khoảng thời gian đứt mạng đã được bù đắp hoàn hảo. Không có bất kỳ cây nến nào bị thiếu, bị thừa hay bị sai lệch giá trị!"* |
| **3:00 – 4:00** | Chiếu hình `results/e3-azure-20261108/e3/tradeoff.png`. | *"Đây là kết quả E3 trên 60.001 bản tin được replay đúng tốc độ thật. Khi không tiêm jitter, cả năm watermark đều cho 100% nến khớp. Khi làm trễ 5% bản tin tối đa 20 giây, watermark 0 giây chỉ còn 12,20% nến khớp; watermark 30 giây phục hồi 100% nhưng p95 tăng lên 35,24 giây. Vì vậy Stream phù hợp cho cảnh báo tức thời, còn Batch/Official là nguồn chuẩn sau T+1."* |

---

# PHẦN 3 · KỊCH BẢN THUYẾT TRÌNH SLIDE (SLIDE 8, 9, 10, 11, 16)

---

### SLIDE 8 · E3: Luồng thực tế gần như đúng thứ tự
- **Tiêu đề Slide:** Binance gần như không có bản tin trễ: Watermark nhỏ là đủ
- **Con số anh hùng:** **100% nến khớp** ở cả 5 watermark · **0 bản tin bị loại**
- **Nội dung hiển thị:**
  - Bảng đối soát 5 mức Watermark trên dữ liệu thật (0s, 2s, 5s, 10s, 30s) $\rightarrow$ Đều đạt **100% nến khớp**.
  - Độ trễ xử lý $p95$ tăng dần từ $5,70\text{ s} \rightarrow 35,09\text{ s}$.
  - Nhận định: Trên dữ liệu Binance thật, Watermark chỉ đánh đổi về **độ trễ**, không đánh đổi về độ đúng.
- **Lời thoại MC (1 phút):**
  *"Kính thưa hội đồng, nhóm phát lại 60.001 giao dịch Binance trong hai giờ theo đúng tốc độ thật và thử năm mức watermark. Ở series không tiêm jitter, cả năm cấu hình đều tạo đủ nến và khớp 100% với Official; không bản tin nào bị loại vì đến trễ. Watermark lúc này chỉ làm tăng p95 từ 5,70 lên 35,09 giây. Vì vậy, trên cửa sổ dữ liệu này, watermark nhỏ cho độ tươi tốt hơn mà chưa làm giảm độ đúng."*
- **Hội đồng hỏi:** *"Tại sao watermark = 0s mà nến vẫn đúng 100%?"*
  **Trả lời:** *"Dạ thưa thầy, vì Watermark trong Spark chỉ được cập nhật sau khi kết thúc một micro-batch. Khi độ trễ mạng thực tế của Binance nhỏ hơn chu kỳ micro-batch 2 giây, dữ liệu chưa bị coi là trễ nên không bị loại bỏ ạ."*

---

### SLIDE 9 · E3: Khi mạng xấu – Bản chất đánh đổi
- **Tiêu đề Slide:** Chỉ 5% bản tin trễ làm sai 87,8% nến ở W=0s
- **Hình ảnh trọng tâm:** Hai panel dùng chung trục watermark trong `results/e3-azure-20261108/e3/tradeoff.png`
- **Nội dung hiển thị:**
  - Khi tiêm độ trễ 5% bản tin trễ $\le 20\text{ s}$:
    - Tại $W = 0\text{s}$: Chỉ khớp **12,20%** nến; 2.476 bản tin bị loại.
    - Tại $W = 30\text{s}$: Phục hồi lên **100%** nến khớp, không còn bản tin bị loại, nhưng độ trễ $p95$ tăng lên $35,24\text{ s}$.
  - Thông điệp cốt lõi: Đây là lý do **tầng Batch bắt buộc phải là nguồn chuẩn sau T+1**.
- **Lời thoại MC (1,5 phút):**
  *"Tuy nhiên, internet công cộng không phải lúc nào cũng ổn định. Khi nhóm làm trễ ngẫu nhiên 5% bản tin tối đa 20 giây, watermark 0 giây chỉ còn 12,20% nến khớp vì 2.476 bản tin bị loại. Khi tăng watermark lên 30 giây, tỷ lệ khớp trở lại 100% và không còn bản tin bị loại, nhưng p95 tăng lên 35,24 giây. Đây là đường cong nhanh thì thiếu, đủ thì chậm và là lý do Streaming không thể thay thế Batch/Official cho tác vụ cần độ chính xác tài chính tuyệt đối."*

---

### SLIDE 10 · E4: Khả năng chịu lỗi của hệ thống
- **Tiêu đề Slide:** 5 kịch bản sự cố – 0 cây nến sai lệch
- **Bảng số liệu nổi bật:**
  - Bảng 5 sự cố: Kill WS, Cắt mạng Pause WS, Kill Spark, Restart Kafka, Chạy lại Batch $\rightarrow$ Cột nến sai/thiếu toàn bộ là **0**.
  - **Điểm nhấn đặc biệt (Tô màu cam):** Sự cố máy chủ ngủ đông 5,7 giờ $\rightarrow$ Tự động backfill **260.074 bản ghi** $\rightarrow$ **514/514 phút khớp tuyệt đối**.
  - Thiết kế mấu chốt: **Backfill hoàn tất trước khi phát tin Live** (tránh trôi watermark).
- **Lời thoại MC (1,5 phút):**
  *"Để kiểm chứng độ bền bỉ của kiến trúc, nhóm đã thực hiện Chaos Testing với 5 kịch bản ngắt cưỡng bức. Kết quả ghi nhận tại bảng: thời gian tự phục hồi chỉ từ 5 đến 25 giây, không có bất kỳ nến nào bị mất hoặc bị trùng lặp. Đáng chú ý nhất là một sự cố tự nhiên ngoài kế hoạch: máy trạm bị ngủ đông 5,7 giờ. Khi bật dậy, bộ nhận đã tự phát hiện khoảng trống ID và kéo thành công hơn 260 nghìn giao dịch qua REST API. Toàn bộ 514 phút nến của cả 5 cặp tiền đều khớp 100% với nến sàn. Bí quyết kỹ thuật nằm ở việc nhóm bắt buộc backfill hoàn tất trước khi đẩy dữ liệu live, ngăn chặn hiện tượng watermark toàn cục loại bỏ nhầm dữ liệu bù."*

---

### SLIDE 11 · DEMO: Kiểm chứng thực tế
- **Tiêu đề Slide:** Trực tiếp: Rút dây mạng hệ thống Streaming
- **Nội dung hiển thị:**
  1. Giám sát thời gian thực trên Grafana Dashboard.
  2. Bơm lỗi ngắt mạng bộ nhận (`ws_netcut`).
  3. Quan sát quá trình tự phục hồi, backfill và đối soát nến tức thời.
- **Lời thoại MC (3–5 phút):** Tiến hành chuyển màn hình và thuyết minh theo kịch bản Demo ở Phần 2.

---

### SLIDE 16 · C5: Chính sách chọn nguồn
- **Tiêu đề Slide:** Stream cho độ tươi, Batch cho độ đúng, Official để phân xử
- **Hình ảnh trọng tâm:** Đồ thị CDF của Time-to-Convergence (`results/c5/ttc_cdf.png`)
- **Nội dung hiển thị:**
  - Cửa sổ 24 giờ có 7.200 nến: **100% hội tụ**; 64,44% đã ổn định tại mốc hết phút và 95% hội tụ trong **2,9 giây**.
  - Ma trận 5 quy tắc chọn nguồn:
    1. Giám sát / Cảnh báo tức thời $\rightarrow$ Chọn **Streaming**.
    2. Báo cáo tài chính / Kiểm toán sau T+1 $\rightarrow$ Chọn **Batch / Official**.
    3. Khi 3 nguồn mâu thuẫn $\rightarrow$ **Official** là nguồn tham chiếu do Binance công bố.
    4. Sau sự cố mạng $\rightarrow$ Stream có backfill kiểm tra chéo với Batch.
    5. Môi trường mạng chập chờn $\rightarrow$ Không tin Stream tức thời, đợi Batch.
- **Lời thoại MC (1 phút):**
  *"Từ cửa sổ live 24 giờ gồm 7.200 nến, nhóm quan sát 100% nến streaming hội tụ về Official; 64,44% đã ổn định ngay tại mốc hết phút và 95% hội tụ trong 2,9 giây. Luồng append chính đạt p50 11,36 giây và p95 16,73 giây. Vì vậy Streaming phù hợp cho dashboard và cảnh báo tức thời; Batch/Official phù hợp cho phân tích sau T+1; khi có xung đột, Official là nguồn tham chiếu do Binance công bố."*

---

# PHẦN 4 · BỘ CÂU HỎI PHẢN BIỆN HỘI ĐỒNG & ĐÁP ÁN CHUẨN

### Q1: Tại sao không tăng Watermark lên thật lớn (ví dụ 10 phút) để khỏi lo mất dữ liệu khi mạng lag?
> **Đáp án:** *"Dạ thưa thầy/cô, việc tăng watermark lên 10 phút sẽ gây ra 2 hậu quả nghiêm trọng: Thứ nhất, trong chế độ Append của Spark, nến chỉ được sinh ra và ghi vào database sau khi watermark đã vượt qua phút đó, nghĩa là dashboard sẽ bị trễ tới 10 phút, làm mất hoàn toàn giá trị thời gian thực của streaming. Thứ hai, Spark phải duy trì trạng thái (state store) trong bộ nhớ RAM cho toàn bộ dữ liệu trong 10 phút đó, gây lãng phí bộ nhớ và nguy cơ Out-Of-Memory rất cao khi thị trường biến động mạnh."*

### Q2: Nếu mất mạng 5–6 tiếng, gọi REST API bù hàng trăm nghìn bản ghi có bị Binance chặn IP (Rate limit HTTP 429/418) không?
> **Đáp án:** *"Dạ thưa thầy/cô, module `src/bdi/ingest/backfill.py` kéo tối đa 1.000 aggTrades mỗi request và phân trang theo `fromId`. Khi Binance trả HTTP 418 hoặc 429, code chờ đúng thời gian trong header `Retry-After` rồi thử lại. Phiên bản hiện tại chưa đọc `X-MBX-USED-WEIGHT` và chưa có exponential backoff riêng cho REST backfill, nên nhóm không khẳng định tuyệt đối rằng sẽ không bị khóa IP; đây là giới hạn và là hướng cần bổ sung nếu triển khai production."*

### Q3: Tính chất Idempotent (ghi lại không trùng) trong tầng Streaming và Batch được đảm bảo như thế nào?
> **Đáp án:** *"Dạ thưa thầy/cô, hệ thống đảm bảo tính Idempotent ở cả 2 tầng: Trong PostgreSQL, bảng `candles` có khóa chính là bộ ba `(symbol, minute, source)`. Khi Spark ghi dữ liệu qua `foreachBatch`, câu lệnh SQL luôn dùng cú pháp `INSERT ... ON CONFLICT (symbol, minute, source) DO UPDATE SET ...`. Vì vậy, dù một micro-batch bị tính lại do crash hoặc một file batch được chạy lại nhiều lần, số dòng và mã băm MD5 của bảng vẫn giữ nguyên tuyệt đối (đã chứng minh ở E4 batch_rerun)."*
