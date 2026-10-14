# 07 · Hướng dẫn cho từng thành viên

Đọc phần **A** trước (ai cũng phải làm), rồi tới phần của mảng mình ở **B**. Mọi lệnh chạy từ thư mục gốc của repo. Số liệu hiện có nằm trong [05-ket-qua.md](05-ket-qua.md), dàn ý slide trong [06-dan-y-slide.md](06-dan-y-slide.md).

**Điền tên phụ trách** (nhóm thống nhất rồi sửa dòng này và bảng trong [02-roadmap.md](02-roadmap.md)):

| Mảng | Phụ trách | Thí nghiệm | Slide trình bày |
|---|---|---|---|
| 1 · Batch, lineage | _______ | E1, E2, C1, C2, C6 | 1–7 |
| 2 · Streaming, chịu lỗi | _______ | E3, E4, C3, C5, demo | 8–11, 16 |
| 3 · Ghép mờ, truy vấn xấp xỉ | _______ | E5, E6 | 12–13 |
| 4 · Bất thường, Grafana, thống kê | _______ | E7, thống kê mô tả | 14–15, 17 |
| Cả nhóm | | Báo cáo, kết luận | 18 |

---

## A · Việc chung cho mọi người

### A1. Lấy code và cài đặt (khoảng 15 phút)

```bash
git clone <địa chỉ repo>
cd bigdata
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
set -a && source .env && set +a
export PYTHONPATH=src
```

- Cần thêm: **Docker Desktop**, **Java 17 hoặc 21** (macOS: `brew install openjdk@21`), Python 3.11–3.14.
- Kiểm tra cài đặt: `make test` phải ra **21 passed**. Lỗi `Java gateway process exited` nghĩa là chưa có Java.
- Chi tiết và cách xử lý lỗi thường gặp: [00-setup.md](00-setup.md).

### A2. Hiểu đúng: dữ liệu KHÔNG có trong repo

Repo chỉ có code, tài liệu và kết quả (`results/*.csv`, `results/*.png`). Dữ liệu thô (39 GB), parquet và cơ sở dữ liệu nằm trên **máy đang chạy luồng live** và không được đẩy lên GitHub (cam kết trong đề xuất).

Vì vậy:

| Việc | Làm ở đâu |
|---|---|
| Việc cần luồng live, Kafka, raw archive (E3, E4, C5, demo) | Trên **máy đang chạy luồng live**, hoặc hẹn người giữ máy đó chạy giúp |
| Việc chỉ cần dữ liệu lịch sử (E1, E5, E6, E7 trên LUNA) | Trên máy mình: tải **đúng phần dữ liệu cần** theo hướng dẫn của mảng |
| Viết báo cáo, làm slide | Ở đâu cũng được, chỉ cần `docs/` và `results/` |

Bật hạ tầng trên máy mình khi cần chạy thí nghiệm:

```bash
make up          # Kafka + Postgres (cổng 5433) + Grafana (http://localhost:3000)
```

### A3. Quy tắc làm việc với git

1. **Không commit thẳng lên `main`.** Mỗi việc một nhánh: `git checkout -b mang2/e3-cua-so-dai`.
2. **Không commit dữ liệu**: `data/`, file `.zip`, `.env`, log. `.gitignore` đã chặn, nhưng hãy xem `git status` trước khi commit.
3. Kết quả mới: commit file trong `results/` **và** cập nhật số trong `docs/experiments/<E?>.md` + `docs/05-ket-qua.md` trong cùng một commit.
4. Push nhánh rồi mở Pull Request; nhờ một người khác xem trước khi gộp.
5. Commit message ngắn, nói việc gì: `E1: chạy đủ 5 cặp × 4 tháng`.

### A4. Sau khi có kết quả mới

```bash
make results                     # nạp results/*.csv vào Postgres + dựng lại dashboard Grafana "Kết quả thí nghiệm"
```

Trang web kết quả (https://claude.ai/artifact/HCZmyQry8Uk5BDzHwDsUCP) là bản cố định ngày 14/10. Báo người quản lý trang khi số liệu đổi, để cập nhật trang.

### A5. Ai cũng phải viết

Mỗi mảng viết phần báo cáo của mình theo khung:

1. Câu hỏi
2. Thiết kế thí nghiệm
3. Kết quả (bảng + hình)
4. Diễn giải
5. Hạn chế

Số liệu lấy từ `docs/experiments/<E?>.md`. Hạn nộp bản nháp: **cuối tuần 3**.

---

## B · Việc riêng theo mảng

### Mảng 1 · Batch, lineage (E1, E2, C1, C2, C6)

**Đã xong:** E2 (878 400/878 400 nến khớp), E1 trên SOL + XRP, toàn bộ code batch.

#### B1.1 Chạy E1 cho đủ 5 cặp × 4 tháng (bắt buộc)

Cần dữ liệu aggTrades + klines của **cả 4 tháng, 5 cặp** (khoảng 39 GB, tải ~15 phút với mạng nhanh). Chạy trên máy đang có sẵn dữ liệu sẽ nhanh hơn nhiều.

```bash
python -m bdi.ingest.download                    # bỏ qua nếu máy đã có data/raw
python experiments/e1_drift_ablation.py          # 1–2 giờ; KHÔNG chạy cùng lúc việc nặng khác
```

- **Đầu ra:** `results/e1/ablation.csv` có **40 dòng** (5 cặp × 4 tháng × 2 phiên bản).
- **Đạt khi:** mọi dòng `naive` + kỳ 2026 có `pct_candle_ok = 0`; mọi dòng `drift_aware` có `pct_candle_ok = 100`.
- **Cập nhật:** bảng trong `docs/experiments/E1-drift-ablation.md`, mục E1 trong `docs/05-ket-qua.md`.

#### B1.2 Vẽ hình E1 cho slide 6

Hiện E1 chưa có hình. Viết thêm phần vẽ vào cuối `experiments/e1_drift_ablation.py`:
- biểu đồ cột nhóm: trục x là kỳ, mỗi kỳ 2 cột (naive, drift-aware), trục y là % nến đúng;
- dùng `_common.SERIES` và `_common.save(fig, "e1", "ablation")`, làm theo mẫu `experiments/e2_batch_vs_official.py`.

#### B1.3 Viết báo cáo C1, C2, C6

- **C1:** hàm `to_micros_col` trong `src/bdi/timeutil.py`. Giải thích ngưỡng 10¹² / 10¹⁴ / 10¹⁷. Dẫn chứng bằng contract test `tests/test_timeutil.py` (chụp output `pytest -v tests/test_timeutil.py`).
- **C2:** ba điểm kỹ thuật trong `src/bdi/candles.py`. Test `tests/test_candles.py::test_batch_equals_streaming` chứng minh cùng một hàm cho cùng kết quả ở hai tầng.
- **C6:** cột lineage trong `sql/init.sql`. Chụp ví dụ thật:
  ```sql
  SELECT source, file_checksum, watermark_config, pipeline_version, ingested_at
  FROM candles WHERE symbol = 'BTCUSDT' ORDER BY minute DESC LIMIT 3;
  ```
  Thêm kết quả `chaos.sh batch_rerun` (md5 không đổi) làm bằng chứng idempotent.

#### B1.4 Slide

- **Slide 4:** vẽ sơ đồ kiến trúc; bố cục đã mô tả trong `06-dan-y-slide.md`.
- **Slide 1–7:** làm theo dàn ý.

---

### Mảng 2 · Streaming, chịu lỗi (E3, E4, C3, C5, demo)

**Đã xong:** E3 trên 30 phút (dữ liệu thật + tiêm độ trễ), E4 với 4 kịch bản + sự cố máy ngủ, C5 sơ bộ. Toàn bộ việc dưới đây làm **trên máy đang chạy luồng live**.

#### B2.1 Giữ luồng live chạy liên tục (bắt buộc, hằng ngày)

```bash
make live-status                 # nến stream mới nhất phải cách hiện tại < 2 phút
docker compose ps                # ws-producer, stream-candles, raw-archive, official-poller đều "Up"
caffeinate -dims &               # giữ máy không ngủ (macOS)
```

Máy ngủ hay quá tải từng làm hỏng một lượt E3; xem bẫy P12, P13 trong [03-pitfalls.md](03-pitfalls.md).

#### B2.2 Chạy lại E3 trên cửa sổ dài (nên làm)

Chọn **2 giờ sôi động** (13:30–15:30 UTC, giờ mở cửa Mỹ) của một ngày đã có trong raw archive.

```bash
ls data/lake/bronze/raw_aggtrades/                                   # xem các ngày đã có
python -m bdi.ingest.rest_klines --start 2026-10-15T13:30 --end 2026-10-15T15:30    # official cho khoảng đó
GROUP=2 experiments/e3_watermark_sweep.sh 2026-10-15 2026-10-15T13:30 2026-10-15T15:30
JITTER_PCT=5 JITTER_MAX=20 GROUP=3 experiments/e3_watermark_sweep.sh 2026-10-15 2026-10-15T13:30 2026-10-15T15:30
python experiments/e3_analyze.py
```

- Mất khoảng 6–7 giờ máy (3 + 2 lượt × 2 giờ). Tắt mọi việc nặng khác trong lúc chạy.
- **Đạt khi:** trong log replay, dòng `trễ so với nhịp mục tiêu` luôn < 1 s.
- Trước khi chạy, xóa cấu hình cũ trong `results/e3/configs/` (hoặc chuyển sang thư mục khác), để hình không trộn hai cửa sổ.

#### B2.3 Hoàn thiện C5 trên 24 giờ (bắt buộc)

```bash
# ngày hôm sau, khi Binance đã công bố file ngày (T+1):
python -m bdi.ingest.download --days 2026-10-15
python -m bdi.batch.load_klines --periods 2026-10-15
python -m bdi.batch.build_batch --periods 2026-10-15
python experiments/c5_source_policy.py --start 2026-10-15 --end 2026-10-16 --sources stream stream:update batch
```

**Cập nhật:** bảng trong `docs/experiments/C5-source-policy.md` (lúc này có thêm cột batch), rồi viết bảng quy tắc chọn nguồn cho báo cáo.

#### B2.4 E4: chờ lần ngắt kết nối tự nhiên sau 24 giờ

```sql
SELECT symbol, disconnected_at, backfilled, reason FROM ws_gaps WHERE reason = 'proactive_24h';
```

Khi đã có dòng, chạy lại `python experiments/e4_analyze.py` và đối soát quanh thời điểm đó (`compare --left stream --start … --end …`).

#### B2.5 Viết báo cáo C3

Bảng nhãn nguyên nhân gộp từ E2, E3, E4 (xem mục C3 trong `05-ket-qua.md`). Nêu nhãn mới `CAPTURE_START`, phát hiện khi chạy thật.

#### B2.6 Chuẩn bị demo (bắt buộc)

- Tập kịch bản trong [04-demo.md](04-demo.md) ít nhất **2 lần**.
- **Quay video dự phòng 60 giây:** Grafana `/d/bdi-live`, chạy `experiments/e4_chaos/chaos.sh ws_netcut`, bảng "Khoảng trống WS + backfill" có dòng mới, đường stream vẫn trùng official.
- Kiểm tra trước buổi bảo vệ xem mạng hội trường có chặn `stream.binance.com` không. Nếu chặn, đổi service `ws-producer` sang `--fallback`.

---

### Mảng 3 · Ghép mờ, truy vấn xấp xỉ (E5, E6)

**Đã xong:** E5 (F1 0,995 với ghép giữ thứ tự), E6 (đếm phân biệt nhanh hơn 12,7 lần; phân vị chính xác hết bộ nhớ).

#### B3.1 E5: biến thể dùng thời điểm giao dịch (nên làm, ~5 phút)

Chạy trên máy đang có raw archive và silver của ngày 13/10:

```bash
python experiments/e5_linkage.py --date 2026-10-13 --start 2026-10-13T15:15 --minutes 30 --stream-time event
```

Kỳ vọng P = R = 1 ở mọi δ. Đây là bằng chứng vì sao phải dùng thời điểm **nhận**. Lưu kết quả sang tên khác trước khi chạy lại bản chính: chép `results/e5/linkage.csv` thành `linkage_event.csv`.

#### B3.2 E6: đo lại thời gian khi máy rảnh (bắt buộc)

Số đo hiện tại được ghi khi máy tải cao (load 25–30), nên thời gian chưa đáng tin.

```bash
docker compose stop stream-candles-update raw-archive     # tạm tắt việc nặng (nhớ bật lại!)
SPARK_DRIVER_MEMORY=6g python experiments/e6_approx.py --repeats 3
docker compose start stream-candles-update raw-archive
```

- Cần silver trades của cả 4 tháng (chỉ có trên máy đã chạy `build_batch`).
- **Đạt khi:** các cấu hình relErr nhanh dần theo thứ tự 0,05 < 0,01 < 0,001 (lần đo cũ bị ngược do tải máy).

#### B3.3 (Mở rộng) E6 trên 2 worker

Dựng Spark standalone (1 master + 2 worker) bằng Docker. Chạy `SPARK_MASTER=spark://<host>:7077 python experiments/e6_approx.py --tag 2workers`, rồi so với `--tag local`.

#### B3.4 Viết báo cáo C4 và E6

- **C4:** vì sao có ground truth miễn phí; ba nguồn lỗi (độ trễ nhận, lệch đồng hồ 24,6% Δt âm, 18% giao dịch trùng hệt); thuật toán ghép giữ thứ tự (`_ordered` trong `src/bdi/linkage/fuzzy.py`).
- **E6:** khi nào nên và không nên dùng xấp xỉ; sai số thứ hạng khác sai số giá trị.

---

### Mảng 4 · Bất thường, Grafana, thống kê (E7, thống kê mô tả)

**Đã xong:** E7 batch (8–9/11 là top-2), replay FTX trên stream (111/111 trùng batch), dashboard Grafana, phát hiện bất thường trên luồng live.

#### B4.1 Kiểm lại E7 trên sự kiện LUNA, giữ nguyên tham số (bắt buộc, quan trọng nhất)

Tham số hiện tại được chọn trên chính dữ liệu FTX. Phải thử trên một sự kiện **chưa dùng để chọn tham số** để trả lời câu hỏi "có khớp theo dữ liệu không". LUNA sụp mạnh nhất khoảng 9–12/5/2022.

```bash
make up
python -m bdi.ingest.download --months 2022-05             # 5 cặp × aggTrades + klines, ~10 GB
python -m bdi.batch.load_klines --periods 2022-05
python -m bdi.batch.build_batch --periods 2022-05          # ~20 phút
python experiments/e7_anomaly.py --month 2022-05 --event-days 2022-05-09 2022-05-10 2022-05-11 2022-05-12 \
    --windows 1440 --thresholds 3.5 --floors 0.005 --tag luna
```

- **Không chỉnh tham số** sau khi xem kết quả. Báo cáo đúng những gì ra, kể cả khi không đẹp.
- **Đầu ra:** `results/e7/sweep_luna.csv` có số báo động và thứ hạng của từng ngày sự kiện.
- **Cập nhật:** `docs/experiments/E7-anomaly-ftx.md` (bỏ dòng "cần kiểm lại trên LUNA"), mục E7 trong `05-ket-qua.md`.

#### B4.2 Thống kê mô tả (bắt buộc cho phần đầu báo cáo)

Chưa có script. Viết `experiments/descriptive_stats.py`, đọc `data/lake/silver/trades` và bảng `candles` (source `batch`), xuất ra `results/desc/`:

1. Số giao dịch theo cặp × tháng (bảng);
2. Phân phối số trade mỗi phút theo cặp (boxplot hoặc histogram);
3. Khối lượng USDT mỗi ngày theo cặp (đường);
4. Tỉ lệ lệnh mua chủ động (`is_buyer_maker = false`) theo tháng.

Dùng `_common.py` để thống nhất màu và kiểu hình. So sánh 11/2022 với 2026 để làm nổi bật giai đoạn FTX.

#### B4.3 Stress test velocity (nên làm, khi máy rảnh)

```bash
SPEEDS="1 10 50 100 200" experiments/e7_stress.sh stress
```

- Chạy khi đã tắt bớt việc nặng.
- **Đầu ra:** `results/e7/stress.csv` và `results/e7/stress.png`.
- Nếu job tụt lại quá xa, thêm `--max-offsets-per-trigger 500000` vào lệnh stream trong `e7_stress.sh` (xem bẫy P14 trong [03-pitfalls.md](03-pitfalls.md)).

#### B4.4 Grafana

- Mở `/d/bdi-live` và `/d/bdi-results`, đề xuất và thêm panel nhóm muốn dùng khi demo.
- Sửa dashboard **kết quả** trong `grafana/build_results_dashboard.py`, rồi chạy `make results`. Không sửa tay file JSON được sinh ra.
- Sửa dashboard **live** trong giao diện Grafana, rồi Export JSON ghi đè `grafana/dashboards/bdi.json`.

---

## C · Lịch còn lại

| Mốc | Việc | Ai |
|---|---|---|
| Ngày 1–2 | A1–A3 cho mọi người; điền tên phụ trách | Cả nhóm |
| Ngày 2–3 | B1.1, B4.1, B3.2 (việc nặng, xếp lần lượt trên máy có dữ liệu) | Mảng 1, 4, 3 |
| Ngày 3 | B2.3 (C5 đủ 24h), B2.4 | Mảng 2 |
| Ngày 3–5 | B2.2 (E3 cửa sổ dài, chạy qua đêm với `caffeinate`) | Mảng 2 |
| Cuối tuần 3 | Nháp báo cáo từng mảng; B4.2 thống kê mô tả | Mỗi mảng |
| Tuần 4 | Chốt số liệu → cập nhật `05-ket-qua.md` + trang web → làm slide → tập demo 2 lần | Cả nhóm |

**Máy có dữ liệu chỉ có một**, nên các việc nặng (E1 đủ, LUNA, E6, E3 cửa sổ dài, stress) phải **xếp lịch lần lượt**, không chạy chồng lên nhau. Chạy chồng sẽ làm sai số đo thời gian và có thể làm sập luồng live (bẫy P13, P16).

**Nếu trễ tiến độ, cắt theo thứ tự:**
1. E6 trên 2 worker;
2. Stress test;
3. E3 cửa sổ dài.

**Không cắt:** C5 đủ 24h, E7 trên LUNA, demo.
