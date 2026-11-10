# Chạy E3 độc lập trên Azure VM

Gói này chỉ chạy Kafka, PostgreSQL, replay và các Spark job của E3. Nó không kết nối
ngược về PostgreSQL trên laptop và không ảnh hưởng luồng C5 đang thu tại nhà.

## VM khuyến nghị

- Region: `Southeast Asia` (nếu Student không có quota thì thử `Japan East`).
- Image: Ubuntu Server 24.04 LTS Gen2, x64.
- Size chuẩn: `Standard_D8ads_v5` — 8 vCPU, 32 GiB RAM, có SSD tạm.
- Fallback: `Standard_D4ads_v5` — 4 vCPU, 16 GiB RAM, đặt `GROUP=2`.
- Không dùng B-series và không dùng Spot: throttling/eviction làm sai latency.
- OS disk: Premium SSD LRS, 64 GiB; Delete with VM = Yes.
- Authentication: SSH public key; username `azureuser`.
- Inbound: chỉ SSH 22 từ IP công cộng của bạn. Không mở 5433/9094 ra Internet.
- Availability: No infrastructure redundancy required; Security type: Standard.

## Cài VM

Upload và giải nén gói vào thư mục home, rồi:

```bash
chmod +x deploy/e3-azure/*.sh
./deploy/e3-azure/bootstrap_ubuntu.sh
exit
```

SSH lại để quyền Docker có hiệu lực. Chuyển workspace sang SSD tạm của VM:

```bash
cp -a ~/e3-azure-bundle /mnt/e3-work/e3-azure
cd /mnt/e3-work/e3-azure
timedatectl status
docker compose version
```

`System clock synchronized` phải là `yes`. `/mnt` là đĩa tạm: trước khi deallocate
VM phải copy `results/` về `~/` hoặc tải xuống laptop.

## Dữ liệu đầu vào

Archive phải có dạng:

```text
data/lake/bronze/raw_aggtrades/date=YYYY-MM-DD/*.parquet
```

Trên máy nguồn Windows, gom một cửa sổ thành một file bằng image Docker đang chạy sẵn:

```powershell
.\deploy\e3-azure\export_e3_data.ps1 `
  -Date 2026-11-07 `
  -Start 2026-11-07T11:34:00 `
  -End 2026-11-07T13:34:00
```

File xuất ra nằm dưới `data/exports/e3-data/lake`; copy nó vào đúng partition
`data/lake/bronze/raw_aggtrades/date=2026-11-07/` trên VM.
Script sẽ từ chối cửa sổ có khoảng trống đầu, giữa hoặc cuối lớn hơn 10 giây;
nếu gặp `[invalid] archive co gap`, hãy chọn một cửa sổ hai giờ sạch khác.

## Chạy

VM 8 core/32 GiB, năm watermark song song:

```bash
cd /mnt/e3-work/e3-azure
tmux new -s e3
GROUP=5 RUN_JITTER=1 ./deploy/e3-azure/run_e3.sh \
  2026-11-07 2026-11-07T11:34:00 2026-11-07T13:34:00
```

Detach tmux bằng `Ctrl-b`, rồi `d`. Gắn lại bằng `tmux attach -t e3`.

Chạy smoke 30 phút trước khi chạy thật:

```bash
WMS="0 5 30" GROUP=3 RUN_JITTER=0 ./deploy/e3-azure/run_e3.sh \
  2026-11-07 2026-11-07T07:32:00 2026-11-07T08:02:00
```

Cửa sổ smoke trên đã được đóng gói sẵn (14.813 bản tin). Nó chỉ dùng để kiểm tra
môi trường; không thay thế lượt E3 chính hai giờ.

Fallback VM 4 core/16 GiB:

```bash
GROUP=2 RUN_JITTER=1 ./deploy/e3-azure/run_e3.sh DATE START END
```

## Điều kiện kết quả hợp lệ

- Log replay `trễ so với nhịp mục tiêu` không tăng dần và tối đa không quá 0,5 s.
- Không có Spark job chết; mọi `results/progress/e3_wm*.jsonl` đều có dữ liệu.
- `validate_e3.py` in `[valid]`.
- Cảnh báo `falling behind` lặp liên tục nghĩa là VM quá yếu hoặc còn tác vụ nền;
  không dùng lượt đó để kết luận latency.
- Nếu validator báo `Spark tut nhip`, chạy lại cùng dữ liệu với `GROUP=2` (hoặc dùng VM mạnh hơn).
- Kết quả chính: `results/e3/tradeoff.csv` và `results/e3/tradeoff.png`.

## Thu kết quả và dừng tính phí

```bash
mkdir -p ~/e3-results
cp -a results/e3 results/replay results/progress ~/e3-results/
tar -czf ~/e3-results.tar.gz -C ~ e3-results
```

Tải `e3-results.tar.gz` về máy. Sau đó dùng **Stop** trong Azure Portal và kiểm tra
trạng thái là `Stopped (deallocated)`, hoặc xóa cả resource group nếu không dùng nữa.
