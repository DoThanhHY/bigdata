#!/usr/bin/env bash
# E7 — Replay FTX qua pipeline streaming (1×/10×/100×) + stress test velocity.
#
#   experiments/e7_stress.sh ftx       # replay 6–10/11/2022 BTCUSDT ở FTX_SPEED× (mặc định 100×, ~58 phút)
#   experiments/e7_stress.sh stress    # tăng tốc dần tới khi p95 > 10s
#
# Detector dùng baseline 1 ngày (W=1440) và MAD xấp xỉ cần 2W−1 phút lịch sử, nên replay
# bắt đầu từ 6/11 (2 ngày khởi động) trước 8–9/11. 4 ngày dữ liệu ở 100× ≈ 58 phút.
set -euo pipefail
cd "$(dirname "$0")/.."

CSV=${CSV:-data/raw/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2022-11.csv}
mkdir -p results/e7/logs

run_one() {  # $1 speed, $2 start, $3 end, $4 extra flags cho stream job, $5 tiền tố run_id
  local id="${5:-e7_x}$1"
  rm -rf "data/checkpoints/stream_candles_$id"
  python -m bdi.stream.stream_candles --topic binance.aggtrades.replay --watermark 5 \
    --source "stream:$id" --run-id "$id" --starting-offsets latest $4 \
    > "results/e7/logs/$id.log" 2>&1 &
  local pid=$!
  sleep 60
  python -m bdi.ingest.replay --from-csv "$CSV" --symbol BTCUSDT --speed "$1" \
    --start "$2" --end "$3" --run-id "$id"
  sleep 60; kill "$pid" || true
  python experiments/e7_stress_analyze.py --run-id "$id"
}

case "${1:?ftx|stress}" in
  ftx)
    run_one "${FTX_SPEED:-100}" 2022-11-06T00:00 2022-11-10T00:00 --anomaly e7_ftx_x ;;
  stress)
    # mỗi mức tốc độ phát lại cùng 20 phút sôi động nhất của FTX
    for s in ${SPEEDS:-1 10 50 100 200 500 1000}; do
      run_one "$s" 2022-11-09T14:00 2022-11-09T14:20 ""
      p95=$(python -c "import json;print(json.load(open('results/e7/stress_e7_x$s.json'))['latency_p95_s'])")
      echo "[stress] speed=${s}x p95=${p95}s"
      if python -c "import sys; sys.exit(0 if float('$p95') > 10 else 1)"; then
        echo "[stress] vượt 10s ở ${s}x — dừng"; break
      fi
    done
    python experiments/e7_stress_analyze.py --summary ;;
esac
