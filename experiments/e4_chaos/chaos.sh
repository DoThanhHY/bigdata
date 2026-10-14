#!/usr/bin/env bash
# E4 — Fault injection. Mỗi kịch bản ghi mốc inject/recover vào bảng chaos_events
# để e4_analyze.py vẽ timeline và đếm nến thiếu/trùng.
#
#   experiments/e4_chaos/chaos.sh ws_kill        # kill bộ nhận WS 60s rồi bật lại
#   experiments/e4_chaos/chaos.sh ws_netcut      # "rút dây mạng": pause container 90s
#   experiments/e4_chaos/chaos.sh spark_kill     # docker kill job streaming
#   experiments/e4_chaos/chaos.sh kafka_restart  # restart broker
#   experiments/e4_chaos/chaos.sh batch_rerun    # chạy lại cùng 1 lô batch 2 lần
#   experiments/e4_chaos/chaos.sh all            # lần lượt 4 kịch bản streaming, cách nhau 10 phút
#
# Kịch bản "WS ngắt tự nhiên sau 24h" không cần gây lỗi: chỉ cần để producer chạy
# >24h rồi xem bảng ws_gaps (reason = proactive_24h hoặc ConnectionClosed…).
set -euo pipefail
cd "$(dirname "$0")/../.."

DOWN=${DOWN:-60}
DC="docker compose"

mark() {  # $1 scenario, $2 phase, $3 note
  $DC exec -T postgres psql -q -U bdi -d bdi \
    -c "INSERT INTO chaos_events (scenario, phase, note) VALUES ('$1', '$2', '${3:-}')"
  echo "[$(date -u +%H:%M:%S)] $1 $2 ${3:-}"
}

ws_kill() {
  mark ws_kill inject "docker kill ws-producer, down ${DOWN}s"
  $DC kill ws-producer; sleep "$DOWN"
  $DC start ws-producer
  mark ws_kill recover
}

ws_netcut() {
  # pause = đóng băng tiến trình: TCP treo, Binance tự ngắt phía server — giống mất mạng
  local d=$((DOWN + 30))
  mark ws_netcut inject "docker pause ws-producer ${d}s"
  $DC pause ws-producer; sleep "$d"
  $DC unpause ws-producer
  mark ws_netcut recover
}

spark_kill() {
  mark spark_kill inject "docker kill stream-candles"
  $DC kill stream-candles; sleep "$DOWN"
  $DC start stream-candles
  mark spark_kill recover
}

kafka_restart() {
  mark kafka_restart inject "docker restart kafka"
  $DC restart kafka
  mark kafka_restart recover
}

batch_rerun() {
  local sym=${SYMBOL:-BTCUSDT} per=${PERIOD:-2026-08}
  mark batch_rerun inject "$sym $per"
  python -m bdi.batch.build_batch --symbols "$sym" --periods "$per"
  python - <<EOF > results/e4/batch_rerun_1.txt
import sys; sys.path.insert(0, "src")
from bdi.sink import table_fingerprint
print(table_fingerprint("source='batch' AND symbol='$sym'"))
EOF
  python -m bdi.batch.build_batch --symbols "$sym" --periods "$per"
  python - <<EOF > results/e4/batch_rerun_2.txt
import sys; sys.path.insert(0, "src")
from bdi.sink import table_fingerprint
print(table_fingerprint("source='batch' AND symbol='$sym'"))
EOF
  mark batch_rerun recover
  if diff -q results/e4/batch_rerun_1.txt results/e4/batch_rerun_2.txt >/dev/null; then
    echo "[ok] idempotent: $(cat results/e4/batch_rerun_1.txt)"
  else
    echo "[FAIL] số dòng/checksum thay đổi sau lần chạy lại"; exit 1
  fi
}

mkdir -p results/e4
case "${1:?kịch bản}" in
  all) for s in ws_kill ws_netcut spark_kill kafka_restart; do $s; sleep 600; done ;;
  ws_kill|ws_netcut|spark_kill|kafka_restart|batch_rerun) "$1" ;;
  *) echo "kịch bản không hợp lệ: $1"; exit 2 ;;
esac
