#!/usr/bin/env bash
# E3 — Đường cong trade-off watermark.
#
# Ý tưởng: ghi live MỘT lần (raw archive), rồi phát lại CÓ NHỊP (speed=1) vào topic
# replay. Các job streaming (watermark 0/2/5/10/30s) đọc topic đó, mỗi job ghi nến
# với source="stream:wm=<W>s". Mọi lượt replay dùng CÙNG đoạn archive => mọi cấu hình
# thấy đúng cùng một chuỗi bản tin (công bằng).
#
# GROUP = số job chạy song song mỗi lượt replay (mặc định 2 — máy 16GB đang chạy
# luồng live). GROUP=5 => 1 lượt duy nhất (nhanh nhất, cần ~6GB RAM trống).
#
# Dùng:  experiments/e3_watermark_sweep.sh 2026-10-13 "2026-10-13T15:15" "2026-10-13T15:45"
set -euo pipefail
cd "$(dirname "$0")/.."

DATE=${1:?ngày trong archive, VD 2026-10-14}
START=${2:-}
END=${3:-}
WMS=(${WMS:-0 2 5 10 30})
GROUP=${GROUP:-2}
# TRIGGER = chu kỳ micro-batch (giây). Watermark chỉ tiến GIỮA các micro-batch nên
# trigger dài che mất bản tin đảo thứ tự nhỏ; TRIGGER=0 = chạy liên tục nhanh nhất có thể.
TRIGGER=${TRIGGER:-2}
# Tiêm độ trễ có kiểm soát vào replay, VD: JITTER_PCT=5 JITTER_MAX=20 (5% bản tin trễ ≤ 20s)
JITTER_PCT=${JITTER_PCT:-0}
JITTER_MAX=${JITTER_MAX:-0}
SUF=""; LBL=""
[[ "$TRIGGER" != "2" ]] && { SUF="${SUF}_t$TRIGGER"; LBL="${LBL}:t=${TRIGGER}s"; }
[[ "$JITTER_PCT" != "0" ]] && { SUF="${SUF}_j${JITTER_PCT}x${JITTER_MAX}"; LBL="${LBL}:j=${JITTER_PCT}%/${JITTER_MAX}s"; }
mkdir -p results/e3/configs
export SPARK_DRIVER_MEMORY=${SPARK_DRIVER_MEMORY:-1g}
export SPARK_SHUFFLE_PARTITIONS=${SPARK_SHUFFLE_PARTITIONS:-4}
mkdir -p results/e3/logs results/replay

start_job() {  # $1 = watermark
  rm -rf "data/checkpoints/stream_candles_e3_wm$1$SUF"
  python -m bdi.stream.stream_candles --topic binance.aggtrades.replay --trigger "$TRIGGER" \
    --watermark "$1" --source "stream:wm=${1}s$LBL" --run-id "e3_wm$1$SUF" --starting-offsets latest \
    > "results/e3/logs/wm$1$SUF.log" 2>&1 &
  echo $!
}

for ((i = 0; i < ${#WMS[@]}; i += GROUP)); do
  batch=("${WMS[@]:i:GROUP}")
  rid="e3_round$((i / GROUP))$SUF"
  echo "[e3] lượt $rid: watermark ${batch[*]}"
  PIDS=()
  for w in "${batch[@]}"; do PIDS+=("$(start_job "$w")"); done
  echo "[e3] đợi 90s cho các job Spark khởi động…"; sleep 90
  python -m bdi.ingest.replay --from-archive --date "$DATE" ${START:+--start "$START"} \
    ${END:+--end "$END"} --speed 1 --run-id "$rid" --jitter-pct "$JITTER_PCT" --jitter-max "$JITTER_MAX"
  # metadata replay theo từng watermark để e3_analyze quy đổi độ trễ
  for w in "${batch[@]}"; do
    cp "results/replay/$rid.json" "results/replay/e3_wm$w$SUF.json"
    printf '{"run_id":"e3_wm%s%s","source":"stream:wm=%ss%s","watermark_s":%s,"trigger_s":%s,"jitter_pct":%s,"jitter_max_s":%s}\n' \
      "$w" "$SUF" "$w" "$LBL" "$w" "$TRIGGER" "$JITTER_PCT" "$JITTER_MAX" > "results/e3/configs/e3_wm$w$SUF.json"
  done
  echo "[e3] đợi watermark lớn nhất + 90s để xả nến cuối…"; sleep $(( ${batch[${#batch[@]}-1]} + 90 ))   # bash 3.2 (macOS) không có chỉ số âm
  kill "${PIDS[@]}" 2>/dev/null || true
  wait 2>/dev/null || true
done
echo "[e3] xong. Phân tích: python experiments/e3_analyze.py"
