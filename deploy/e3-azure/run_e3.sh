#!/usr/bin/env bash
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
export E3_ROOT="$ROOT"
COMPOSE=(docker compose -p bdi-e3 -f "$HERE/docker-compose.yml")

DATE=${1:?Dung: run_e3.sh YYYY-MM-DD START_UTC END_UTC}
START=${2:?Thieu START_UTC, vi du 2026-11-07T11:34:00}
END=${3:?Thieu END_UTC, vi du 2026-11-07T13:34:00}

read -r -a WMS_ARR <<< "${WMS:-0 2 5 10 30}"
GROUP=${GROUP:-5}
TRIGGER=${TRIGGER:-2}
STARTUP_WAIT=${STARTUP_WAIT:-75}
FLUSH_WAIT=${FLUSH_WAIT:-75}
RUN_JITTER=${RUN_JITTER:-1}
JITTER_PCT=${JITTER_PCT:-5}
JITTER_MAX=${JITTER_MAX:-20}
MAX_REPLAY_LAG_S=${MAX_REPLAY_LAG_S:-0.5}

ARCHIVE="$ROOT/data/lake/bronze/raw_aggtrades/date=$DATE"
if [[ ! -d "$ARCHIVE" ]] || ! find "$ARCHIVE" -maxdepth 1 -name '*.parquet' -print -quit | grep -q .; then
  echo "[fatal] Khong co archive Parquet: $ARCHIVE" >&2
  exit 2
fi

mkdir -p "$ROOT/results/e3/configs" "$ROOT/results/e3/logs" \
         "$ROOT/results/replay" "$ROOT/results/progress" "$ROOT/data/checkpoints"

active_jobs=()
cleanup_jobs() {
  if ((${#active_jobs[@]})); then
    docker stop -t 30 "${active_jobs[@]}" >/dev/null 2>&1 || true
    docker rm -f "${active_jobs[@]}" >/dev/null 2>&1 || true
  fi
}
trap cleanup_jobs EXIT INT TERM

echo "[e3] Build image va khoi dong Kafka/Postgres"
"${COMPOSE[@]}" build e3-cli e3-job
mkdir -p "$ROOT/.runtime/kafka" "$ROOT/.runtime/postgres"
# Cac image chay bang UID rieng; cap quyen cho bind mount tren SSD tam cua VM.
chmod 0777 "$ROOT/.runtime/kafka" "$ROOT/.runtime/postgres"
"${COMPOSE[@]}" up -d --wait --wait-timeout 180 kafka postgres
"${COMPOSE[@]}" run --rm --no-deps kafka-init

echo "[e3] Nap official REST cho $START .. $END"
"${COMPOSE[@]}" run --rm --no-deps e3-cli \
  python -m bdi.ingest.rest_klines --start "$START" --end "$END"

run_series() {
  local jitter_pct=$1
  local jitter_max=$2
  local suffix=""
  local label=""
  if [[ "$jitter_pct" != "0" ]]; then
    suffix="_j${jitter_pct}x${jitter_max}"
    label=":j=${jitter_pct}%/${jitter_max}s"
  fi

  local i round rid replay_log w job max_w
  for ((i=0; i<${#WMS_ARR[@]}; i+=GROUP)); do
    local batch=("${WMS_ARR[@]:i:GROUP}")
    round=$((i / GROUP))
    rid="e3_round${round}${suffix}"
    active_jobs=()
    echo "[e3] $rid: watermark ${batch[*]}"

    for w in "${batch[@]}"; do
      rm -rf "$ROOT/data/checkpoints/stream_candles_e3_wm${w}${suffix}"
      rm -f "$ROOT/results/progress/e3_wm${w}${suffix}.jsonl"
      job="bdie3-${rid}-w${w}"
      docker rm -f "$job" >/dev/null 2>&1 || true
      "${COMPOSE[@]}" run -d --no-deps --name "$job" e3-job \
        python -m bdi.stream.stream_candles \
          --topic binance.aggtrades.replay --trigger "$TRIGGER" \
          --watermark "$w" --source "stream:wm=${w}s${label}" \
          --run-id "e3_wm${w}${suffix}" --starting-offsets latest >/dev/null
      active_jobs+=("$job")
    done

    echo "[e3] Doi ${STARTUP_WAIT}s cho Spark khoi dong"
    sleep "$STARTUP_WAIT"
    for job in "${active_jobs[@]}"; do
      if [[ $(docker inspect -f '{{.State.Running}}' "$job") != true ]]; then
        docker logs "$job" >&2 || true
        echo "[fatal] Spark job $job da dung som" >&2
        exit 3
      fi
    done

    replay_log="$ROOT/results/e3/logs/${rid}_replay.log"
    "${COMPOSE[@]}" run --rm --no-deps e3-cli \
      python -m bdi.ingest.replay --from-archive --date "$DATE" \
        --start "$START" --end "$END" --speed 1 --run-id "$rid" \
        --jitter-pct "$jitter_pct" --jitter-max "$jitter_max" | tee "$replay_log"

    for w in "${batch[@]}"; do
      cp "$ROOT/results/replay/$rid.json" "$ROOT/results/replay/e3_wm${w}${suffix}.json"
      printf '{"run_id":"e3_wm%s%s","source":"stream:wm=%ss%s","watermark_s":%s,"trigger_s":%s,"jitter_pct":%s,"jitter_max_s":%s}\n' \
        "$w" "$suffix" "$w" "$label" "$w" "$TRIGGER" "$jitter_pct" "$jitter_max" \
        > "$ROOT/results/e3/configs/e3_wm${w}${suffix}.json"
    done

    max_w=${batch[0]}
    for w in "${batch[@]}"; do ((w > max_w)) && max_w=$w; done
    echo "[e3] Doi $((max_w + FLUSH_WAIT))s de day watermark"
    sleep $((max_w + FLUSH_WAIT))

    for job in "${active_jobs[@]}"; do
      docker logs "$job" > "$ROOT/results/e3/logs/${job}.log" 2>&1 || true
    done
    cleanup_jobs
    active_jobs=()
  done
}

run_series 0 0
if [[ "$RUN_JITTER" == "1" ]]; then
  run_series "$JITTER_PCT" "$JITTER_MAX"
fi

echo "[e3] Phan tich"
"${COMPOSE[@]}" run --rm --no-deps e3-cli python experiments/e3_analyze.py
"${COMPOSE[@]}" run --rm --no-deps e3-cli \
  python deploy/e3-azure/validate_e3.py \
    --max-replay-lag "$MAX_REPLAY_LAG_S" --max-batch-utilization 1.0

echo "[e3] HOAN TAT"
echo "  $ROOT/results/e3/tradeoff.csv"
echo "  $ROOT/results/e3/tradeoff.png"
