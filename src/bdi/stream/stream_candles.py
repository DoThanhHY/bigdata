"""Tầng streaming: Kafka -> canonical -> watermark -> loại trùng -> build_candles -> upsert.

Dùng CHUNG build_candles với tầng batch (C2).

    # luồng live chính
    python -m bdi.stream.stream_candles --watermark 5

    # E3: một cấu hình watermark trên topic replay
    python -m bdi.stream.stream_candles --topic binance.aggtrades.replay \
        --watermark 10 --source stream:wm=10s --run-id e3_wm10 --starting-offsets latest

    # C5: ghi mọi phiên bản nến để đo time-to-convergence
    python -m bdi.stream.stream_candles --output-mode update --source stream:update

Mỗi micro-batch ghi một dòng tiến độ vào results/progress/<run_id>.jsonl
(inputRowsPerSecond, processedRowsPerSecond, độ trễ batch) cho E3/E7 stress test.
"""

from __future__ import annotations

import argparse
import json
import time

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from bdi.candles import build_candles
from bdi.config import path, settings
from bdi.lineage import with_lineage
from bdi.schema import canonical_from_ws_json
from bdi.sink import append_versions, upsert_candles
from bdi.spark import get_spark


def make_foreach_batch(source: str, topic: str, watermark_s: int, output_mode: str, anomaly: bool,
                       anomaly_history: str | None = None):
    def _fn(batch: DataFrame, batch_id: int) -> None:
        if batch.isEmpty():
            return
        # mỗi micro-batch chỉ vài chục nến: gộp 1 partition => 1 kết nối Postgres thay vì 16
        batch = batch.coalesce(1).persist()
        out = with_lineage(batch, source, f"kafka:{topic}", f"{watermark_s}s")
        upsert_candles(out)
        if output_mode == "update":
            append_versions(out, batch_id)
        if anomaly:
            from bdi.anomaly.mzscore import detect_for_batch
            detect_for_batch(batch, source, anomaly_history)
        batch.unpersist()
    return _fn


def main() -> None:
    cfg = settings()
    sc = cfg["stream"]
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", default=cfg["kafka"]["topic_live"])
    ap.add_argument("--watermark", type=int, default=sc["watermark_seconds"], help="giây")
    ap.add_argument("--trigger", type=int, default=sc["trigger_seconds"], help="giây")
    ap.add_argument("--output-mode", choices=["append", "update"], default=sc["output_mode"])
    ap.add_argument("--source", default="stream")
    ap.add_argument("--run-id", default="live")
    ap.add_argument("--starting-offsets", default="latest")
    ap.add_argument("--max-offsets-per-trigger", type=int)
    ap.add_argument("--no-dedup", action="store_true", help="tắt loại trùng (minh họa nhãn DUPLICATE)")
    ap.add_argument("--anomaly", action="store_true", help="E7: phát hiện bất thường trên stream")
    ap.add_argument("--anomaly-history", help="nguồn bù lịch sử cho detector, VD official")
    a = ap.parse_args()

    spark = get_spark(f"stream_candles_{a.run_id}", kafka=True)
    reader = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", cfg["kafka"]["bootstrap"])
              .option("subscribe", a.topic)
              .option("startingOffsets", a.starting_offsets)
              .option("failOnDataLoss", "false"))
    if a.max_offsets_per_trigger:
        reader = reader.option("maxOffsetsPerTrigger", a.max_offsets_per_trigger)

    trades = (reader.load()
              .select(*canonical_from_ws_json(F.col("value").cast("string")))
              .withWatermark("event_time", f"{a.watermark} seconds"))

    # Nhiều toán tử có trạng thái nối tiếp chỉ được hỗ trợ ở append mode
    if not a.no_dedup and a.output_mode == "append":
        trades = trades.dropDuplicatesWithinWatermark(["symbol", "agg_id"])
    elif not a.no_dedup:
        print("[warn] output_mode=update: bỏ loại trùng trong Spark (dựa vào producer + upsert)")

    candles = build_candles(trades)

    ckpt = path("checkpoints") / f"stream_candles_{a.run_id}"
    q = (candles.writeStream
         .outputMode(a.output_mode)
         .foreachBatch(make_foreach_batch(a.source, a.topic, a.watermark, a.output_mode, a.anomaly,
                                          a.anomaly_history))
         .option("checkpointLocation", str(ckpt))
         .trigger(processingTime=f"{a.trigger} seconds")
         .queryName(f"candles_{a.run_id}")
         .start())

    prog = path("results") / "progress" / f"{a.run_id}.jsonl"
    prog.parent.mkdir(parents=True, exist_ok=True)
    last_id = -1
    with open(prog, "a") as f:
        while q.isActive:
            p = q.lastProgress
            if p and p["batchId"] != last_id:
                last_id = p["batchId"]
                f.write(json.dumps({
                    "run_id": a.run_id, "batch_id": p["batchId"], "timestamp": p["timestamp"],
                    "num_input_rows": p["numInputRows"],
                    "input_rows_per_second": p.get("inputRowsPerSecond"),
                    "processed_rows_per_second": p.get("processedRowsPerSecond"),
                    "batch_duration_ms": p["durationMs"].get("triggerExecution"),
                    "watermark": p.get("eventTime", {}).get("watermark"),
                    "state_rows": sum(o.get("numRowsTotal", 0) for o in p.get("stateOperators", [])),
                    "late_rows_dropped": sum(o.get("numRowsDroppedByWatermark", 0)
                                             for o in p.get("stateOperators", [])),
                }) + "\n")
                f.flush()
            time.sleep(1)
    q.awaitTermination()


if __name__ == "__main__":
    main()
