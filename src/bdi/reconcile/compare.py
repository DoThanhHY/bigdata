"""C3 — Khung đối soát 3 nguồn (batch / stream / official).

    python -m bdi.reconcile.compare --left batch --right official --run-id e2
    python -m bdi.reconcile.compare --left stream --right official \
        --start 2026-10-14 --end 2026-10-15 --run-id live_d1 --with-late 5

Kết quả:
  - bảng Postgres reconcile_results (mỗi nến 1 dòng, có nhãn nguyên nhân)
  - results/reconcile/<run_id>_summary.csv  (% khớp từng trường theo symbol × tháng)
  - results/reconcile/<run_id>_causes.csv   (phân bố nguyên nhân lệch)
"""

from __future__ import annotations

import argparse
import json

import pandas as pd
import psycopg

from bdi.config import path, pg_dsn
from bdi.reconcile.taxonomy import label
from bdi.sink import upsert_rows

FIELDS = ["open", "high", "low", "close", "volume", "trades"]


def load_candles(source: str, symbols: list[str] | None = None, start: str | None = None,
                 end: str | None = None, dsn: str | None = None) -> pd.DataFrame:
    sql = "SELECT symbol, minute, open, high, low, close, volume, trades, emitted_at FROM candles WHERE source = %s"
    params: list = [source]
    if symbols:
        sql += " AND symbol = ANY(%s)"
        params.append(symbols)
    if start:
        sql += " AND minute >= %s"
        params.append(start)
    if end:
        sql += " AND minute < %s"
        params.append(end)
    with psycopg.connect(dsn or pg_dsn()) as conn:
        cur = conn.execute(sql, params)
        df = pd.DataFrame(cur.fetchall(), columns=[d.name for d in cur.description])
    df["minute"] = pd.to_datetime(df["minute"], utc=True)
    return df


def compare(left: pd.DataFrame, right: pd.DataFrame, how: str = "outer") -> pd.DataFrame:
    """Join theo (symbol, minute); so khớp TUYỆT ĐỐI (Decimal == Decimal) từng trường."""
    m = left.merge(right, on=["symbol", "minute"], how=how, suffixes=("_l", "_r"))
    for f in FIELDS:
        m[f"match_{f}"] = m[f"{f}_l"].notna() & m[f"{f}_r"].notna() & (m[f"{f}_l"] == m[f"{f}_r"])
    m["all_match"] = m[[f"match_{f}" for f in FIELDS]].all(axis=1)
    return m


def summarize(m: pd.DataFrame) -> pd.DataFrame:
    g = m.assign(month=m["minute"].dt.strftime("%Y-%m")).groupby(["symbol", "month"])
    cols = [f"match_{f}" for f in FIELDS] + ["all_match"]
    out = g[cols].mean().mul(100).round(4)
    out["n_minutes"] = g.size()
    out["missing_left"] = g["trades_l"].apply(lambda s: s.isna().sum())
    out["missing_right"] = g["trades_r"].apply(lambda s: s.isna().sum())
    return out.reset_index()


# ---------------- ngữ cảnh cho taxonomy ----------------

def load_gaps(dsn: str | None = None) -> pd.DataFrame:
    with psycopg.connect(dsn or pg_dsn()) as conn:
        cur = conn.execute("SELECT symbol, disconnected_at, coalesce(reconnected_at, now()) FROM ws_gaps")
        g = pd.DataFrame(cur.fetchall(), columns=["symbol", "disconnected_at", "reconnected_at"])
    for c in ("disconnected_at", "reconnected_at"):
        g[c] = pd.to_datetime(g[c], utc=True)
    return g


def load_raw(start: str | None, end: str | None) -> pd.DataFrame:
    """Bản tin raw từ archive: symbol, agg_id, p, q (chuỗi), ts_us, recv_ts_us, origin, thứ tự Kafka."""
    import pyarrow.dataset as ds

    from bdi.timeutil import to_micros

    t = ds.dataset(path("lake") / "bronze" / "raw_aggtrades", format="parquet", partitioning="hive").to_table(
        columns=["value", "kafka_partition", "kafka_offset", "kafka_ts"])
    df = t.to_pandas()
    js = pd.json_normalize(df["value"].map(json.loads))
    df = pd.concat([df.drop(columns="value"), js[["s", "a", "p", "q", "T", "recv_ts_us", "origin"]]], axis=1)
    df = df.rename(columns={"s": "symbol", "a": "agg_id"})
    df["ts_us"] = df["T"].map(to_micros)
    df["minute"] = pd.to_datetime(df["ts_us"] // 60_000_000 * 60, unit="s", utc=True)
    if start:
        df = df[df["minute"] >= pd.Timestamp(start, tz="UTC")]
    if end:
        df = df[df["minute"] < pd.Timestamp(end, tz="UTC")]
    return df


def simulate_late(raw: pd.DataFrame, watermark_s: float) -> pd.DataFrame:
    """Bản tin nào bị Spark coi là trễ với watermark W?

    Mô phỏng theo từng bản tin (theo thứ tự đến Kafka): watermark = max(event_time
    đã thấy) − W; bản tin có event_time < watermark bị loại. Spark chỉ cập nhật
    watermark giữa các micro-batch nên con số thật ≤ con số mô phỏng (cận trên).
    """
    r = raw.sort_values(["kafka_ts", "kafka_partition", "kafka_offset"])
    wm = r["ts_us"].cummax().shift(1) - int(watermark_s * 1_000_000)
    r = r.assign(is_late=r["ts_us"] < wm)
    return (r[r["is_late"]].groupby(["symbol", "minute"]).size()
            .rename("late_count").reset_index())


def count_duplicates(raw: pd.DataFrame) -> pd.DataFrame:
    d = raw[raw.duplicated(["symbol", "agg_id"], keep="first")]
    return d.groupby(["symbol", "minute"]).size().rename("dup_count").reset_index()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--left", required=True, help="nguồn kiểm tra: batch | stream | stream:wm=5s ...")
    ap.add_argument("--right", default="official")
    ap.add_argument("--symbols", nargs="*")
    ap.add_argument("--start")
    ap.add_argument("--end")
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--with-late", type=float, help="watermark (giây) để mô phỏng bản tin trễ từ raw archive")
    ap.add_argument("--inner", action="store_true", help="chỉ so các phút cả hai bên đều có")
    a = ap.parse_args()

    left = load_candles(a.left, a.symbols, a.start, a.end)
    right = load_candles(a.right, a.symbols, a.start, a.end)
    if left.empty:
        raise SystemExit(f"Không có nến nào với source={a.left}")
    # chỉ so trong khoảng thời gian mà nguồn trái có dữ liệu (stream chỉ chạy vài ngày)
    lo, hi = left["minute"].min(), left["minute"].max()
    right = right[(right["minute"] >= lo) & (right["minute"] <= hi)]

    m = compare(left, right, "inner" if a.inner else "outer")
    gaps = load_gaps() if a.left.startswith("stream") else None
    late = dups = None
    if a.with_late is not None:
        raw = load_raw(a.start, a.end)
        late, dups = simulate_late(raw, a.with_late), count_duplicates(raw)
    m["cause"] = label(m, gaps, late, dups)

    out_dir = path("results") / "reconcile"
    out_dir.mkdir(parents=True, exist_ok=True)
    s = summarize(m)
    s.to_csv(out_dir / f"{a.run_id}_summary.csv", index=False)
    causes = m.loc[~m["all_match"], "cause"].value_counts().rename_axis("cause").reset_index(name="n")
    causes.to_csv(out_dir / f"{a.run_id}_causes.csv", index=False)
    m.loc[~m["all_match"]].to_csv(out_dir / f"{a.run_id}_mismatches.csv.gz", index=False)

    cols = ["run_id", "symbol", "minute", "left_src", "right_src", "match_open", "match_high", "match_low",
            "match_close", "match_volume", "match_trades", "all_match", "cause"]
    rows = (
        (a.run_id, r.symbol, r.minute.to_pydatetime(), a.left, a.right, r.match_open, r.match_high, r.match_low,
         r.match_close, r.match_volume, r.match_trades, r.all_match, None if pd.isna(r.cause) else r.cause)
        for r in m.itertuples(index=False)
    )
    upsert_rows("reconcile_results", cols, ["run_id", "symbol", "minute", "left_src", "right_src"], rows)

    print(s.to_string(index=False))
    print("\nNguyên nhân lệch:\n", causes.to_string(index=False))


if __name__ == "__main__":
    main()
