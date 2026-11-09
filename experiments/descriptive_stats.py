"""Mảng 4 (B4.2) — Thống kê mô tả tập dữ liệu 5 cặp × 4 tháng (2022-11 và 2026-06..08).

Đọc `data/lake/silver/trades` (hoặc CSV `aggTrades` trong `data/raw/`) và bảng `candles`
(source `batch`, tự động fallback sang `official` nếu chưa nạp `batch` vì E2 đã chứng
minh hai nguồn khớp tuyệt đối 100%).

Xuất ra `results/desc/`:
  1. trades_by_month.csv          : số giao dịch (aggTrades & trades) theo cặp × tháng
  2. trades_per_minute_summary.csv: thống kê phân vị số trade/phút theo cặp × giai đoạn
     trades_per_minute.png        : boxplot phân phối số trade/phút (2022-11 vs 2026-06..08)
  3. daily_quote_volume.csv       : khối lượng USDT mỗi ngày theo cặp
     daily_quote_volume.png       : biểu đồ đường khối lượng USDT/ngày (11/2022 vs 6–8/2026)
  4. taker_buy_ratio.csv          : tỉ lệ lệnh mua chủ động (is_buyer_maker = false) theo tháng
     taker_buy_ratio.png          : biểu đồ cột tỉ lệ lệnh mua chủ động theo cặp × tháng

Chạy:
    python experiments/descriptive_stats.py
"""

from __future__ import annotations

import argparse

import _common
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import psycopg
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.dataset as ds

from bdi.batch.build_batch import select_files
from bdi.config import path, pg_dsn, settings
from bdi.ingest.download import load_manifest
from bdi.schema import AGGTRADES_CSV_COLS, KLINES_CSV_COLS

FTX_DAYS = ["2022-11-08", "2022-11-09"]


def load_candles_desc(source: str, symbols: list[str], months: list[str]) -> pd.DataFrame:
    """Đọc nến 1 phút (gồm cả quote_volume, trades) cho các tháng chỉ định."""
    sql = """
        SELECT symbol, minute, to_char(minute AT TIME ZONE 'UTC', 'YYYY-MM') AS month,
               volume::double precision AS volume,
               quote_volume::double precision AS quote_volume,
               trades, n_agg
        FROM candles
        WHERE source = %s AND symbol = ANY(%s)
          AND to_char(minute AT TIME ZONE 'UTC', 'YYYY-MM') = ANY(%s)
    """
    with psycopg.connect(pg_dsn()) as conn:
        cur = conn.execute(sql, [source, symbols, months])
        df = pd.DataFrame(cur.fetchall(), columns=[d.name for d in cur.description])
    if df.empty and source == "batch":
        print("[desc] bảng candles chưa có source='batch', dùng source='official' (E2 khớp 100%)")
        return load_candles_desc("official", symbols, months)
    df["minute"] = pd.to_datetime(df["minute"], utc=True)
    return df


def agg_trades_stats(symbols: list[str], months: list[str], force: bool = False) -> pd.DataFrame:
    """Tính số dòng aggTrades và tỉ lệ mua chủ động (is_buyer_maker = false) theo cặp × tháng.

    Ưu tiên đọc `data/lake/silver/trades` nếu đã có đủ; nếu chưa thì đọc trực tiếp từ
    file CSV `data/raw/aggTrades` bằng PyArrow đa luồng (nhanh và nhẹ RAM).
    """
    cached = _common.out_dir("desc") / "taker_buy_ratio.csv"
    if not force and cached.exists():
        df_c = pd.read_csv(cached)
        if len(df_c) == len(symbols) * len(months) and "taker_buy_quote_pct" in df_c.columns:
            return df_c

    silver_dir = path("lake") / "silver" / "trades"
    rows = []
    if silver_dir.exists():
        dataset = ds.dataset(silver_dir, format="parquet", partitioning="hive")
        for sym in symbols:
            for m in months:
                tbl = dataset.to_table(
                    filter=(ds.field("symbol") == sym) & (ds.field("period") == m),
                    columns=["is_buyer_maker", "price", "qty"],
                )
                if tbl.num_rows == 0:
                    continue
                n_agg = tbl.num_rows
                is_bm = tbl["is_buyer_maker"]
                taker_buy_n = int(pc.sum(pc.invert(is_bm).cast(pa.int64())).as_py() or 0)
                rows.append({
                    "symbol": sym,
                    "month": m,
                    "n_agg": n_agg,
                    "taker_buy_agg": taker_buy_n,
                    "taker_buy_agg_pct": 100.0 * taker_buy_n / n_agg,
                })
        if len(rows) == len(symbols) * len(months):
            return pd.DataFrame(rows)
        rows.clear()

    # Đọc trực tiếp từ CSV aggTrades bằng PyArrow streaming reader nếu đã tải đủ
    agg_files = [
        e for e in load_manifest().values()
        if e["kind"] == "aggTrades" and e["symbol"] in symbols and e["period"] in months
    ]
    if len(agg_files) == len(symbols) * len(months):
        read_opts = pacsv.ReadOptions(column_names=AGGTRADES_CSV_COLS, block_size=64 << 20)
        conv_opts = pacsv.ConvertOptions(
            include_columns=["price", "qty", "is_buyer_maker"],
            column_types={"price": pa.float64(), "qty": pa.float64(), "is_buyer_maker": pa.string()},
        )
        for e in sorted(agg_files, key=lambda x: (x["symbol"], x["period"])):
            n_agg = 0
            taker_buy_n = 0
            total_quote = 0.0
            taker_buy_quote = 0.0
            with pacsv.open_csv(e["csv"], read_options=read_opts, convert_options=conv_opts) as reader:
                for batch in reader:
                    n_agg += batch.num_rows
                    is_taker_buy = pc.invert(pc.equal(pc.ascii_lower(batch["is_buyer_maker"]), "true"))
                    taker_buy_n += int(pc.sum(is_taker_buy.cast(pa.int64())).as_py() or 0)
                    notional = pc.multiply(batch["price"], batch["qty"])
                    total_quote += float(pc.sum(notional).as_py() or 0.0)
                    taker_buy_quote += float(
                        pc.sum(pc.if_else(is_taker_buy, notional, 0.0)).as_py() or 0.0
                    )
            row = {
                "symbol": e["symbol"],
                "month": e["period"],
                "n_agg": n_agg,
                "taker_buy_agg": taker_buy_n,
                "taker_buy_agg_pct": round(100.0 * taker_buy_n / n_agg, 4),
                "taker_buy_quote_pct": round(100.0 * taker_buy_quote / total_quote, 4) if total_quote else 0.0,
            }
            rows.append(row)
            print(f"[desc] {e['symbol']} {e['period']}: n_agg={n_agg:,} taker_buy={row['taker_buy_agg_pct']:.2f}%")
        return pd.DataFrame(rows)

    # Nếu chưa tải hết 20 file aggTrades (39 GB): tính từ CSV klines (cột taker_buy_base / taker_buy_quote
    # của klines chính là tổng khối lượng các lệnh is_buyer_maker = false do Binance tổng hợp sẵn)
    print("[desc] tính tỉ lệ mua chủ động (is_buyer_maker = false) từ klines CSV...")
    read_opts = pacsv.ReadOptions(column_names=KLINES_CSV_COLS, block_size=16 << 20)
    conv_opts = pacsv.ConvertOptions(
        include_columns=["volume", "quote_volume", "trades", "taker_buy_base", "taker_buy_quote"],
        column_types={
            "volume": pa.float64(),
            "quote_volume": pa.float64(),
            "trades": pa.int64(),
            "taker_buy_base": pa.float64(),
            "taker_buy_quote": pa.float64(),
        },
    )
    for e in select_files("klines", symbols, months):
        tbl = pacsv.read_csv(e["csv"], read_options=read_opts, convert_options=conv_opts)
        vol = float(pc.sum(tbl["volume"]).as_py() or 0.0)
        qvol = float(pc.sum(tbl["quote_volume"]).as_py() or 0.0)
        tb_vol = float(pc.sum(tbl["taker_buy_base"]).as_py() or 0.0)
        tb_qvol = float(pc.sum(tbl["taker_buy_quote"]).as_py() or 0.0)
        n_trades = int(pc.sum(tbl["trades"]).as_py() or 0)
        tb_pct = round(100.0 * tb_vol / vol, 4) if vol else 0.0
        tb_qpct = round(100.0 * tb_qvol / qvol, 4) if qvol else 0.0
        rows.append({
            "symbol": e["symbol"],
            "month": e["period"],
            "n_agg": n_trades,
            "taker_buy_agg": round(n_trades * tb_pct / 100.0),
            "taker_buy_agg_pct": tb_pct,
            "taker_buy_quote_pct": tb_qpct,
        })
    return pd.DataFrame(rows)


def plot_trades_per_minute(candles: pd.DataFrame, symbols: list[str]) -> pd.DataFrame:
    """Mục 2: Boxplot phân phối số trade/phút theo cặp, so sánh 2022-11 (FTX) vs 2026-06..08."""
    c = candles.copy()
    c["regime"] = np.where(c["month"] == "2022-11", "2022-11 (FTX)", "2026-06..08 (yên)")

    # Bảng thống kê phân vị
    g = c.groupby(["symbol", "regime"])["trades"]
    summary = pd.DataFrame({
        "n_minutes": g.size(),
        "mean": g.mean().round(1),
        "p50": g.median().round(1),
        "p75": g.quantile(0.75).round(1),
        "p95": g.quantile(0.95).round(1),
        "p99": g.quantile(0.99).round(1),
        "max": g.max(),
    }).reset_index()
    summary.to_csv(_common.out_dir("desc") / "trades_per_minute_summary.csv", index=False)

    fig, ax = plt.subplots(figsize=(8.5, 4.5))
    positions_ftx = np.arange(len(symbols)) * 2.4 - 0.38
    positions_quiet = np.arange(len(symbols)) * 2.4 + 0.38

    data_ftx = [c[(c["symbol"] == s) & (c["month"] == "2022-11")]["trades"].to_numpy() for s in symbols]
    data_quiet = [c[(c["symbol"] == s) & (c["month"] != "2022-11")]["trades"].to_numpy() for s in symbols]

    bp1 = ax.boxplot(
        data_ftx, positions=positions_ftx, widths=0.62, patch_artist=True, showfliers=False,
        whis=(5, 95), medianprops={"color": "white", "linewidth": 1.6},
    )
    bp2 = ax.boxplot(
        data_quiet, positions=positions_quiet, widths=0.62, patch_artist=True, showfliers=False,
        whis=(5, 95), medianprops={"color": "white", "linewidth": 1.6},
    )
    for b in bp1["boxes"]:
        b.set(facecolor=_common.SERIES[1], edgecolor=_common.SERIES[1], alpha=0.9)
    for b in bp2["boxes"]:
        b.set(facecolor=_common.SERIES[0], edgecolor=_common.SERIES[0], alpha=0.9)

    ax.set_yscale("log")
    ax.set_xticks(np.arange(len(symbols)) * 2.4, symbols)
    ax.set_ylabel("số giao dịch (trades) / phút [thang log]")
    ax.set_title("Thống kê mô tả · Phân phối số giao dịch mỗi phút (hộp: p25–p75, râu: p5–p95)")
    ax.plot([], [], color=_common.SERIES[1], linewidth=6, label="2022-11 (sự kiện FTX)")
    ax.plot([], [], color=_common.SERIES[0], linewidth=6, label="2026-06..08 (thị trường yên)")
    ax.legend(frameon=False, loc="upper right")
    _common.save(fig, "desc", "trades_per_minute")
    return summary


def plot_daily_quote_volume(candles: pd.DataFrame, symbols: list[str]) -> pd.DataFrame:
    """Mục 3: Khối lượng giao dịch USDT mỗi ngày theo cặp (2 panel: 11/2022 và 6–8/2026)."""
    c = candles.assign(day=candles["minute"].dt.floor("D"))
    daily = (
        c.groupby(["symbol", "month", "day"], as_index=False)
        .agg(quote_volume_usdt=("quote_volume", "sum"), trades=("trades", "sum"))
    )
    daily["quote_volume_billion_usdt"] = daily["quote_volume_usdt"] / 1e9
    daily.to_csv(_common.out_dir("desc") / "daily_quote_volume.csv", index=False)

    fig, (ax1, ax2) = plt.subplots(
        1, 2, figsize=(11, 4.2), sharey=True, gridspec_kw={"width_ratios": [1, 2.4]}
    )
    for d in FTX_DAYS:
        t0 = pd.Timestamp(d, tz="UTC")
        ax1.axvspan(t0, t0 + pd.Timedelta(days=1), color=_common.GRID, alpha=0.95, lw=0)

    for i, sym in enumerate(symbols):
        d22 = daily[(daily["symbol"] == sym) & (daily["month"] == "2022-11")].sort_values("day")
        d26 = daily[(daily["symbol"] == sym) & (daily["month"] != "2022-11")].sort_values("day")
        ax1.plot(d22["day"], d22["quote_volume_billion_usdt"], color=_common.SERIES[i], linewidth=1.6, label=sym)
        ax2.plot(d26["day"], d26["quote_volume_billion_usdt"], color=_common.SERIES[i], linewidth=1.4, label=sym)

    ax1.set_title("11/2022 (vùng tô: 8–9/11 FTX)")
    ax2.set_title("Thống kê mô tả · Khối lượng USDT mỗi ngày — 6–8/2026 (thị trường yên)")
    ax1.set_ylabel("khối lượng giao dịch (tỷ USDT / ngày) [thang log]")
    ax1.set_yscale("log")
    ax1.tick_params(axis="x", rotation=25)
    ax2.tick_params(axis="x", rotation=20)
    ax2.legend(frameon=False, ncol=5, loc="upper right", fontsize=9)
    _common.save(fig, "desc", "daily_quote_volume")
    return daily


def plot_taker_buy_ratio(table_df: pd.DataFrame, symbols: list[str], months: list[str]) -> None:
    """Mục 4: Tỉ lệ lệnh mua chủ động (is_buyer_maker = false) theo cặp × tháng."""
    cols = ["symbol", "month", "n_agg", "taker_buy_agg", "taker_buy_agg_pct"]
    if "taker_buy_quote_pct" in table_df.columns:
        cols.append("taker_buy_quote_pct")
    tdf = table_df[cols].sort_values(["symbol", "month"])
    tdf.to_csv(_common.out_dir("desc") / "taker_buy_ratio.csv", index=False)

    fig, ax = plt.subplots(figsize=(8.5, 4.2))
    x = np.arange(len(symbols))
    width = 0.18
    month_colors = [_common.SERIES[1], _common.SERIES[0], _common.SERIES[2], _common.SERIES[3]]
    for j, m in enumerate(months):
        sub = tdf[tdf["month"] == m].set_index("symbol").reindex(symbols)
        offset = (j - (len(months) - 1) / 2) * width
        color = month_colors[j % len(month_colors)]
        ax.bar(x + offset, sub["taker_buy_agg_pct"], width=width * 0.92, color=color, label=m)

    ax.axhline(50.0, color=_common.TEXT_2, linestyle=":", linewidth=1.2, label="cân bằng 50%")
    ax.set_xticks(x, symbols)
    ax.set_ylim(40, 58)
    ax.set_ylabel("% lệnh mua chủ động (is_buyer_maker = false)")
    ax.set_title("Thống kê mô tả · Tỉ lệ lệnh mua chủ động (taker buy) theo cặp × tháng")
    ax.legend(frameon=False, ncol=5, loc="upper right")
    _common.save(fig, "desc", "taker_buy_ratio")


def main() -> None:
    cfg = settings()
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default="batch")
    ap.add_argument("--symbols", nargs="+", default=cfg["symbols"])
    ap.add_argument("--months", nargs="+", default=cfg["months"])
    a = ap.parse_args()

    candles = load_candles_desc(a.source, a.symbols, a.months)
    if candles.empty:
        raise SystemExit("Không có dữ liệu nến trong Postgres — chạy load_klines hoặc build_batch trước.")

    c_month = (
        candles.groupby(["symbol", "month"], as_index=False)
        .agg(
            n_minutes=("trades", "size"),
            n_trades=("trades", "sum"),
            quote_volume_usdt=("quote_volume", "sum"),
            trades_per_min_p50=("trades", "median"),
            trades_per_min_p95=("trades", lambda s: float(np.percentile(s, 95))),
            trades_per_min_max=("trades", "max"),
        )
    )
    agg_df = agg_trades_stats(a.symbols, a.months)
    merged = c_month.merge(agg_df, on=["symbol", "month"], how="left").sort_values(["symbol", "month"])
    merged["quote_volume_billion_usdt"] = (merged["quote_volume_usdt"] / 1e9).round(3)
    merged.to_csv(_common.out_dir("desc") / "trades_by_month.csv", index=False)

    print("=== 1. Số giao dịch & khối lượng theo cặp × tháng ===")
    print(merged[[
        "symbol", "month", "n_minutes", "n_agg", "n_trades",
        "quote_volume_billion_usdt", "taker_buy_agg_pct",
    ]].to_string(index=False))
    print(f"Tổng số dòng aggTrades: {int(merged['n_agg'].sum()):,}")
    print(f"Tổng số giao dịch (trades): {int(merged['n_trades'].sum()):,}")

    plot_trades_per_minute(candles, a.symbols)
    plot_daily_quote_volume(candles, a.symbols)
    plot_taker_buy_ratio(merged, a.symbols, a.months)


if __name__ == "__main__":
    main()
