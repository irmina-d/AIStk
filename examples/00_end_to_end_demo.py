"""
00_end_to_end_demo.py — End-to-End AIS Demo (CSV → Stats → Events → Map → Streaming)

This script shows a complete workflow using the `aistk` toolkit:
1) Configure input paths and parameters
2) Load & filter with AISDataset
3) Compute statistics (eager and streaming-friendly)
4) Detect events
5) Generate a quick map preview (Folium)
6) Simulate online streaming from CSV and emit events

Adjust the constants in the CONFIG section to point at your data.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from aistk.core import AISDataset
from aistk.stats import compute_stats_df
from aistk.stats_streaming import compute_stats_lazy
from aistk.events import detect_events_df
from aistk.streaming.events_online import process_stream


# ------------------------------------------------------------------
# CONFIG — adjust to your environment
# ------------------------------------------------------------------
CSV_ROOT = "data/ais"           # directory with CSV(s) OR path to a single CSV
CSV_PATTERN = "*.csv"           # e.g. '*.csv' or '2024.csv'
DATE_FROM = "2024-01-01"
DATE_TO   = "2024-02-01"
MMSI_LIST = [244660000]          # set to [] or None to include all

OUT_PARQUET = Path("out/demo_data.parquet")
OUT_STATS   = Path("out/demo_stats.parquet")
OUT_EVENTS  = Path("out/demo_events.parquet")
OUT_MAP     = Path("out/demo_map.html")


def main() -> None:
    OUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)

    print("[1/9] Build dataset & collect subset…")
    ds = AISDataset(CSV_ROOT, pattern=CSV_PATTERN)
    if MMSI_LIST:
        ds = ds.filter(mmsi=MMSI_LIST)
    ds = ds.between(DATE_FROM, DATE_TO)
    df = ds.collect()
    print("  rows:", df.height, "cols:", df.columns)

    print("[2/9] Save filtered subset to Parquet…", OUT_PARQUET)
    ds.write_parquet(str(OUT_PARQUET))

    print("[3/9] Stats (eager Polars DataFrame)…")
    stats_df = compute_stats_df(df, level="mmsi")
    print(stats_df.sort("distance_km", descending=True).head())
    print("[4/9] Save stats to:", OUT_STATS)
    stats_df.write_parquet(str(OUT_STATS))

    print("[5/9] Stats (streaming-friendly with Polars Lazy)…")
    lf = AISDataset(CSV_ROOT, pattern=CSV_PATTERN)._build()
    if MMSI_LIST:
        lf = lf.filter(pl.col("MMSI").is_in(MMSI_LIST))
    lf = lf.filter((pl.col("ts") >= pl.lit(DATE_FROM)) & (pl.col("ts") < pl.lit(DATE_TO)))
    res_lazy = compute_stats_lazy(lf, level="mmsi").collect(engine="streaming")
    print(res_lazy.sort("distance_km", descending=True).head())

    print("[6/9] Event detection (batch)…")
    ev = detect_events_df(df, turn_deg=30.0, stop_sog=0.5, stop_min=15, draft_jump_m=0.3)
    print(ev.head())
    print("[7/9] Save events to:", OUT_EVENTS)
    ev.write_parquet(str(OUT_EVENTS))

    print("[8/9] Quick Folium map preview…", OUT_MAP)
    html_path = ds.plot_map(str(OUT_MAP), mmsi=MMSI_LIST[0] if MMSI_LIST else None)
    print("  wrote:", html_path)

    print("[9/9] Streaming simulation from CSV (chunked)…\n   This prints JSON events as they occur.")
    lf_stream = pl.scan_csv(f"{CSV_ROOT}/{CSV_PATTERN}", has_header=True, infer_schema_length=0, ignore_errors=True, try_parse_dates=True)
    lf_stream_schema = set(lf_stream.collect_schema().names())
    if MMSI_LIST and "MMSI" in lf_stream_schema:
        lf_stream = lf_stream.filter(pl.col("MMSI").is_in(MMSI_LIST))

    chunk_size = 20_000
    offset = 0
    total_events = 0
    while True:
        chunk = lf_stream.slice(offset, chunk_size).collect(engine="streaming")
        if chunk.height == 0:
            break
        cols = [c for c in ["MMSI","ts","LAT","LON","COG","SOG","Draft"] if c in chunk.columns]
        recs = (dict(zip(cols, row)) for row in chunk.select(cols).iter_rows())
        for event in process_stream(recs, stop_min=10):
            print(json.dumps(event))
            total_events += 1
        offset += chunk_size
    print(f"Streaming finished. Emitted events: {total_events}")


if __name__ == "__main__":
    main()
