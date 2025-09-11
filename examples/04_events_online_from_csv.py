"""
Online (incremental) event detection from a CSV, read in chunks.
This simulates a streaming input (e.g., tail -f or Kafka consumer).
"""
import json
import polars as pl
from aistk.streaming.events_online import process_stream

def main():
    path = "data/ais/2024.csv"
    chunk = 20_000

    lf = pl.scan_csv(path, has_header=True, infer_schema_length=0, ignore_errors=True, try_parse_dates=True)

    offset = 0
    while True:
        df = lf.slice(offset, chunk).collect(streaming=True)
        if df.height == 0:
            break
        cols = [c for c in ["MMSI","ts","LAT","LON","COG","SOG","Draft"] if c in df.columns]
        recs = (dict(zip(cols, row)) for row in df.select(cols).iter_rows())
        for ev in process_stream(recs, stop_min=10):
            print(json.dumps(ev))
        offset += chunk

if __name__ == "__main__":
    main()
