"""Online-style event detection from the bundled sample CSV, read in chunks."""

import json

import polars as pl

from aistk.streaming.events_online import process_stream


def main() -> None:
    path = "data/sample/ais_sample.csv"
    chunk_size = 3

    lf = (
        pl.scan_csv(path, has_header=True, infer_schema_length=0, ignore_errors=True)
        .with_columns(pl.col("BaseDateTime").str.to_datetime(strict=False).dt.epoch("ms").alias("ts"))
    )

    offset = 0
    emitted = 0
    while True:
        df = lf.slice(offset, chunk_size).collect(engine="streaming")
        if df.height == 0:
            break
        cols = [c for c in ["MMSI", "ts", "LAT", "LON", "COG", "SOG", "Draft"] if c in df.columns]
        records = (dict(zip(cols, row)) for row in df.select(cols).iter_rows())
        for event in process_stream(records, stop_min=10):
            print(json.dumps(event))
            emitted += 1
        offset += chunk_size
    print(f"Streaming finished. Emitted events: {emitted}")


if __name__ == "__main__":
    main()
