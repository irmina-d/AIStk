"""
00_end_to_end_demo.py — End-to-End AIS Demo using bundled sample data.

Workflow: CSV → filtered Parquet → trajectory statistics → vessel-wise events.
The Folium map step is optional and is skipped when the optional `viz` extra is
not installed.
"""

from __future__ import annotations

from pathlib import Path

from aistk import AISDataset

CSV_ROOT = "data/sample"
CSV_PATTERN = "ais_sample.csv"
DATE_FROM = "2024-01-01"
DATE_TO = "2024-01-02"

OUT_DIR = Path("out")
OUT_PARQUET = OUT_DIR / "demo_data.parquet"
OUT_STATS = OUT_DIR / "demo_stats.parquet"
OUT_EVENTS = OUT_DIR / "demo_events.parquet"
OUT_MAP = OUT_DIR / "demo_map.html"


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print("[1/5] Build dataset and collect sample...")
    ds = (
        AISDataset(CSV_ROOT, pattern=CSV_PATTERN)
        .with_columns(["MMSI", "BaseDateTime", "LAT", "LON", "SOG", "COG", "Draft"])
        .between(DATE_FROM, DATE_TO)
    )
    df = ds.collect()
    print("  rows:", df.height, "cols:", df.columns)

    print("[2/5] Save filtered subset to Parquet:", OUT_PARQUET)
    ds.write_parquet(OUT_PARQUET, sort_rows=False)

    print("[3/5] Compute trajectory statistics:", OUT_STATS)
    stats = ds.stats()
    print(stats)
    stats.write_parquet(OUT_STATS)

    print("[4/5] Detect vessel-wise events:", OUT_EVENTS)
    events = ds.detect_events()
    print(events)
    events.write_parquet(OUT_EVENTS)

    print("[5/5] Optional map preview:", OUT_MAP)
    try:
        html_path = ds.plot_map(str(OUT_MAP), mmsi=123456789)
        print("  wrote:", html_path)
    except Exception as exc:  # pragma: no cover - optional dependency path
        print("  skipped map preview:", exc)


if __name__ == "__main__":
    main()
