# cli.py — Command Line Interface

## Purpose
Provide a unified command-line interface to interact with the AIS Toolkit, covering ingestion, filtering, statistics, event detection, streaming simulation, and export. Designed for both quick exploration and batch workflows.

## Responsibilities
- Discover and load AIS data from CSV/Parquet with flexible glob patterns.  
- Apply filters (time ranges, MMSI, column selection).  
- Run statistics (distance, tortuosity, speed, turn index) via multiple backends (Polars, Polars streaming, Dask, Spark).  
- Detect navigational events (sharp turns, stops, draught changes, temporal gaps).  
- Export results to Parquet/CSV or interactive HTML maps.  
- Simulate online streaming from CSV files and emit events incrementally.  

## Interactions with Other Modules
- `core.py` → dataset abstraction (lazy building, filtering, materialization).  
- `stats.py` / `stats_streaming.py` → trajectory metrics.  
- `events.py` / `streaming/events_online.py` → batch and online event detection.  
- `viz.py` → map rendering with Folium.  
- `backends/dask_backend.py` and `backends/spark_backend.py` → large-scale execution engines.  

## Usage Examples
```bash
# General help
aistk --help

# Scan AIS files, restrict to January 2024, export Parquet
aistk scan data/ais --from 2024-01-01 --to 2024-02-01 \
  --mmsi 244660000,244770000 --to-parquet out/ais.parquet

# Compute per-MMSI stats with Polars streaming
aistk stats data/ais --engine polars-stream --out stats.parquet

# Detect navigational events
aistk events data/ais --mmsi 244660000 --out events.csv

# Simulate online streaming from a CSV (emit events as JSON lines)
aistk stream-csv data/ais/2024.csv --chunk-size 5000
```

## Public API (Outline)
**Typer Commands**  
- `scan(...)` → batch ingest + filtering + export (Parquet/HTML).  
- `stats(...)` → run statistics with selectable backend.  
- `events(...)` → detect events in batch datasets.  
- `stream_csv(...)` → simulate online stream from CSV, detect events incrementally.  

**Top-level variables**  
- `app` : `typer.Typer` instance.  

## Notes & Design Considerations
- Uses **Typer** for CLI ergonomics (structured help, autocompletion).  
- All heavy imports (Polars, Dask, Spark) are done lazily inside commands.  
- Backend abstraction allows the same API across Polars/Dask/Spark.  
- Event streaming (`stream-csv`) reuses the same logic as batch detection, maintaining per-MMSI state online.  
- Designed to keep analytics and I/O separated for testability.  
- Prefer vectorized operations in Polars/Spark/Dask; Python loops only in online streaming (stateful per record).  
