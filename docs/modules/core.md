# core.py — Core Dataset Management

## Purpose
Provides the central interface for working with AIS data. Wraps file discovery, reading, filtering, and column selection into a unified dataset object/API.

## Responsibilities
- Discover and load AIS data from folders/files (CSV/Parquet).
- Select and validate columns against the library schema.
- Filter by time windows, MMSI, or geographic bounds.
- Provide a consistent DataFrame for analytics modules.

## Interactions with Other Modules
- schema.py (column standards)
- io.py (reading/writing)
- stats.py
- events.py
- spatial.py
- viz.py

## Usage Example
```python
from aisdataset.core import AISDataset

ds = AISDataset("data/ais/2024")\
    .select_columns(["mmsi","timestamp","lat","lon","sog","cog"])\
    .filter(time_range=("2024-01-01","2024-01-31"), mmsi=244660000)

df = ds.to_dataframe()  # pass to stats/events/spatial
```

## Public API (Outline)
**Classes**
- `AISDataset` — High-level lazy dataset wrapper for decoded AIS CSV files.
  - `__init__(self, root, pattern=...)`
  - `with_columns(self, cols)`
  - `between(self, start, end)`
  - `filter(self, mmsi=..., imo=..., callsign=...)`
  - `_build(self)`
  - `collect(self)`
  - `write_parquet(self, path, partition=...)`
  - `stats(self, level=...)`
  - `detect_events(self, turn_deg=..., stop_sog=..., stop_min=..., draft_jump_m=...)`
  - `plot_map(self, out_html, mmsi=...)`
**Functions**
- `_scan_many(path, pattern)`
- `_valid_geo()`
- `_ts_expr()`

## Notes & Design Considerations
- Assumes canonical AIS columns after `schema.validate_columns()`.
- Keep I/O and analytics separated for testability.
- Prefer vectorized operations; avoid per-row Python loops where possible.