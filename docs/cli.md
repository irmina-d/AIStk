# cli.py — Command Line Interface

## Purpose
Wraps common workflows (ingest → clean → stats/events → export) into CLI commands.

## Responsibilities
- Batch ingestion and cleaning with schema alignment.
- Running statistics and event detection over folders.
- Exporting maps/reports.

## Interactions with Other Modules
- io.py
- schema.py
- core.py
- stats.py
- events.py
- viz.py

## Usage Example
```bash
aisdataset --help
aisdataset ingest --src data/ais --out lake/raw
aisdataset clean  --in lake/raw --out lake/clean
aisdataset stats  --in lake/clean --by mmsi --out out/metrics
```

## Public API (Outline)
**Functions**
- `scan(root, pattern=..., date_from=..., date_to=..., mmsi=..., cols=..., to_parquet=..., html=...)`
**Top-level variables:** `app`

## Notes & Design Considerations
- Assumes canonical AIS columns after `schema.validate_columns()`.
- Keep I/O and analytics separated for testability.
- Prefer vectorized operations; avoid per-row Python loops where possible.