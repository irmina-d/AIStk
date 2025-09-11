# io.py — I/O Utilities

## Purpose
File-system level helpers for reading/writing AIS dataframes in formats such as Parquet/CSV/GeoJSON.

## Responsibilities
- Read datasets (single file or glob patterns).
- Write partitioned Parquet for scalable storage.
- Optionally support GeoJSON export for mapping.

## Interactions with Other Modules
- core.py (dataset construction)
- viz.py (map export)
- spatial.py (geometry I/O)

## Usage Example
```python
from aisdataset.io import read_parquet, write_parquet

df = read_parquet("lake/clean/2024/*.parquet")
write_parquet(df, "out/ais_2024/")
```

## Public API (Outline)
**Functions**
- `write_parquet(df, path)`

## Notes & Design Considerations
- Assumes canonical AIS columns after `schema.validate_columns()`.
- Keep I/O and analytics separated for testability.
- Prefer vectorized operations; avoid per-row Python loops where possible.