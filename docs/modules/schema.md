# schema.py — Schema & Column Contracts

## Purpose
Defines the canonical column set for AIS frames and provides helpers to validate/rename/cast columns.

## Responsibilities
- Map raw vendor-specific names to canonical AIS column names.
- Validate presence and dtypes of essential fields.
- Provide access to required/optional column sets.

## Interactions with Other Modules
- core.py (column selection)
- io.py (read types)
- stats.py/events.py/spatial.py (assume canonical names)

## Usage Example
```python
from aisdataset import schema, io

df = io.read_parquet("data/ais/*.parquet")
df = schema.validate_columns(df)  # ensures columns like mmsi, imo, timestamp, lat, lon, sog
```

## Public API (Outline)
**Functions**
- `normalize_columns(df)` — Rename aliases and ensure required columns exist.
**Top-level variables:** `DEFAULT_COLUMNS`, `ALIASES`

## Notes & Design Considerations
- Assumes canonical AIS columns after `schema.validate_columns()`.
- Keep I/O and analytics separated for testability.
- Prefer vectorized operations; avoid per-row Python loops where possible.