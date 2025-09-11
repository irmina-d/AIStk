# utils.py — Utilities & Helpers

## Purpose
Small, reusable functions for validation, parsing, windowing, or math helpers used across modules.

## Responsibilities
- Timestamp parsing and timezone normalization.
- Numeric helpers (bearing diff, unwrap heading, haversine).
- Windowing/rolling operations.

## Interactions with Other Modules
- stats.py
- events.py
- spatial.py
- core.py

## Usage Example
```python
from aisdataset.utils import haversine_km, unwrap_heading

d = haversine_km(lat1, lon1, lat2, lon2)
hdg = unwrap_heading(series_heading_deg)
```

## Public API (Outline)
**Functions**
- `haversine_km(lat1, lon1, lat2, lon2)` — Vectorized haversine distance (km). Inputs are in degrees (numpy arrays or scalars).
**Top-level variables:** `EARTH_RADIUS_KM`

## Notes & Design Considerations
- Assumes canonical AIS columns after `schema.validate_columns()`.
- Keep I/O and analytics separated for testability.
- Prefer vectorized operations; avoid per-row Python loops where possible.