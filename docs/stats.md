# stats.py — Trajectory Statistics & Aggregates

## Purpose
Computes numeric summaries per vessel/trajectory or over time windows (e.g., speed, course variability).

## Responsibilities
- Aggregate per MMSI or trip (min/max/mean speed, heading change rate, distance).
- Produce feature sets for modeling and QA.
- Optionally resample time series.

## Interactions with Other Modules
- core.py (validated DataFrame)
- schema.py (column names)

## Usage Example
```python
from aisdataset import stats
metrics = stats.trajectory_metrics(df, by=["mmsi"])  # e.g., mean_sog, max_accel, turn_rate
```

## Public API (Outline)
**Functions**
- `compute_stats_df(df, level=...)`

## Notes & Design Considerations
- Assumes canonical AIS columns after `schema.validate_columns()`.
- Keep I/O and analytics separated for testability.
- Prefer vectorized operations; avoid per-row Python loops where possible.