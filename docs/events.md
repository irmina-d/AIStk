# events.py — Event Detection (Turns, Stops, Gaps, Draft Changes)

## Purpose
Detects domain-relevant events from sequences of AIS points to support safety/risk analysis.

## Responsibilities
- Identify sharp turns, prolonged stops/anchorage, signal gaps, and draught changes.
- Emit event-level tables with timestamps, positions, and derived measures.
- Allow thresholds configuration (e.g., min speed for 'stop').

## Interactions with Other Modules
- stats.py (rate/variance signals)
- schema.py (column names)
- spatial.py (geofencing during events)

## Usage Example
```python
from aisdataset.events import detect_events

events = detect_events(df, min_stop_minutes=15, turn_threshold_deg=30)
```

## Public API (Outline)
**Functions**
- `detect_events_df(df, turn_deg=..., stop_sog=..., stop_min=..., draft_jump_m=...)`

## Notes & Design Considerations
- Assumes canonical AIS columns after `schema.validate_columns()`.
- Keep I/O and analytics separated for testability.
- Prefer vectorized operations; avoid per-row Python loops where possible.