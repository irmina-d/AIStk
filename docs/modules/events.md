# events.py — Event Detection

## Purpose

Detect selected event indicators from AIS point sequences to support exploratory maritime data preparation and quality assessment.

Implemented indicators:

- `sharp_turn` — wrapped change in course over ground above a configurable threshold;
- `stop` — speed over ground below a configurable threshold for a minimum duration;
- `gap` — temporal gap between consecutive AIS messages;
- `draft_change` — change in AIS draught above a configurable threshold.

## Vessel-wise processing

When the `MMSI` column is present, event detection is performed independently for each vessel. This avoids false events caused by comparing the last record of one vessel with the first record of another vessel in large multi-vessel AIS dumps.

## Draught caution

AIS draught values are often manually entered, missing, outdated, or updated irregularly. For this reason, `draft_change` should be interpreted as a low-confidence data-quality or cargo-state indicator rather than a direct behavioural anomaly. Missing draught values are not imputed during batch event detection. If draught reliability is uncertain, disable this indicator:

```python
from aistk.events import detect_events_df

events = detect_events_df(df, include_draft_changes=False)
```

or from the CLI:

```bash
aistk events data/sample --pattern ais_sample.csv --skip-draft-events
```

## Public API

```python
detect_events_df(
    df,
    turn_deg=30.0,
    stop_sog=0.5,
    stop_min=15,
    draft_jump_m=0.3,
    gap_s=600,
    group_col="MMSI",
    include_draft_changes=True,
)
```
