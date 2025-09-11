# streaming/events_online.py — Online Event Detection

## Purpose
Detect navigational events incrementally in **online streams** of AIS records.  
Maintains per-MMSI state and yields events as soon as they can be detected.

## Supported Events
- sharp_turn — ΔCOG above threshold.
- stop — SOG below threshold for N minutes.
- gap — missing reports for > threshold seconds.
- draft_change — draught jump above threshold.

## Usage Example
```python
from aisdataset.streaming.events_online import process_stream

records = [
    {"MMSI": 1, "ts": 1710000000000, "COG": 10.0, "SOG": 10.0, "Draft": 8.0},
    {"MMSI": 1, "ts": 1710000005000, "COG": 50.0, "SOG": 0.2,  "Draft": 8.0},
    {"MMSI": 1, "ts": 1710000900000, "COG": 55.0, "SOG": 0.1,  "Draft": 8.5},
]
for ev in process_stream(records, stop_min=1):
    print(ev)
```
