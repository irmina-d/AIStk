[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
![Python Version](https://img.shields.io/badge/python-3.9%2B-blue)
[![Docs](https://img.shields.io/badge/docs-online-blue.svg)](https://irmina-d.github.io/AIStk/)
![Visitors](https://komarev.com/ghpvc/?username=irmina-d&color=green&style=plastic)

# AIStk — AIS Toolkit for Spatio‑Temporal Datasets

Minimal, fast toolkit for building high‑resolution AIS datasets from decoded CSV files.

## Features
- Lazy loading of 365+ daily CSVs (Polars)
- Column selection, date & MMSI filtering
- Track metrics (distance, straight‑line, tortuosity, turn index, speed stats)
- Event detection (sharp turns, stops, AIS gaps, draft changes)
- Map plotting (Folium) and Parquet export

## Install
```bash
pip install aistk
```

## Quickstart
```python
from aistk import AISDataset

ds = (AISDataset("data/ais/2024", pattern="AIS_2024_*.csv")
      .with_columns(["MMSI","BaseDateTime","LAT","LON","SOG","COG","Draft"])
      .between("2024-01-01","2024-12-31")
      .filter(mmsi=338075892))

df = ds.collect()
stats = ds.stats()                # Polars DataFrame with metrics per MMSI
events = ds.detect_events()       # Detected events
ds.plot_map("track.html")         # Save interactive map to HTML
```

## CLI
```bash
aistk scan --root ./AIS/2024 --pattern "AIS_2024_*.csv"                --from 2024-01-01 --to 2024-12-31                --mmsi 338075892                --cols MMSI,BaseDateTime,LAT,LON,SOG,COG,Draft                --to-parquet out/ais.parquet --html out/track.html
```

## Project layout
```
aistk/
  aistk/ (library)
  tests/
  examples/
```

## License
MIT © 2025 by Irmina Durlik
