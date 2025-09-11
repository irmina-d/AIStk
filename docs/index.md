# AISDATASET — AIS Data Toolkit

[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](../LICENSE)
![Python Version](https://img.shields.io/badge/python-3.9%2B-blue)
[![Docs](https://img.shields.io/badge/docs-ready-blue.svg)](./)

A modular Python library for working with **AIS (Automatic Identification System)** data: schema normalization, dataset handling,
trajectory statistics, event detection, spatial processing (H3, geofencing), visualization, and a productive CLI.

## Installation

```bash
pip install aisdataset
```

## Quick Start

```python
from aisdataset import core, schema, stats

ds = core.AISDataset("data/ais/2024")
df = schema.validate_columns(ds.to_dataframe())
metrics = stats.trajectory_metrics(df, by=["mmsi"])
```

## Modules

- [core.py](modules/core.md) — Core Dataset Management  
- [schema.py](modules/schema.md) — Schema & Column Contracts  
- [io.py](modules/io.md) — I/O Utilities  
- [stats.py](modules/stats.md) — Trajectory Statistics & Aggregates  
- [events.py](modules/events.md) — Event Detection  
- [spatial.py](modules/spatial.md) — Spatial Processing  
- [viz.py](modules/viz.md) — Visualization  
- [cli.py](modules/cli.md) — Command Line Interface  
- [utils.py](modules/utils.md) — Utilities & Helpers  
- [__init__.py](modules/__init__.md) — Package Export Surface  

## Example Workflow

```bash
# 1) Ingest and clean
aisdataset ingest --src data/ais/raw --out lake/raw
aisdataset clean  --in lake/raw --out lake/clean

# 2) Statistics and events
aisdataset stats  --in lake/clean --by mmsi --out out/metrics
aisdataset events detect --in lake/clean --out out/events

# 3) Visualization
aisdataset viz map --in lake/clean --sample 10000 --out out/map.html
```

## Contributing

- Keep docstrings up to date (NumPy/Google style).  
- Add tests (`pytest -q`) for all public functions.  
- Run linters (ruff/black/mypy) before committing.
