# AIStk — AIS Data Toolkit

[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](../LICENSE)  
![Python Version](https://img.shields.io/badge/python-3.9%2B-blue)  
[![Docs](https://img.shields.io/badge/docs-ready-blue.svg)](./)  

A modular Python library for working with **AIS (Automatic Identification System)** data: schema normalization, dataset handling, trajectory statistics, event detection, spatial processing (H3, geofencing), visualization, multiple backends (Polars, Dask, Spark), and a productive CLI.

---

## Installation

```bash
pip install aistk
```

---

## Quick Start

```python
from aistk.core import AISDataset
from aistk.stats import compute_stats_df

ds = AISDataset("data/ais/2024")
df = ds.collect()
metrics = compute_stats_df(df, level="mmsi")
print(metrics.head())
```

---

## Modules

- [core.py](modules/core.md) — Core Dataset Management  
- [schema.py](modules/schema.md) — Schema & Column Contracts  
- [io.py](modules/io.md) — I/O Utilities  
- [stats.py](modules/stats.md) — Trajectory Statistics (eager, Polars DataFrame)  
- [stats_streaming.py](modules/stats_streaming.md) — Streaming Stats (Polars LazyFrame)  
- [events.py](modules/events.md) — Event Detection (batch)  
- [streaming/events_online.py](modules/events_online.md) — Online Streaming Event Detection  
- [spatial.py](modules/spatial.md) — Spatial Processing (H3, geofencing)  
- [viz.py](modules/viz.md) — Visualization (Folium maps)  
- [cli.py](modules/cli.md) — Command Line Interface  
- [utils.py](modules/utils.md) — Utilities & Helpers  
- [backends/dask_backend.py](modules/dask_backend.md) — Dask Backend (out-of-core)  
- [backends/spark_backend.py](modules/spark_backend.md) — Spark Backend (cluster/distributed)  
- [__init__.py](modules/__init__.md) — Package Export Surface  

---

## Example CLI Workflow

```bash
# 1) Scan and export
aistk scan data/ais --from 2024-01-01 --to 2024-02-01 \
  --mmsi 244660000,244770000 --to-parquet out/data.parquet

# 2) Statistics (streaming Polars)
aistk stats data/ais --engine polars-stream --out out/stats.parquet

# 3) Events (batch detection)
aistk events data/ais --mmsi 244660000 --out out/events.csv

# 4) Streaming simulation from CSV
aistk stream-csv data/ais/2024.csv --chunk-size 5000
```

---

## Contributing

- Keep docstrings up to date (NumPy/Google style).  
- Add tests (`pytest -q`) for all public functions.  
- Run linters (`ruff`, `black`, `mypy`) before committing.  
- Ensure new features support at least the Polars backend; add Dask/Spark support where appropriate.  
