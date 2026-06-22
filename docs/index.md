# AIStk — AIS Data Toolkit

A modular Python library for working with **AIS (Automatic Identification System)** data: schema normalization, dataset handling, trajectory statistics, event detection, spatial processing, visualization, multiple backends, and a productive CLI.

---

## Installation

```bash
pip install aistk
```

For local development from a cloned repository:

```bash
python -m pip install -e ".[viz,benchmark,dev]"
```

---

## Directly runnable quick start

The repository ships a tiny sample AIS file at `data/sample/ais_sample.csv`, so the first example can be run without downloading external data.

```python
from aistk import AISDataset

sample = (
    AISDataset("data/sample", pattern="ais_sample.csv")
    .with_columns(["MMSI", "BaseDateTime", "LAT", "LON", "SOG", "COG", "Draft"])
    .between("2024-01-01", "2024-01-02")
)

print(sample.collect().head())
print(sample.stats())
print(sample.detect_events())
```

Equivalent CLI smoke test:

```bash
aistk scan data/sample --pattern ais_sample.csv \
  --from 2024-01-01 --to 2024-01-02 \
  --cols MMSI,BaseDateTime,LAT,LON,SOG,COG,Draft \
  --to-parquet out/sample.parquet --no-sort-output

aistk stats data/sample --pattern ais_sample.csv \
  --from 2024-01-01 --to 2024-01-02 \
  --engine polars-stream --out out/sample_stats.parquet

aistk events data/sample --pattern ais_sample.csv \
  --from 2024-01-01 --to 2024-01-02 \
  --out out/sample_events.parquet
```

---

## AIS data quality

AIS data may contain missing, invalid, delayed, or manually entered values. AIStk currently applies basic geographic coordinate validity checks and removes records with latitude outside `[-90, 90]` or longitude outside `[-180, 180]`. Advanced trajectory-based outlier detection and reconstruction are outside the current MVP scope.

Draught-change events require particular caution: AIS draught values can be missing, outdated, or inconsistently updated. AIStk therefore treats `draft_change` as a low-confidence data-quality or cargo-state indicator, not as direct evidence of behavioural anomaly. Draft-change reporting can be disabled with `--skip-draft-events` or `include_draft_changes=False`.

---

## Modules

- [core.py](modules/core.md) — Core dataset management  
- [schema.py](modules/schema.md) — Schema and column contracts  
- [io.py](modules/io.md) — I/O utilities  
- [stats.py](modules/stats.md) — Trajectory statistics  
- [stats_streaming.py](modules/streaming_stats.md) — Streaming-friendly statistics  
- [events.py](modules/events.md) — Batch event detection  
- [streaming/events_online.py](modules/events_online.md) — Online streaming event detection  
- [spatial.py](modules/spatial.md) — Spatial processing  
- [viz.py](modules/viz.md) — Folium maps  
- [cli.py](modules/cli.md) — Command-line interface  
- [utils.py](modules/utils.md) — Utilities  
- [backends/dask_backend.py](modules/dask_backend.md) — Dask backend  
- [backends/spark_backend.py](modules/spark_backend.md) — Spark backend
