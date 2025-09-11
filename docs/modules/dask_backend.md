# backends/dask_backend.py — Dask Backend

## Purpose
Enable **out-of-core** trajectory statistics on large AIS datasets using Dask.

## Responsibilities
- Read large CSV/Parquet files with Dask DataFrame.
- Partition workload across cores/workers.
- Compute per-MMSI metrics: points, distance, straight-line distance, tortuosity, turn index, average/max SOG.

## Usage Example
```python
import dask.dataframe as dd
from aisdataset.backends.dask_backend import compute_stats_dask

ddf = dd.read_parquet("lake/clean/2024/*.parquet")
out = compute_stats_dask(ddf, level="mmsi")
print(out.head())
```
