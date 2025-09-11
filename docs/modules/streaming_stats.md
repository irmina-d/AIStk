## `stats_streaming.py`
### Purpose
Implements **streaming-friendly statistics** on Polars LazyFrames, avoiding materialization into NumPy arrays.  
Designed for large AIS datasets processed with `collect(streaming=True)`.

### Responsibilities
- Compute distances, straight-line displacement, tortuosity, turn index, and SOG metrics.
- Implement all metrics as Polars expressions (no Python loops).
- Enable incremental execution with Polars streaming engine.

### Usage Example
```python
import polars as pl
from aistk.stats_streaming import compute_stats_lazy
from aistk.core import AISDataset

lf = AISDataset("data/ais")._build()
out = compute_stats_lazy(lf, level="mmsi").collect(streaming=True)
print(out)
```
