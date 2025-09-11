# Examples

These scripts and notebooks demonstrate how to use **aistk** with AIS CSV files.  
In tests we ship a tiny sample dataset in `tests/data/mini_ais.csv`.  
In real projects, point `CSV_ROOT` to your directory with daily AIS CSVs and adjust `pattern`.

Run from the project root (after installation, e.g. `pip install -e .[viz,cli]`):

```bash
python examples/01_getting_started.py
python examples/02_stats_streaming_polars.py
python examples/03_events_batch.py
python examples/04_events_online_from_csv.py
python examples/05_dask_large_stats.py
python examples/06_spark_large_stats.py
python examples/07_map_quicklook.py
python examples/08_spatial_h3_geofence.py

# End-to-end demo (all steps in one flow)
python examples/00_end_to_end_demo.py
```

Outputs are written into the `out/` folder (Parquet/CSV results and `demo_map.html` map).  
For interactive exploration, Jupyter notebooks with the same numbering (`.ipynb`) are also provided.
