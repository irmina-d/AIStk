# Examples

These scripts and notebooks demonstrate how to use **AIStk** with AIS CSV files.
The repository includes a directly runnable sample dataset:

```text
data/sample/ais_sample.csv
```

Run from the project root after installation, for example:

```bash
python -m pip install -e ".[viz]"
```

Core examples:

```bash
python examples/01_quickstart.py
python examples/01_getting_started.py
python examples/02_stats_streaming_polars.py
python examples/03_events_batch.py
python examples/04_events_online_from_csv.py

# End-to-end demo
python examples/00_end_to_end_demo.py
```

Some examples require optional extras:

```bash
python -m pip install -e ".[dask]"      # for examples/05_dask_large_stats.py
python -m pip install -e ".[spark]"     # for examples/06_spark_large_stats.py
python -m pip install -e ".[spatial]"   # for examples/08_spatial_h3_geofence.py
python -m pip install -e ".[viz]"       # for map quicklooks
```

Outputs are written into the `out/` folder. In real projects, point the input
root to your directory with daily AIS CSV files and adjust `pattern`.
