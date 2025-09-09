# Examples

These scripts demonstrate how to use **aistk** with the tiny sample CSV in `tests/data/mini_ais.csv`.
In your project, point `data_root` to the directory with your AIS daily CSV files and adjust `pattern`.

Run from the project root (after `pip install -e .[viz,cli]`):

```bash
python examples/01_quickstart.py
python examples/02_compute_stats.py
python examples/03_detect_events_and_map.py
```

Outputs are written into the `examples/` folder (CSV/Parquet and `track_demo.html` map).
