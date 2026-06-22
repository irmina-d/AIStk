# Large AIS benchmark

These scripts are intended for the SoftwareX revision and reviewer-response
experiments. They are **not** pytest fixtures and should be run manually on real
AIS dumps.

## Recommended local layout

```text
AIStk-main/
  data/
    raw/
      ais-2024-01-01.csv
  results/
```

## Main benchmark command

```bash
python benchmarks/benchmark_large_csv.py \
  --root data/raw \
  --pattern ais-2024-01-01.csv \
  --from 2024-01-01 \
  --to 2024-01-02 \
  --out-dir results/benchmark_ais_2024_01_01 \
  --repeats 3 \
  --include-pandas
```

Use `--repeats 5` for a stronger final result. The event benchmark is enabled by
default because reviewer feedback specifically highlighted AIS event detection
as a useful capability. Use `--skip-events` only for quick local smoke tests.

## Output files

The script writes one subdirectory per run:

```text
results/benchmark_ais_2024_01_01/run_01/aistk_scan.parquet
results/benchmark_ais_2024_01_01/run_01/aistk_stats.parquet
results/benchmark_ais_2024_01_01/run_01/aistk_events.parquet
results/benchmark_ais_2024_01_01/run_01/raw_polars_scan.parquet
results/benchmark_ais_2024_01_01/run_01/pandas_scan.parquet   # only with --include-pandas
```

and the summary artefacts:

```text
results/benchmark_ais_2024_01_01/input_diagnostics.json
results/benchmark_ais_2024_01_01/benchmark_results.csv
results/benchmark_ais_2024_01_01/benchmark_results.json
results/benchmark_ais_2024_01_01/benchmark_table_for_manuscript.csv
results/benchmark_ais_2024_01_01/benchmark_table_for_manuscript.json
```

## What to report in the manuscript

Use `benchmark_table_for_manuscript.csv` to report:

- input file size,
- number of AIS records,
- number of unique MMSI values,
- number of repeated runs,
- median, mean and standard deviation of runtime,
- median, mean and standard deviation of peak RSS memory,
- median output size and output row count,
- whether Pandas succeeded or failed under the same local constraints.

This benchmark positions AIStk as an AIS-specific layer on top of Polars, not as
a replacement for Polars. The raw Polars baseline is included to quantify the
wrapper overhead, while Pandas is included as a conventional research-script
baseline. The vessel-wise event benchmark demonstrates that AIStk also evaluates
AIS-specific analytical functionality, not only generic CSV export.
