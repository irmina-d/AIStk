[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
![Python Version](https://img.shields.io/badge/python-3.9%2B-blue)
[![Docs](https://img.shields.io/badge/docs-online-blue.svg)](https://irmina-d.github.io/AIStk/)
![Visitors](https://komarev.com/ghpvc/?username=irmina-d&color=green&style=plastic)
[![PyPI Downloads](https://static.pepy.tech/personalized-badge/aistk?period=total&units=INTERNATIONAL_SYSTEM&left_color=BLACK&right_color=GREEN&left_text=downloads)](https://pepy.tech/projects/aistk)
[![DOI](https://zenodo.org/badge/1053545932.svg)](https://doi.org/10.5281/zenodo.19560234)
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

The first example is directly runnable from a fresh clone because the repository
ships a tiny AIS sample file at `data/sample/ais_sample.csv`. It demonstrates
the same workflow used on larger AIS files: load, normalize timestamps, filter,
compute trajectory statistics, and detect events.

```python
from aistk import AISDataset

ds = (
    AISDataset("data/sample", pattern="ais_sample.csv")
    .with_columns(["MMSI", "BaseDateTime", "LAT", "LON", "SOG", "COG", "Draft"])
    .between("2024-01-01", "2024-01-02")
)

df = ds.collect()
stats = ds.stats()
events = ds.detect_events()

print(df.head())
print(stats)
print(events)
```

Run the corresponding script:

```bash
python examples/01_quickstart.py
```

## CLI quickstart

The same bundled sample data can be used to check the command-line interface:

```bash
aistk scan data/sample \
  --pattern "ais_sample.csv" \
  --from 2024-01-01 \
  --to 2024-01-02 \
  --cols MMSI,BaseDateTime,LAT,LON,SOG,COG,Draft \
  --to-parquet out/sample.parquet \
  --no-sort-output

aistk stats data/sample \
  --pattern "ais_sample.csv" \
  --from 2024-01-01 \
  --to 2024-01-02 \
  --engine polars-stream \
  --out out/sample_stats.parquet

aistk events data/sample \
  --pattern "ais_sample.csv" \
  --from 2024-01-01 \
  --to 2024-01-02 \
  --out out/sample_events.parquet
```

These commands are also covered by the test suite to ensure that the documented
quick-start workflow remains executable.

## AIS data-quality notes

AIS data are noisy and should not be interpreted as perfectly observed vessel
trajectories. AIStk currently applies basic coordinate validation by retaining
records with latitude values between -90 and 90 degrees and longitude values
between -180 and 180 degrees. Advanced trajectory-based outlier detection,
map-matching and reconstruction are outside the current MVP scope and are planned
for future releases.

Draught variation should also be interpreted cautiously. AIS draught values may
be missing, outdated, manually entered, or updated irregularly. Therefore,
`draft_change` events in AIStk should be treated as low-confidence data-quality
or cargo-state indicators rather than direct behavioural anomalies. The detector
does not impute missing draught values, and draft-change reporting can be disabled
from the CLI with `--skip-draft-events` or from Python by passing
`include_draft_changes=False` to `detect_events_df`.

## Project layout
```
aistk/
  aistk/ (library)
  tests/
  examples/
```

## License
MIT © 2025 by Irmina Durlik

## Large-file benchmarking workflow

AIStk is designed as an AIS-specific analytical layer on top of Polars. For
large raw CSV files, use the lazy/streaming path and write intermediate results
to Parquet before running heavier downstream analyses.

Recommended local layout:

```text
AIStk-main/
  data/
    raw/
      ais-2024-01-01.csv
  results/
```

CSV to Parquet using the Polars lazy path:

```bash
aistk scan data/raw \
  --pattern "ais-2024-01-01.csv" \
  --from 2024-01-01 \
  --to 2024-01-02 \
  --cols MMSI,BaseDateTime,LAT,LON,SOG,COG,Heading,IMO,CallSign,VesselName,VesselType,Status,Length,Width,Draft,Cargo,TransceiverClass \
  --to-parquet results/ais_2024_01_01.parquet \
  --stream \
  --no-sort-output
```

Trajectory statistics per vessel:

```bash
aistk stats data/raw \
  --pattern "ais-2024-01-01.csv" \
  --from 2024-01-01 \
  --to 2024-01-02 \
  --engine polars-stream \
  --out results/stats_ais_2024_01_01.parquet
```

Reviewer-oriented benchmark table:

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

The benchmark compares:

- AIStk/Polars lazy CSV → Parquet workflow,
- AIStk/Polars streaming trajectory statistics,
- AIStk vessel-wise event detection per MMSI,
- raw Polars lazy baseline,
- optional Pandas baseline.

The script writes per-run outputs to `run_XX/` subdirectories and creates both
raw and manuscript-ready result tables:

```text
benchmark_results.csv
benchmark_results.json
benchmark_table_for_manuscript.csv
benchmark_table_for_manuscript.json
input_diagnostics.json
```

Use `--repeats 3` or `--repeats 5` for reviewer-facing results. The manuscript
table reports median, mean and standard deviation of runtime and peak RSS. Use
`--skip-events` only when you need a quick scan/statistics benchmark without the
heavier vessel-wise event detector.

## Vessel-aware event detection

Event detection is performed independently per `MMSI` when the vessel identifier
is available. This prevents false sharp turns, draught changes, stops or AIS gaps
from being generated by comparing the last record of one vessel with the first
record of another vessel in large multi-vessel AIS dumps.

### Troubleshooting large CSV benchmarks

If `benchmark_results.json` reports errors such as `ColumnNotFoundError('unable to find column "BaseDateTime"; valid columns: []')`, the benchmark is not measuring performance yet. It means that the input CSV dialect or header did not match the expected AIS schema. The benchmark now writes `input_diagnostics.json` to the output directory before running the tasks. Check this file first; it shows the discovered separator, raw columns, and normalized column names.

AIStk now handles common AIS CSV variants automatically:

- comma, semicolon, tab and pipe separators,
- lowercase headers such as `mmsi`, `basedatetime`, `lat`, `lon`,
- selected aliases such as `timestamp`/`datetime` for `BaseDateTime`, `longitude` for `LON`, `draught` for `Draft`, and `transceiver` for `TransceiverClass`.

If your file uses non-standard column names, pass an explicit `--cols` list matching the canonical names after normalization, or rename the header once before benchmarking.
