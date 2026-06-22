"""Large-file benchmark for AIStk and reviewer-response experiments.

This script is intentionally outside pytest. It is designed for real AIS dumps,
for example ``ais-2024-01-01.csv`` (~760 MB), and produces both raw per-run
benchmark results and an aggregated table ready to paste into the manuscript.

Example
-------
python benchmarks/benchmark_large_csv.py \
  --root data/raw \
  --pattern ais-2024-01-01.csv \
  --from 2024-01-01 \
  --to 2024-01-02 \
  --out-dir results/benchmark_2024_01_01 \
  --repeats 3 \
  --include-pandas
"""

from __future__ import annotations

import argparse
import csv
import gc
import json
import math
import os
import statistics
import sys
import threading
import time
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable, Iterator, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import polars as pl

from aistk.core import (
    AISDataset,
    _available_columns_message,
    _canonical_rename_map,
    _infer_csv_separator,
    _scan_files,
    _select_existing_columns,
)
from aistk.events import detect_events_df
from aistk.stats_streaming import compute_stats_lazy

try:  # optional but useful for peak RSS sampling
    import psutil  # type: ignore
except Exception:  # pragma: no cover
    psutil = None


@dataclass
class BenchmarkResult:
    run: int
    task: str
    engine: str
    input_size_mb: float
    rows: Optional[int]
    unique_mmsi: Optional[int]
    runtime_s: float
    peak_rss_mb: Optional[float]
    output: str
    output_size_mb: Optional[float]
    output_rows: Optional[int]
    status: str
    error: str = ""


def _size_mb(path: Path) -> float:
    if path.is_file():
        return path.stat().st_size / 1024**2
    if path.is_dir():
        return sum(p.stat().st_size for p in path.rglob("*") if p.is_file()) / 1024**2
    return 0.0


def _parquet_row_count(path: Path) -> Optional[int]:
    """Return row count for a Parquet output without loading all columns."""
    if not path.exists() or not path.is_file() or path.suffix.lower() != ".parquet":
        return None
    try:
        out = pl.scan_parquet(path).select(pl.len().alias("n")).collect()
        return int(out[0, "n"])
    except Exception:
        return None


def _parse_datetime_literal(value: Optional[str]):
    if value is None:
        return None
    parsed = pl.Series([value]).str.to_datetime(strict=False).to_list()[0]
    if parsed is None:
        raise ValueError(f"Could not parse datetime literal: {value!r}")
    return parsed


@contextmanager
def measure_peak_rss() -> Iterator[Callable[[], Optional[float]]]:
    """Measure peak RSS in MB during a code block using a background sampler."""
    if psutil is None:
        start = time.perf_counter()
        yield lambda: None
        _ = start
        return

    proc = psutil.Process(os.getpid())
    stop = threading.Event()
    peak = {"rss": proc.memory_info().rss}

    def sampler() -> None:
        while not stop.is_set():
            try:
                peak["rss"] = max(peak["rss"], proc.memory_info().rss)
            except Exception:
                pass
            time.sleep(0.05)

    thread = threading.Thread(target=sampler, daemon=True)
    thread.start()
    try:
        yield lambda: peak["rss"] / 1024**2
    finally:
        stop.set()
        thread.join(timeout=1)


def _run_task(
    *,
    run: int,
    task: str,
    engine: str,
    input_size_mb: float,
    output_path: Path,
    rows: Optional[int],
    unique_mmsi: Optional[int],
    fn: Callable[[], None],
) -> BenchmarkResult:
    gc.collect()
    start = time.perf_counter()
    status = "ok"
    error = ""
    peak_value: Optional[float]
    with measure_peak_rss() as peak:
        try:
            fn()
        except Exception as exc:  # keep benchmark table informative instead of crashing
            status = "failed"
            error = repr(exc)
        finally:
            runtime_s = time.perf_counter() - start
            peak_value = peak()

    return BenchmarkResult(
        run=run,
        task=task,
        engine=engine,
        input_size_mb=round(input_size_mb, 2),
        rows=rows,
        unique_mmsi=unique_mmsi,
        runtime_s=round(runtime_s, 3),
        peak_rss_mb=round(peak_value, 2) if peak_value is not None else None,
        output=str(output_path),
        output_size_mb=round(_size_mb(output_path), 2) if output_path.exists() else None,
        output_rows=_parquet_row_count(output_path) if status == "ok" else None,
        status=status,
        error=error,
    )


def _base_dataset(args: argparse.Namespace) -> AISDataset:
    ds = AISDataset(args.root, pattern=args.pattern)
    if args.cols:
        ds = ds.with_columns([c.strip() for c in args.cols.split(",") if c.strip()])
    if args.date_from and args.date_to:
        ds = ds.between(args.date_from, args.date_to)
    return ds


def _events_dataset(args: argparse.Namespace) -> AISDataset:
    event_cols = ["MMSI", "BaseDateTime", "SOG", "COG", "Draft"]
    ds = AISDataset(args.root, pattern=args.pattern).with_columns(event_cols)
    if args.date_from and args.date_to:
        ds = ds.between(args.date_from, args.date_to)
    return ds


def _count_metadata(ds: AISDataset) -> tuple[Optional[int], Optional[int]]:
    try:
        lf = ds.lazy(sort_rows=False)
        schema = set(lf.collect_schema().names())
        exprs = [pl.len().alias("rows")]
        if "MMSI" in schema:
            exprs.append(pl.col("MMSI").n_unique().alias("unique_mmsi"))
        out = lf.select(exprs).collect(engine="streaming")
        rows = int(out[0, "rows"])
        unique_mmsi = int(out[0, "unique_mmsi"]) if "unique_mmsi" in out.columns else None
        return rows, unique_mmsi
    except Exception:
        return None, None


def _write_input_diagnostics(input_files: list[Path], out_dir: Path) -> None:
    """Write separator and schema diagnostics for reproducibility/debugging."""
    diagnostics = []
    for path in input_files[:5]:
        try:
            sep = _infer_csv_separator(path)
            schema = pl.read_csv(path, separator=sep, n_rows=0).columns
            normalized = [_canonical_rename_map(schema).get(c, c) for c in schema]
            diagnostics.append(
                {
                    "file": str(path),
                    "size_mb": round(path.stat().st_size / 1024**2, 2),
                    "separator": "TAB" if sep == "\t" else sep,
                    "columns": schema,
                    "normalized_columns": normalized,
                }
            )
        except Exception as exc:
            diagnostics.append({"file": str(path), "error": repr(exc)})
    (out_dir / "input_diagnostics.json").write_text(json.dumps(diagnostics, indent=2), encoding="utf-8")


def _safe_unlink(path: Path) -> None:
    try:
        if path.exists() and path.is_file():
            path.unlink()
    except Exception:
        pass


def _run_repeated(
    args: argparse.Namespace,
    *,
    input_files: list[Path],
    input_size_mb: float,
    rows: Optional[int],
    unique_mmsi: Optional[int],
    ds: AISDataset,
    out_dir: Path,
) -> list[BenchmarkResult]:
    results: list[BenchmarkResult] = []

    for run in range(1, args.repeats + 1):
        run_dir = out_dir / f"run_{run:02d}"
        run_dir.mkdir(parents=True, exist_ok=True)

        # 1) AIStk domain pipeline: CSV -> normalized/filtered Parquet via Polars lazy path.
        aistk_parquet = run_dir / "aistk_scan.parquet"
        _safe_unlink(aistk_parquet)
        results.append(
            _run_task(
                run=run,
                task="csv_scan_filter_to_parquet",
                engine="AIStk/Polars lazy",
                input_size_mb=input_size_mb,
                output_path=aistk_parquet,
                rows=rows,
                unique_mmsi=unique_mmsi,
                fn=lambda p=aistk_parquet: ds.write_parquet(p, streaming=True, sort_rows=False),
            )
        )

        # 2) AIStk vessel-wise trajectory statistics.
        stats_path = run_dir / "aistk_stats.parquet"
        _safe_unlink(stats_path)

        def run_aistk_stats(path: Path = stats_path) -> None:
            stats = compute_stats_lazy(ds.lazy(sort_rows=True), level="mmsi").collect(engine="streaming")
            stats.write_parquet(path)

        results.append(
            _run_task(
                run=run,
                task="trajectory_stats_per_mmsi",
                engine="AIStk/Polars streaming",
                input_size_mb=input_size_mb,
                output_path=stats_path,
                rows=rows,
                unique_mmsi=unique_mmsi,
                fn=run_aistk_stats,
            )
        )

        # 3) AIStk vessel-wise event detection. This explicitly benchmarks one
        # reviewer-valued feature beyond simple scan/export and trajectory metrics.
        if not args.skip_events:
            events_path = run_dir / "aistk_events.parquet"
            _safe_unlink(events_path)
            event_ds = _events_dataset(args)

            def run_aistk_events(path: Path = events_path) -> None:
                frame = event_ds.lazy(sort_rows=True).collect(engine="streaming")
                events = detect_events_df(
                    frame,
                    turn_deg=args.turn_deg,
                    stop_sog=args.stop_sog,
                    stop_min=args.stop_min,
                    draft_jump_m=args.draft_jump_m,
                    gap_s=args.gap_s,
                )
                events.write_parquet(path)

            results.append(
                _run_task(
                    run=run,
                    task="event_detection_per_mmsi",
                    engine="AIStk/Polars + vessel-wise events",
                    input_size_mb=input_size_mb,
                    output_path=events_path,
                    rows=rows,
                    unique_mmsi=unique_mmsi,
                    fn=run_aistk_events,
                )
            )

        # 4) Raw Polars baseline: similar scan/filter/export without the AIStk wrapper.
        raw_polars_path = run_dir / "raw_polars_scan.parquet"
        _safe_unlink(raw_polars_path)

        def run_raw_polars(path: Path = raw_polars_path) -> None:
            # Raw Polars implementation of the same operation, using the same
            # dialect/schema tolerance so the comparison measures wrapper overhead
            # rather than CSV separator/header accidents.
            lf = _scan_files(input_files)
            if args.cols:
                requested = [c.strip() for c in args.cols.split(",") if c.strip()]
                required_extra = ["BaseDateTime"] if args.date_from and args.date_to else []
                lf, _ = _select_existing_columns(lf, requested, required_extra=required_extra)
            if args.date_from and args.date_to:
                start_dt = _parse_datetime_literal(args.date_from)
                end_dt = _parse_datetime_literal(args.date_to)
                schema_names = set(lf.collect_schema().names())
                if "BaseDateTime" not in schema_names:
                    raise pl.exceptions.ColumnNotFoundError(
                        "Raw Polars baseline could not find BaseDateTime after schema normalization; "
                        + _available_columns_message(schema_names)
                    )
                lf = lf.with_columns(
                    pl.coalesce(
                        [
                            pl.col("BaseDateTime").str.strptime(pl.Datetime, strict=False),
                            pl.col("BaseDateTime").str.to_datetime(strict=False),
                        ]
                    ).alias("ts")
                ).filter((pl.col("ts") >= pl.lit(start_dt)) & (pl.col("ts") < pl.lit(end_dt)))
            path.parent.mkdir(parents=True, exist_ok=True)
            try:
                lf.sink_parquet(str(path))
            except Exception:
                lf.collect(engine="streaming").write_parquet(path)

        results.append(
            _run_task(
                run=run,
                task="csv_scan_filter_to_parquet",
                engine="Raw Polars lazy",
                input_size_mb=input_size_mb,
                output_path=raw_polars_path,
                rows=rows,
                unique_mmsi=unique_mmsi,
                fn=run_raw_polars,
            )
        )

        # 5) Optional Pandas baseline. This is intentionally optional because it can
        # be slow or fail on memory-constrained laptops with larger AIS dumps.
        if args.include_pandas:
            pandas_path = run_dir / "pandas_scan.parquet"
            _safe_unlink(pandas_path)

            def run_pandas(path_out: Path = pandas_path) -> None:
                import pandas as pd

                frames = []
                requested = [c.strip() for c in args.cols.split(",") if c.strip()] if args.cols else None
                read_cols = list(requested) if requested else None
                if read_cols and args.date_from and args.date_to and "BaseDateTime" not in read_cols:
                    read_cols = read_cols + ["BaseDateTime"]
                for path in input_files:
                    sep = _infer_csv_separator(path)
                    # Read full columns first so we can normalize names. Passing
                    # usecols before schema normalization hides the real problem.
                    frame = pd.read_csv(path, sep=sep)
                    frame = frame.rename(columns=_canonical_rename_map(frame.columns))
                    if read_cols:
                        available = set(frame.columns)
                        keep = [c for c in read_cols if c in available]
                        if not keep:
                            raise ValueError(
                                "Pandas baseline found none of the requested columns after schema normalization. "
                                f"Requested: {read_cols}. {_available_columns_message(available)}"
                            )
                        frame = frame[keep]
                    if args.date_from and args.date_to:
                        frame["ts"] = pd.to_datetime(frame["BaseDateTime"], errors="coerce")
                        frame = frame[(frame["ts"] >= args.date_from) & (frame["ts"] < args.date_to)]
                    frames.append(frame)
                out = pd.concat(frames, ignore_index=True)
                out.to_parquet(path_out, index=False)

            results.append(
                _run_task(
                    run=run,
                    task="csv_scan_filter_to_parquet",
                    engine="Pandas baseline",
                    input_size_mb=input_size_mb,
                    output_path=pandas_path,
                    rows=rows,
                    unique_mmsi=unique_mmsi,
                    fn=run_pandas,
                )
            )

    return results


def benchmark(args: argparse.Namespace) -> list[BenchmarkResult]:
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    input_files = sorted(Path(args.root).glob(args.pattern))
    if not input_files:
        raise FileNotFoundError(f"No files match: {args.root}/{args.pattern}")
    input_size_mb = round(sum(p.stat().st_size for p in input_files if p.is_file()) / 1024**2, 2)

    _write_input_diagnostics(input_files, out_dir)

    ds = _base_dataset(args)
    rows, unique_mmsi = _count_metadata(ds)
    return _run_repeated(
        args,
        input_files=input_files,
        input_size_mb=input_size_mb,
        rows=rows,
        unique_mmsi=unique_mmsi,
        ds=ds,
        out_dir=out_dir,
    )


def _mean(values: list[float]) -> Optional[float]:
    return statistics.mean(values) if values else None


def _median(values: list[float]) -> Optional[float]:
    return statistics.median(values) if values else None


def _sd(values: list[float]) -> Optional[float]:
    return statistics.stdev(values) if len(values) >= 2 else 0.0 if len(values) == 1 else None


def _round_or_none(value: Optional[float], digits: int = 3) -> Optional[float]:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    return round(float(value), digits)


def _aggregate_for_manuscript(results: list[BenchmarkResult]) -> list[dict[str, object]]:
    groups: dict[tuple[str, str], list[BenchmarkResult]] = defaultdict(list)
    for result in results:
        groups[(result.task, result.engine)].append(result)

    rows: list[dict[str, object]] = []
    for (task, engine), items in sorted(groups.items()):
        ok_items = [r for r in items if r.status == "ok"]
        runtime = [r.runtime_s for r in ok_items]
        peak = [r.peak_rss_mb for r in ok_items if r.peak_rss_mb is not None]
        output_size = [r.output_size_mb for r in ok_items if r.output_size_mb is not None]
        output_rows = [r.output_rows for r in ok_items if r.output_rows is not None]
        first = items[0]
        rows.append(
            {
                "task": task,
                "engine": engine,
                "input_size_mb": first.input_size_mb,
                "rows": first.rows,
                "unique_mmsi": first.unique_mmsi,
                "n_runs": len(items),
                "n_ok": len(ok_items),
                "n_failed": len(items) - len(ok_items),
                "runtime_s_median": _round_or_none(_median(runtime)),
                "runtime_s_mean": _round_or_none(_mean(runtime)),
                "runtime_s_sd": _round_or_none(_sd(runtime)),
                "peak_rss_mb_median": _round_or_none(_median(peak), 2),
                "peak_rss_mb_mean": _round_or_none(_mean(peak), 2),
                "peak_rss_mb_sd": _round_or_none(_sd(peak), 2),
                "output_size_mb_median": _round_or_none(_median(output_size), 2),
                "output_rows_median": int(_median([float(v) for v in output_rows])) if output_rows else None,
                "status": "ok" if len(ok_items) == len(items) else "partial/failed",
            }
        )
    return rows


def _write_csv_dicts(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_results(results: list[BenchmarkResult], out_dir: Path) -> None:
    rows = [asdict(r) for r in results]
    csv_path = out_dir / "benchmark_results.csv"
    json_path = out_dir / "benchmark_results.json"
    _write_csv_dicts(csv_path, rows)
    json_path.write_text(json.dumps(rows, indent=2), encoding="utf-8")

    manuscript_rows = _aggregate_for_manuscript(results)
    manuscript_csv = out_dir / "benchmark_table_for_manuscript.csv"
    manuscript_json = out_dir / "benchmark_table_for_manuscript.json"
    _write_csv_dicts(manuscript_csv, manuscript_rows)
    manuscript_json.write_text(json.dumps(manuscript_rows, indent=2), encoding="utf-8")

    print(f"Wrote {csv_path}")
    print(f"Wrote {json_path}")
    print(f"Wrote {manuscript_csv}")
    print(f"Wrote {manuscript_json}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark AIStk on a large AIS CSV file.")
    parser.add_argument("--root", required=True, help="Input directory, e.g. data/raw")
    parser.add_argument("--pattern", default="*.csv", help="Input glob pattern, e.g. ais-2024-01-01.csv")
    parser.add_argument("--from", dest="date_from", default=None, help="Inclusive datetime start")
    parser.add_argument("--to", dest="date_to", default=None, help="Exclusive datetime end")
    parser.add_argument(
        "--cols",
        default="MMSI,BaseDateTime,LAT,LON,SOG,COG,Heading,IMO,CallSign,VesselName,VesselType,Status,Length,Width,Draft,Cargo,TransceiverClass",
        help="Comma-separated columns to keep for the scan/export benchmark.",
    )
    parser.add_argument("--out-dir", default="results/benchmark", help="Output directory")
    parser.add_argument("--repeats", type=int, default=3, help="Number of repeated runs for each benchmark task")
    parser.add_argument("--include-pandas", action="store_true", help="Run Pandas baseline as well")
    parser.add_argument("--skip-events", action="store_true", help="Skip vessel-wise event detection benchmark")
    parser.add_argument("--turn-deg", type=float, default=30.0, help="Sharp-turn threshold for event benchmark")
    parser.add_argument("--stop-sog", type=float, default=0.5, help="Stop SOG threshold for event benchmark")
    parser.add_argument("--stop-min", type=int, default=15, help="Stop duration threshold in minutes")
    parser.add_argument("--draft-jump-m", type=float, default=0.3, help="Draft-change threshold in metres")
    parser.add_argument("--gap-s", type=int, default=600, help="AIS signal-gap threshold in seconds")
    args = parser.parse_args()
    if args.repeats < 1:
        raise ValueError("--repeats must be >= 1")
    return args


if __name__ == "__main__":
    ns = parse_args()
    results_ = benchmark(ns)
    write_results(results_, Path(ns.out_dir))
    for item in results_:
        print(asdict(item))
