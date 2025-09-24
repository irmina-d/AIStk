from __future__ import annotations

from typing import List, Optional, Sequence, Literal

import typer

app = typer.Typer(help="AIS Toolkit CLI")


def _parse_mmsi_csv(mmsi: Optional[str]) -> Optional[List[int]]:
    """Parse a comma-separated MMSI string into a list of ints."""
    if not mmsi:
        return None
    vals: List[int] = []
    for x in mmsi.split(","):
        s = x.strip()
        if not s:
            continue
        try:
            vals.append(int(s))
        except ValueError:
            raise typer.BadParameter(f"MMSI must be integers, got: {s!r}")
    return vals


# ---------------------------------------------------------------------
# SCAN
# ---------------------------------------------------------------------
@app.command()
def scan(
    root: str = typer.Argument(..., help="Root directory with AIS files."),
    pattern: str = typer.Option("*.csv", help="Glob pattern for input files, e.g. '*.csv' or '*.parquet'."),
    date_from: Optional[str] = typer.Option(None, "--from", help="Inclusive ISO datetime start, e.g. 2024-01-01T00:00:00"),
    date_to: Optional[str] = typer.Option(None, "--to", help="Exclusive ISO datetime end, e.g. 2024-02-01T00:00:00"),
    mmsi: Optional[str] = typer.Option(None, help="Single MMSI or comma-separated list, e.g. '244660000,244770000'."),
    cols: Optional[str] = typer.Option(None, help="Comma-separated list of columns to keep."),
    to_parquet: Optional[str] = typer.Option(None, help="Write result to this Parquet file."),
    html: Optional[str] = typer.Option(None, help="Save an interactive track map (HTML)."),
    engine: Literal["polars", "dask", "spark"] = typer.Option("polars", help="Execution engine."),
    stream: bool = typer.Option(False, help="Streaming/lazy path when supported (Polars)."),
) -> None:
    """
    Scan AIS files and optionally filter, export, and/or plot a map.

    Pipeline
    --------
    1) Discover files under ROOT matching PATTERN,
    2) (Optional) select COLUMNS,
    3) (Optional) filter by date range and MMSI,
    4) (Optional) write Parquet and/or HTML map.

    Examples
    --------
    aistk scan data/ais
    aistk scan data/ais --from 2024-01-01 --to 2024-02-01 --mmsi 244660000,244770000 --to-parquet out/data.parquet
    aistk scan data/ais --cols MMSI,LAT,LON,COG,SOG --html out/map.html
    """
    # Basic validation of date range pairing
    if (date_from and not date_to) or (date_to and not date_from):
        raise typer.BadParameter("--from and --to must be provided together")

    mmsi_list = _parse_mmsi_csv(mmsi)

    if engine == "polars":
        from .core import AISDataset  # lazy import to keep CLI light
        import polars as pl

        ds = AISDataset(root, pattern=pattern)

        if cols:
            ds = ds.with_columns([c.strip() for c in cols.split(",") if c.strip()])

        if date_from and date_to:
            ds = ds.between(date_from, date_to)

        if mmsi_list:
            ds = ds.filter(mmsi=mmsi_list)

        if stream:
            # streaming/lazy path
            from .stats_streaming import compute_stats_lazy

            lf = ds._build()
            # optional outputs
            if to_parquet:
                # If Polars version supports sink_parquet, prefer it; otherwise collect streaming.
                lf.collect(engine="streaming").write_parquet(to_parquet)
                typer.echo(f"Written Parquet to {to_parquet} (streaming collect)")

            if html:
                df = lf.collect(engine="streaming")
                if mmsi_list and "MMSI" in df.columns:
                    df = df.filter(pl.col("MMSI").is_in(mmsi_list))
                from .viz import plot_track_html
                plot_track_html(df, html)
                typer.echo(f"Wrote map to {html}")

            # Show summary stats to console
            stats_df = compute_stats_lazy(lf, level="mmsi").collect(engine="streaming")
            typer.echo(stats_df.head().to_string())
            return

        # classic path
        if to_parquet:
            ds.write_parquet(to_parquet)
            typer.echo(f"Written Parquet to {to_parquet}")

        if html:
            ds.plot_map(html)
            typer.echo(f"Wrote map to {html}")

        return

    elif engine == "dask":
        import dask.dataframe as dd

        path = f"{root}/{pattern}"
        if pattern.endswith(".parquet") or "*.parquet" in pattern:
            ddf = dd.read_parquet(path)
        else:
            ddf = dd.read_csv(path, blocksize="256MB", assume_missing=True)

        # filters
        if date_from and date_to and "ts" in ddf.columns:
            ddf = ddf[(ddf["ts"] >= date_from) & (ddf["ts"] < date_to)]
        if mmsi_list and "MMSI" in ddf.columns:
            ddf = ddf[ddf["MMSI"].isin(mmsi_list)]

        if to_parquet:
            ddf.to_parquet(to_parquet, write_index=False)
            typer.echo(f"Wrote Dask Parquet dataset to {to_parquet}")

        if html:
            typer.echo("HTML plotting is only supported on Polars DataFrame (use --engine polars).")

        return

    elif engine == "spark":
        try:
            from pyspark.sql import SparkSession
        except Exception as e:  # pragma: no cover
            raise typer.BadParameter(f"Spark not available: {e}")

        spark = SparkSession.builder.appName("aistk-scan").getOrCreate()
        path = f"{root}/{pattern}"
        if pattern.endswith(".parquet") or "*.parquet" in pattern:
            sdf = spark.read.parquet(path)
        else:
            sdf = spark.read.option("header", True).csv(path, inferSchema=True)

        if date_from and date_to and "ts" in sdf.columns:
            sdf = sdf.where((sdf.ts >= date_from) & (sdf.ts < date_to))
        if mmsi_list and "MMSI" in sdf.columns:
            sdf = sdf.where(sdf.MMSI.isin(mmsi_list))

        if to_parquet:
            sdf.write.mode("overwrite").parquet(to_parquet)
            typer.echo(f"Wrote Spark Parquet to {to_parquet}")

        if html:
            typer.echo("HTML plotting is only supported on Polars DataFrame (use --engine polars).")

        spark.stop()
        return

    else:
        raise typer.BadParameter(f"Unknown engine: {engine}")


# ---------------------------------------------------------------------
# STATS
# ---------------------------------------------------------------------
@app.command()
def stats(
    root: str = typer.Argument(..., help="Root directory with AIS files."),
    pattern: str = typer.Option("*.csv", help="Glob pattern for input files."),
    level: Literal["mmsi"] = typer.Option("mmsi", help="Aggregation level."),
    mmsi: Optional[str] = typer.Option(None, help="Filter MMSI(s), comma-separated."),
    date_from: Optional[str] = typer.Option(None, "--from", help="Inclusive ISO datetime start."),
    date_to: Optional[str] = typer.Option(None, "--to", help="Exclusive ISO datetime end."),
    engine: Literal["polars", "polars-stream", "dask", "spark"] = typer.Option(
        "polars", help="Execution engine (polars-stream uses lazy streaming)."
    ),
    out: Optional[str] = typer.Option(None, help="Write aggregated stats to this Parquet/CSV (by extension)."),
) -> None:
    """
    Compute trajectory statistics for a dataset (per MMSI by default).

    Examples
    --------
    aistk stats data/ais --from 2024-01-01 --to 2024-02-01 --mmsi 244660000
    aistk stats data/ais --engine polars-stream --out stats.parquet
    """
    if (date_from and not date_to) or (date_to and not date_from):
        raise typer.BadParameter("--from and --to must be provided together")

    mmsi_list = _parse_mmsi_csv(mmsi)

    if engine == "polars":
        from .core import AISDataset
        ds = AISDataset(root, pattern=pattern)
        if date_from and date_to:
            ds = ds.between(date_from, date_to)
        if mmsi_list:
            ds = ds.filter(mmsi=mmsi_list)
        df = ds.stats(level=level)
        if out:
            if out.endswith(".csv"):
                df.write_csv(out)
            else:
                df.write_parquet(out)
            typer.echo(f"Wrote stats to {out}")
        else:
            typer.echo(df.head().to_string())
        return

    if engine == "polars-stream":
        import polars as pl
        from .core import AISDataset
        from .stats_streaming import compute_stats_lazy

        ds = AISDataset(root, pattern=pattern)
        if date_from and date_to:
            ds = ds.between(date_from, date_to)
        if mmsi_list:
            ds = ds.filter(mmsi=mmsi_list)

        lf = ds._build()
        out_df = compute_stats_lazy(lf, level=level).collect(engine="streaming")
        if out:
            if out.endswith(".csv"):
                out_df.write_csv(out)
            else:
                out_df.write_parquet(out)
            typer.echo(f"Wrote stats to {out}")
        else:
            typer.echo(out_df.head().to_string())
        return

    if engine == "dask":
        import dask.dataframe as dd
        from .backends.dask_backend import compute_stats_dask

        path = f"{root}/{pattern}"
        ddf = dd.read_parquet(path) if (pattern.endswith(".parquet") or "*.parquet" in pattern) else dd.read_csv(
            path, blocksize="256MB", assume_missing=True
        )
        if date_from and date_to and "ts" in ddf.columns:
            ddf = ddf[(ddf["ts"] >= date_from) & (ddf["ts"] < date_to)]
        if mmsi_list and "MMSI" in ddf.columns:
            ddf = ddf[ddf["MMSI"].isin(mmsi_list)]
        res = compute_stats_dask(ddf, level=level)
        if out:
            if out.endswith(".csv"):
                res.to_csv(out, index=False)
            else:
                import pandas as pd  # to write parquet via pandas
                if out.endswith(".parquet"):
                    try:
                        res.to_parquet(out, index=False)
                    except Exception:
                        pd.DataFrame(res).to_parquet(out, index=False)
                else:
                    pd.DataFrame(res).to_parquet(out, index=False)
            typer.echo(f"Wrote stats to {out}")
        else:
            typer.echo(res.head().to_string())
        return

    if engine == "spark":
        from pyspark.sql import SparkSession
        from .backends.spark_backend import compute_stats_spark

        spark = SparkSession.builder.appName("aistk-stats").getOrCreate()
        path = f"{root}/{pattern}"
        sdf = spark.read.parquet(path) if (pattern.endswith(".parquet") or "*.parquet" in pattern) else \
              spark.read.option("header", True).csv(path, inferSchema=True)
        if date_from and date_to and "ts" in sdf.columns:
            sdf = sdf.where((sdf.ts >= date_from) & (sdf.ts < date_to))
        if mmsi_list and "MMSI" in sdf.columns:
            sdf = sdf.where(sdf.MMSI.isin(mmsi_list))
        out_df = compute_stats_spark(sdf, level=level)
        if out:
            if out.endswith(".parquet"):
                out_df.write.mode("overwrite").parquet(out)
            else:
                out_df.write.mode("overwrite").csv(out, header=True)
            typer.echo(f"Wrote stats to {out}")
        else:
            out_df.show(20, truncate=False)
        spark.stop()
        return

    raise typer.BadParameter(f"Unknown engine: {engine}")


# ---------------------------------------------------------------------
# EVENTS
# ---------------------------------------------------------------------
@app.command()
def events(
    root: str = typer.Argument(..., help="Root directory with AIS files."),
    pattern: str = typer.Option("*.csv", help="Glob pattern for input files."),
    mmsi: Optional[str] = typer.Option(None, help="Filter MMSI(s), comma-separated."),
    date_from: Optional[str] = typer.Option(None, "--from", help="Inclusive ISO datetime start."),
    date_to: Optional[str] = typer.Option(None, "--to", help="Exclusive ISO datetime end."),
    turn_deg: float = typer.Option(30.0, help="Sharp turn threshold (deg)."),
    stop_sog: float = typer.Option(0.5, help="Stop SOG threshold (knots)."),
    stop_min: int = typer.Option(15, help="Stop minimum duration (minutes)."),
    draft_jump_m: float = typer.Option(0.3, help="Draft change threshold (m)."),
    out: Optional[str] = typer.Option(None, help="Write events to this Parquet/CSV."),
) -> None:
    """
    Detect navigational events (sharp_turn, stop, gap, draft_change) on a dataset.
    """
    if (date_from and not date_to) or (date_to and not date_from):
        raise typer.BadParameter("--from and --to must be provided together")

    mmsi_list = _parse_mmsi_csv(mmsi)

    from .core import AISDataset
    from .events import detect_events_df

    ds = AISDataset(root, pattern=pattern)
    if date_from and date_to:
        ds = ds.between(date_from, date_to)
    if mmsi_list:
        ds = ds.filter(mmsi=mmsi_list)

    df = ds.collect()
    ev = detect_events_df(df, turn_deg=turn_deg, stop_sog=stop_sog, stop_min=stop_min, draft_jump_m=draft_jump_m)
    if out:
        if out.endswith(".csv"):
            ev.write_csv(out)
        else:
            ev.write_parquet(out)
        typer.echo(f"Wrote events to {out}")
    else:
        typer.echo(ev.head().to_string())


# ---------------------------------------------------------------------
# STREAM-CVS (pseudo-stream)
# ---------------------------------------------------------------------
@app.command(name="stream-csv")
def stream_csv(
    path: str = typer.Argument(..., help="CSV file to stream as if arriving online."),
    chunk_size: int = typer.Option(10_000, help="Rows per chunk to simulate streaming."),
    mmsi: Optional[str] = typer.Option(None, help="Filter MMSI(s) (comma-separated) before streaming."),
    turn_deg: float = typer.Option(30.0, help="Sharp turn threshold (deg)."),
    stop_sog: float = typer.Option(0.5, help="Stop SOG threshold (knots)."),
    stop_min: int = typer.Option(15, help="Stop minimum duration (minutes)."),
    gap_s: int = typer.Option(600, help="Gap threshold (seconds)."),
    draft_jump_m: float = typer.Option(0.3, help="Draft change threshold (m)."),
) -> None:
    """
    Simulate an online stream from a CSV: read in chunks and emit events on the fly.

    Prints JSON lines to stdout for each detected event.
    """
    import json
    import polars as pl

    from .streaming.events_online import process_stream

    lf = pl.scan_csv(path, has_header=True, infer_schema_length=0, ignore_errors=True, try_parse_dates=True)

    mmsi_list = _parse_mmsi_csv(mmsi)
    schema_names = set(lf.collect_schema().names())
    if mmsi_list and "MMSI" in schema_names:
        import polars as pl
        lf = lf.filter(pl.col("MMSI").is_in(mmsi_list))

    offset = 0
    while True:
        df = lf.slice(offset, chunk_size).collect(engine="streaming")
        if df.height == 0:
            break
        cols = [c for c in ["MMSI", "ts", "LAT", "LON", "COG", "SOG", "Draft"] if c in df.columns]
        recs = (dict(zip(cols, row)) for row in df.select(cols).iter_rows())
        for ev in process_stream(
            recs,
            turn_deg=turn_deg,
            stop_sog=stop_sog,
            stop_min=stop_min,
            gap_s=gap_s,
            draft_jump_m=draft_jump_m,
        ):
            typer.echo(json.dumps(ev))
        offset += chunk_size


if __name__ == "__main__":
    app()
