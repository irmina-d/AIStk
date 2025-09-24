from __future__ import annotations

import glob
import os
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional, Sequence, Union

import polars as pl

from .events import detect_events_df
from .stats import compute_stats_df
from .utils import haversine_km  # used indirectly by stats
from .viz import plot_track_html

PathLike = Union[str, Path]


def _scan_many(path: PathLike, pattern: str) -> pl.LazyFrame:
    """
    Scan a directory for many CSV files and return a concatenated LazyFrame.

    Parameters
    ----------
    path : str or pathlib.Path
        Root directory to search.
    pattern : str
        Glob pattern (e.g., ``"*.csv"``).

    Returns
    -------
    pl.LazyFrame
        Concatenated lazy scan of matching CSV files.

    Raises
    ------
    FileNotFoundError
        If no files match the given pattern.

    Notes
    -----
    - Uses ``pl.scan_csv`` with ``infer_schema_length=0`` and ``try_parse_dates=True``
      for performance and loose parsing on large AIS dumps.
    """
    files = sorted(glob.glob(os.path.join(str(path), pattern)))
    if not files:
        raise FileNotFoundError(f"No files match: {path}/{pattern}")
    scans = [
        pl.scan_csv(
            f,
            has_header=True,
            infer_schema_length=0,
            ignore_errors=True,
            try_parse_dates=True,
        )
        for f in files
    ]
    return pl.concat(scans, rechunk=False)


def _valid_geo() -> pl.Expr:
    """
    Polars expression that validates geographic bounds for LAT/LON.

    Returns
    -------
    pl.Expr
        Boolean expression: ``-90 <= LAT <= 90`` and ``-180 <= LON <= 180``.
    """
    lat = pl.col("LAT").cast(pl.Float64, strict=False)
    lon = pl.col("LON").cast(pl.Float64, strict=False)
    return lat.is_between(-90, 90) & lon.is_between(-180, 180)


def _parse_temporal_literal(value: str) -> datetime:
    """Parse an ISO-like timestamp string into a Python ``datetime``.

    Parameters
    ----------
    value : str
        Literal to parse. ``strict=False`` parsing is used to accept a
        reasonably wide range of ISO formats (e.g. ``YYYY-MM-DD`` and
        ``YYYY-MM-DDTHH:MM:SS``).

    Returns
    -------
    datetime
        Parsed timestamp.

    Raises
    ------
    ValueError
        If the literal cannot be parsed into a timestamp.
    """

    parsed = pl.Series([value]).str.to_datetime(strict=False)
    dt = parsed.to_list()[0]
    if dt is None:
        raise ValueError(f"Could not parse datetime literal: {value!r}")
    return dt


def _ts_expr() -> pl.Expr:
    """
    Polars expression that creates/normalizes a timestamp column named ``ts``.

    Returns
    -------
    pl.Expr
        Expression that tries to parse ``BaseDateTime`` into a Polars ``Datetime``
        using `str.strptime`; falls back to `str.to_datetime`. The resulting
        column is aliased to ``"ts"``.
    """
    return pl.coalesce(
        [
            pl.col("BaseDateTime").str.strptime(pl.Datetime, strict=False),
            pl.col("BaseDateTime").str.to_datetime(strict=False),
        ]
    ).alias("ts")


class AISDataset:
    """
    High-level **lazy** dataset wrapper for decoded AIS CSV files.

    This class encapsulates file discovery, lightweight column selection and
    filtering, with optional timestamp materialization. It keeps a `LazyFrame`
    internally and only materializes on `collect()` / I/O / analytics calls.

    Parameters
    ----------
    root : str or pathlib.Path
        Directory containing AIS CSV files.
    pattern : str, default="*.csv"
        Glob pattern used during discovery.

    Attributes
    ----------
    root : str
        Root directory (as provided).
    pattern : str
        Glob pattern (as provided).
    """

    # -----------------------
    # Construction
    # -----------------------
    def __init__(self, root: PathLike, pattern: str = "*.csv") -> None:
        self.root: str = str(root)
        self.pattern: str = pattern
        self._lf: pl.LazyFrame = _scan_many(self.root, pattern)
        self._filters: list[pl.Expr] = []
        self._selected: Optional[Sequence[str]] = None
        self._need_ts: bool = False

    # -----------------------
    # Configuration
    # -----------------------
    def with_columns(self, cols: Sequence[str]) -> "AISDataset":
        """
        Restrict the dataset to a subset of columns (lazy).

        Parameters
        ----------
        cols : sequence of str
            Column names to retain.

        Returns
        -------
        AISDataset
            Self, for chaining.
        """
        self._selected = cols
        return self

    def between(self, start: str, end: str) -> "AISDataset":
        """
        Filter by a closed time interval on the derived ``ts`` column.

        Parameters
        ----------
        start : str
            Inclusive start (ISO-like string parsable by Polars).
        end : str
            Exclusive end (ISO-like string).

        Returns
        -------
        AISDataset
            Self, for chaining.

        Notes
        -----
        - This marks that a timestamp column ``ts`` must be materialized.
        """
        self._need_ts = True

        start_dt = _parse_temporal_literal(start)
        end_dt = _parse_temporal_literal(end)
        self._filters.append((pl.col("ts") >= pl.lit(start_dt)) & (pl.col("ts") < pl.lit(end_dt)))
        return self

    def filter(
        self,
        mmsi: Optional[Union[int, Iterable[int]]] = None,
        imo: Optional[Union[int, Iterable[int]]] = None,
        callsign: Optional[Union[str, Iterable[str]]] = None,
    ) -> "AISDataset":
        """
        Filter by vessel identifiers (MMSI/IMO/CallSign).

        Parameters
        ----------
        mmsi : int or iterable of int, optional
            Single MMSI or a collection of MMSIs to include.
        imo : int or iterable of int, optional
            Single IMO or a collection of IMOs to include.
        callsign : str or iterable of str, optional
            Single call sign or a collection of call signs to include.

        Returns
        -------
        AISDataset
            Self, for chaining.
        """
        def _coerce_int_values(values: Iterable[Union[int, str, None]]) -> list[int]:
            coerced: list[int] = []
            for v in values:
                if v is None:
                    continue
                coerced.append(int(v))
            return coerced

        if mmsi is not None:
            if isinstance(mmsi, (list, tuple, set)):
                coerced = _coerce_int_values(mmsi)
            else:
                coerced = int(mmsi)
            expr = (
                pl.col("MMSI").cast(pl.Int64, strict=False).is_in(coerced)
                if isinstance(coerced, list)
                else pl.col("MMSI").cast(pl.Int64, strict=False) == coerced
            )
            self._filters.append(expr)

        if imo is not None:
            if isinstance(imo, (list, tuple, set)):
                coerced = _coerce_int_values(imo)
            else:
                coerced = int(imo)
            expr = (
                pl.col("IMO").cast(pl.Int64, strict=False).is_in(coerced)
                if isinstance(coerced, list)
                else pl.col("IMO").cast(pl.Int64, strict=False) == coerced
            )
            self._filters.append(expr)

        if callsign is not None:
            if isinstance(callsign, (list, tuple, set)):
                self._filters.append(pl.col("CallSign").is_in(list(callsign)))
            else:
                self._filters.append(pl.col("CallSign") == callsign)

        return self

    # -----------------------
    # Build LazyFrame
    # -----------------------
    def _build(self) -> pl.LazyFrame:
        """
        Compose the internal LazyFrame with pending selections/filters.

        Returns
        -------
        pl.LazyFrame
            The lazily-built frame with optional `ts` and geo validation.

        Notes
        -----
        - If any time filter was requested or a filter references `BaseDateTime`,
          the ``ts`` column is derived via :func:`_ts_expr`.
        - If ``LAT`` and ``LON`` exist, geographic bounds are enforced via
          :func:`_valid_geo`.
        - If ``ts`` exists, sorting is applied (by ``["MMSI","ts"]`` if possible).
        """
        lf = self._lf

        if self._selected:
            lf = lf.select([pl.col(c) for c in self._selected if isinstance(c, str)])

        need_ts = self._need_ts or any("BaseDateTime" in str(f) for f in self._filters)
        if need_ts:
            lf = lf.with_columns(_ts_expr())

        if self._filters:
            cond = self._filters[0]
            for f in self._filters[1:]:
                cond = cond & f
            lf = lf.filter(cond)

        schema_names = set(lf.collect_schema().names())

        numeric_casts: list[pl.Expr] = []
        numeric_dtypes = {
            "LAT": pl.Float64,
            "LON": pl.Float64,
            "SOG": pl.Float64,
            "COG": pl.Float64,
            "Draft": pl.Float64,
            "MMSI": pl.Int64,
            "IMO": pl.Int64,
        }
        for col, dtype in numeric_dtypes.items():
            if col in schema_names:
                numeric_casts.append(pl.col(col).cast(dtype, strict=False).alias(col))
        if numeric_casts:
            lf = lf.with_columns(numeric_casts)

        if {"LAT", "LON"}.issubset(schema_names):
            lf = lf.filter(_valid_geo())

        schema_names = set(lf.collect_schema().names())
        if {"ts", "MMSI"}.issubset(schema_names):
            lf = lf.sort(["MMSI", "ts"])
        elif "ts" in schema_names:
            lf = lf.sort("ts")

        return lf

    # -----------------------
    # Materialization / I/O
    # -----------------------
    def collect(self) -> pl.DataFrame:
        """
        Materialize the dataset as a Polars DataFrame.

        Returns
        -------
        pl.DataFrame
            Collected frame (streaming enabled).
        """
        return self._build().collect(engine="streaming")

    def write_parquet(self, path: PathLike, partition: Optional[str] = None) -> None:
        """
        Write the (materialized) dataset to a Parquet file.

        Parameters
        ----------
        path : str or pathlib.Path
            Output file path (or directory if you later add partitioned writes).
        partition : {"year", "year/month", "year/month/day"}, optional
            When provided **and** ``ts`` exists, helper columns (``year``,
            ``month``, ``day``) are added before writing. (Current
            implementation writes a single Parquet file; it does not create a
            directory tree per partition yet.)

        Notes
        -----
        - Extend this method if you need true partitioned output layout
          (e.g., ``root/year=YYYY/month=MM/day=DD/*.parquet``).
        """
        df = self.collect()
        if partition and "ts" in df.columns:
            if partition.lower() in {"year", "year/month", "year/month/day"}:
                df = df.with_columns(
                    [
                        pl.col("ts").dt.year().alias("year"),
                        pl.col("ts").dt.month().alias("month"),
                        pl.col("ts").dt.day().alias("day"),
                    ]
                )
            df.write_parquet(str(path))
        else:
            df.write_parquet(str(path))

    # -----------------------
    # Analytics
    # -----------------------
    def stats(self, level: str = "mmsi") -> pl.DataFrame:
        """
        Compute trajectory statistics for the dataset.

        Parameters
        ----------
        level : {"mmsi"}, default="mmsi"
            Aggregation level passed through to :func:`compute_stats_df`.

        Returns
        -------
        pl.DataFrame
            Aggregated metrics.
        """
        df = self.collect()
        return compute_stats_df(df, level=level)

    def detect_events(
        self,
        turn_deg: float = 30.0,
        stop_sog: float = 0.5,
        stop_min: int = 15,
        draft_jump_m: float = 0.3,
    ) -> pl.DataFrame:
        """
        Detect navigational events for the dataset.

        Parameters
        ----------
        turn_deg : float, default=30.0
            Minimum heading change to flag a "sharp_turn".
        stop_sog : float, default=0.5
            Speed-over-ground threshold (knots) for stop detection.
        stop_min : int, default=15
            Minimum stop duration (minutes).
        draft_jump_m : float, default=0.3
            Draught change threshold (meters).

        Returns
        -------
        pl.DataFrame
            Event table as produced by :func:`detect_events_df`.
        """
        df = self.collect()
        return detect_events_df(
            df,
            turn_deg=turn_deg,
            stop_sog=stop_sog,
            stop_min=stop_min,
            draft_jump_m=draft_jump_m,
        )

    # -----------------------
    # Visualization
    # -----------------------
    def plot_map(self, out_html: PathLike, mmsi: Optional[int] = None) -> str:
        """
        Export a quick interactive HTML map for visual inspection.

        Parameters
        ----------
        out_html : str or pathlib.Path
            Output HTML file path.
        mmsi : int, optional
            If provided and ``"MMSI"`` exists, restrict the view to this vessel.

        Returns
        -------
        str
            Path to the written HTML file.
        """
        df = self.collect()
        if mmsi is not None and "MMSI" in df.columns:
            df = df.filter(pl.col("MMSI") == mmsi)
        return plot_track_html(df, out_html)
