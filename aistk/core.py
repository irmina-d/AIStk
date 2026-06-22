from __future__ import annotations

import glob
import os
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional, Sequence, Union

import polars as pl

from .events import detect_events_df
from .stats import compute_stats_df
from .stats_streaming import compute_stats_lazy
from .utils import haversine_km  # used indirectly by stats
from .viz import plot_track_html

PathLike = Union[str, Path]


_AIS_COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    "MMSI": ("MMSI", "mmsi", "maritime_mobile_service_identity", "maritimemobileserviceidentity"),
    "BaseDateTime": (
        "BaseDateTime",
        "base_date_time",
        "basedatetime",
        "base datetime",
        "timestamp",
        "datetime",
        "date_time",
        "time",
        "ts",
    ),
    "LAT": ("LAT", "lat", "latitude", "Latitude"),
    "LON": ("LON", "lon", "long", "lng", "longitude", "Longitude"),
    "SOG": ("SOG", "sog", "speed_over_ground", "speed over ground"),
    "COG": ("COG", "cog", "course_over_ground", "course over ground"),
    "Heading": ("Heading", "heading", "true_heading", "true heading"),
    "IMO": ("IMO", "imo", "imo_number", "imo number"),
    "CallSign": ("CallSign", "callsign", "call_sign", "call sign"),
    "VesselName": ("VesselName", "vesselname", "vessel_name", "vessel name", "shipname", "ship_name"),
    "VesselType": ("VesselType", "vesseltype", "vessel_type", "vessel type"),
    "Status": ("Status", "status", "nav_status", "navigation_status"),
    "Length": ("Length", "length", "length_m", "length_meters"),
    "Width": ("Width", "width", "width_m", "width_meters"),
    "Draft": ("Draft", "draft", "draught", "Draught", "draught_m", "draft_m"),
    "Cargo": ("Cargo", "cargo"),
    "TransceiverClass": (
        "TransceiverClass",
        "transceiverclass",
        "transceiver_class",
        "transceiver class",
        "transceiver",
        "ais_transceiver",
        "class",
    ),
}


def _column_key(name: str) -> str:
    """Normalize a column name for tolerant AIS schema matching."""
    cleaned = str(name).replace("\ufeff", "").strip().lower()
    return "".join(ch for ch in cleaned if ch.isalnum())


def _canonical_rename_map(columns: Iterable[str]) -> dict[str, str]:
    """Return a safe rename mapping from common AIS variants to canonical names."""
    alias_to_canonical: dict[str, str] = {}
    for canonical, aliases in _AIS_COLUMN_ALIASES.items():
        for alias in aliases:
            alias_to_canonical[_column_key(alias)] = canonical

    existing = list(columns)
    used_targets = set(existing)
    mapping: dict[str, str] = {}
    for col in existing:
        stripped = str(col).replace("\ufeff", "").strip()
        target = alias_to_canonical.get(_column_key(stripped), stripped)
        # Rename BOM/whitespace variants and known aliases, but never create a
        # duplicate target column. Duplicate schemas should be handled upstream.
        if target != col and target not in used_targets - {col} and target not in mapping.values():
            mapping[col] = target
            used_targets.add(target)
    return mapping


def _normalize_ais_columns(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Normalize common AIS column-name variants in a LazyFrame."""
    rename_map = _canonical_rename_map(lf.collect_schema().names())
    return lf.rename(rename_map) if rename_map else lf


def _infer_csv_separator(path: PathLike, sample_bytes: int = 8192) -> str:
    """Infer a simple CSV separator from the first non-empty line of a file."""
    path = Path(path)
    with path.open("rb") as f:
        sample = f.read(sample_bytes)
    text = sample.decode("utf-8-sig", errors="ignore")
    first_line = next((line for line in text.splitlines() if line.strip()), "")
    candidates = [",", ";", "\t", "|"]
    counts = {sep: first_line.count(sep) for sep in candidates}
    best = max(counts, key=counts.get)
    return best if counts[best] > 0 else ","


def _discover_files(path: PathLike, pattern: str) -> list[str]:
    """Discover input CSV files and fail loudly when the glob is empty."""
    files = sorted(glob.glob(os.path.join(str(path), pattern)))
    if not files:
        raise FileNotFoundError(f"No files match: {path}/{pattern}")
    return files


def _scan_files(files: Sequence[PathLike]) -> pl.LazyFrame:
    """Scan AIS CSV files with per-file separator inference and schema normalization."""
    if not files:
        raise FileNotFoundError("No input files were provided to _scan_files().")

    scans: list[pl.LazyFrame] = []
    for file in files:
        sep = _infer_csv_separator(file)
        scans.append(
            pl.scan_csv(
                str(file),
                has_header=True,
                separator=sep,
                infer_schema_length=0,
                ignore_errors=True,
                try_parse_dates=True,
                truncate_ragged_lines=True,
            )
        )

    try:
        lf = pl.concat(scans, how="diagonal_relaxed", rechunk=False)
    except TypeError:  # older Polars fallback
        lf = pl.concat(scans, rechunk=False)
    return _normalize_ais_columns(lf)


def _scan_many(path: PathLike, pattern: str) -> pl.LazyFrame:
    """
    Scan a directory for many AIS CSV files and return a concatenated LazyFrame.

    The scanner is intentionally tolerant for public AIS dumps used in reviewer
    benchmarks: it infers the separator from the header line and normalizes
    common column-name variants (e.g. ``mmsi`` -> ``MMSI``, ``timestamp`` ->
    ``BaseDateTime``). This prevents large-file benchmarks from silently
    producing zero-column plans when a CSV uses a different dialect.
    """
    return _scan_files(_discover_files(path, pattern))


def _available_columns_message(available: Iterable[str]) -> str:
    cols = list(available)
    preview = ", ".join(cols[:40])
    suffix = " ..." if len(cols) > 40 else ""
    return f"available columns: [{preview}{suffix}]"


def _select_existing_columns(
    lf: pl.LazyFrame,
    requested: Sequence[str],
    *,
    required_extra: Sequence[str] = (),
) -> tuple[pl.LazyFrame, list[str]]:
    """Select requested columns, retaining required extras, and fail informatively."""
    available = set(lf.collect_schema().names())
    requested_clean = [c for c in requested if isinstance(c, str) and c]
    selected = [c for c in requested_clean if c in available]
    missing = [c for c in requested_clean if c not in available]
    extras = [c for c in required_extra if c in available and c not in selected]

    if requested_clean and not selected:
        raise ValueError(
            "None of the requested columns were found after AIS schema normalization. "
            f"Requested: {requested_clean}. {_available_columns_message(available)}. "
            "Check the CSV separator/header, or run the benchmark with --cols matching the file."
        )

    keep = list(dict.fromkeys(selected + extras))
    if not keep:
        return lf, selected
    return lf.select([pl.col(c) for c in keep]), selected


def _with_ts(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Create/normalize a ``ts`` column from ``BaseDateTime`` or existing ``ts``."""
    schema = lf.collect_schema()
    names = set(schema.names())
    if "BaseDateTime" in names:
        return lf.with_columns(
            pl.coalesce(
                [
                    pl.col("BaseDateTime").str.strptime(pl.Datetime, strict=False),
                    pl.col("BaseDateTime").str.to_datetime(strict=False),
                ]
            ).alias("ts")
        )
    if "ts" in names:
        dtype = schema["ts"]
        if dtype == pl.Datetime:
            return lf
        return lf.with_columns(pl.col("ts").str.to_datetime(strict=False).alias("ts"))
    raise pl.exceptions.ColumnNotFoundError(
        "AIStk could not create a timestamp column. Expected 'BaseDateTime' "
        f"or 'ts'; {_available_columns_message(names)}."
    )

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
        self.files: list[str] = _discover_files(self.root, pattern)
        self._lf: pl.LazyFrame = _scan_files(self.files)
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
    def _build(self, sort_rows: bool = True) -> pl.LazyFrame:
        """
        Compose the internal LazyFrame with pending selections/filters.

        Parameters
        ----------
        sort_rows : bool, default=True
            Sort by ``[MMSI, ts]`` when available. Disable for raw CSV→Parquet
            conversion benchmarks where preserving input order avoids a costly
            global sort.

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

        need_ts = self._need_ts or any("BaseDateTime" in str(f) or "ts" in str(f) for f in self._filters)
        final_selected: Optional[list[str]] = None
        if self._selected:
            available = set(lf.collect_schema().names())
            required_extra: list[str] = []
            # Keep a timestamp source available for filtering even if the user
            # did not request it in --cols. We select final requested columns
            # after filters have been applied.
            if need_ts:
                if "BaseDateTime" in available and "BaseDateTime" not in self._selected:
                    required_extra.append("BaseDateTime")
                elif "ts" in available and "ts" not in self._selected:
                    required_extra.append("ts")
            lf, selected = _select_existing_columns(lf, self._selected, required_extra=required_extra)
            final_selected = list(selected)

        if need_ts:
            lf = _with_ts(lf)

        if self._filters:
            cond = self._filters[0]
            for f in self._filters[1:]:
                cond = cond & f
            lf = lf.filter(cond)

        if final_selected is not None:
            # Preserve user-requested columns and retain derived ts when it was
            # needed for filtering or downstream trajectory operations.
            keep = list(dict.fromkeys(final_selected + (["ts"] if need_ts else [])))
            lf = lf.select([pl.col(c) for c in keep if c in lf.collect_schema().names()])

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
        if sort_rows:
            if {"ts", "MMSI"}.issubset(schema_names):
                lf = lf.sort(["MMSI", "ts"])
            elif "ts" in schema_names:
                lf = lf.sort("ts")

        return lf

    def lazy(self, sort_rows: bool = True) -> pl.LazyFrame:
        """Return the built Polars LazyFrame without materialising it."""
        return self._build(sort_rows=sort_rows)

    # -----------------------
    # Materialization / I/O
    # -----------------------
    def collect(self, sort_rows: bool = True) -> pl.DataFrame:
        """
        Materialize the dataset as a Polars DataFrame.

        Returns
        -------
        pl.DataFrame
            Collected frame (streaming enabled).
        """
        return self._build(sort_rows=sort_rows).collect(engine="streaming")

    def write_parquet(
        self,
        path: PathLike,
        partition: Optional[str] = None,
        *,
        streaming: bool = True,
        sort_rows: bool = False,
    ) -> None:
        """
        Write the dataset to a Parquet file.

        For large raw AIS CSV exports, ``sort_rows=False`` avoids an expensive
        global sort and is the recommended CSV→Parquet benchmarking path. When
        ``streaming=True``, AIStk first tries Polars ``sink_parquet`` and falls
        back to streaming collection if the current Polars plan/version does not
        support direct sinking.
        """
        out_path = Path(path)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        if partition:
            df = self.collect(sort_rows=sort_rows)
            if "ts" in df.columns and partition.lower() in {"year", "year/month", "year/month/day"}:
                df = df.with_columns(
                    [
                        pl.col("ts").dt.year().alias("year"),
                        pl.col("ts").dt.month().alias("month"),
                        pl.col("ts").dt.day().alias("day"),
                    ]
                )
            df.write_parquet(str(out_path))
            return

        lf = self._build(sort_rows=sort_rows)
        if streaming and hasattr(lf, "sink_parquet"):
            try:
                lf.sink_parquet(str(out_path))
                return
            except Exception:
                # Some query plans (e.g. global sorting) may not be sinkable in
                # older Polars versions. Fall back to streaming collect.
                pass
        lf.collect(engine="streaming").write_parquet(str(out_path))

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
        return compute_stats_lazy(self._build(sort_rows=True), level=level).collect(engine="streaming")

    def detect_events(
        self,
        turn_deg: float = 30.0,
        stop_sog: float = 0.5,
        stop_min: int = 15,
        draft_jump_m: float = 0.3,
        include_draft_changes: bool = True,
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
            Draught change threshold (meters). Draught changes should be treated
            as low-confidence data-quality/cargo-state indicators because AIS
            draught values may be missing, outdated, or irregularly updated.
        include_draft_changes : bool, default=True
            Whether to report draft_change events. Set to False when draught is
            not reliable for the intended analysis.

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
            include_draft_changes=include_draft_changes,
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
