
from __future__ import annotations
import os, glob
import polars as pl

from .utils import haversine_km
from .stats import compute_stats_df
from .events import detect_events_df
from .viz import plot_track_html

def _scan_many(path: str, pattern: str) -> pl.LazyFrame:
    files = sorted(glob.glob(os.path.join(path, pattern)))
    if not files:
        raise FileNotFoundError(f"No files match: {path}/{pattern}")
    scans = [pl.scan_csv(f, has_header=True, infer_schema_length=0, ignore_errors=True, try_parse_dates=True)
             for f in files]
    return pl.concat(scans, rechunk=False)

def _valid_geo():
    return (pl.col("LAT").is_between(-90, 90)) & (pl.col("LON").is_between(-180, 180))

def _ts_expr():
    return pl.coalesce([
        pl.col("BaseDateTime").str.strptime(pl.Datetime, strict=False),
        pl.col("BaseDateTime").str.to_datetime(strict=False)
    ]).alias("ts")

class AISDataset:
    """High-level lazy dataset wrapper for decoded AIS CSV files."""
    def __init__(self, root: str, pattern: str = "*.csv"):
        self.root = root
        self.pattern = pattern
        self._lf = _scan_many(root, pattern)
        self._filters = []
        self._selected = None
        self._need_ts = False

    # configuration
    def with_columns(self, cols: list[str]) -> "AISDataset":
        self._selected = cols
        return self

    def between(self, start: str, end: str) -> "AISDataset":
        self._need_ts = True
        self._filters.append((_ts_expr() >= pl.lit(start)) & (_ts_expr() < pl.lit(end)))
        return self

    def filter(self, mmsi=None, imo=None, callsign=None) -> "AISDataset":
        if mmsi is not None:
            if isinstance(mmsi, (list, tuple, set)):
                self._filters.append(pl.col("MMSI").is_in(list(mmsi)))
            else:
                self._filters.append(pl.col("MMSI") == mmsi)
        if imo is not None:
            if isinstance(imo, (list, tuple, set)):
                self._filters.append(pl.col("IMO").is_in(list(imo)))
            else:
                self._filters.append(pl.col("IMO") == imo)
        if callsign is not None:
            if isinstance(callsign, (list, tuple, set)):
                self._filters.append(pl.col("CallSign").is_in(list(callsign)))
            else:
                self._filters.append(pl.col("CallSign") == callsign)
        return self

    # build lazyframe
    def _build(self) -> pl.LazyFrame:
        lf = self._lf
        if self._selected:
            lf = lf.select([pl.col(c) for c in self._selected if isinstance(c, str)])
        if self._need_ts or any("BaseDateTime" in str(f) for f in self._filters):
            lf = lf.with_columns(_ts_expr())
        if self._filters:
            cond = self._filters[0]
            for f in self._filters[1:]:
                cond = cond & f
            lf = lf.filter(cond)
        if "LAT" in lf.columns and "LON" in lf.columns:
            lf = lf.filter(_valid_geo())
        if "ts" in lf.columns and "MMSI" in lf.columns:
            lf = lf.sort(["MMSI", "ts"])
        elif "ts" in lf.columns:
            lf = lf.sort("ts")
        return lf

    # materialization / IO
    def collect(self) -> pl.DataFrame:
        return self._build().collect(streaming=True)

    def write_parquet(self, path: str, partition: str | None = None) -> None:
        df = self.collect()
        if partition and "ts" in df.columns:
            if partition.lower() in {"year", "year/month", "year/month/day"}:
                df = df.with_columns([
                    pl.col("ts").dt.year().alias("year"),
                    pl.col("ts").dt.month().alias("month"),
                    pl.col("ts").dt.day().alias("day"),
                ])
            df.write_parquet(path)
        else:
            df.write_parquet(path)

    # analytics
    def stats(self, level: str = "mmsi"):
        df = self.collect()
        return compute_stats_df(df, level=level)

    def detect_events(self, turn_deg: float = 30.0, stop_sog: float = 0.5, stop_min: int = 15,
                      draft_jump_m: float = 0.3):
        df = self.collect()
        return detect_events_df(df, turn_deg=turn_deg, stop_sog=stop_sog, stop_min=stop_min,
                                draft_jump_m=draft_jump_m)

    # viz
    def plot_map(self, out_html: str, mmsi: int | None = None) -> str:
        df = self.collect()
        if mmsi is not None and "MMSI" in df.columns:
            df = df.filter(pl.col("MMSI") == mmsi)
        return plot_track_html(df, out_html)
