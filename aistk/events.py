from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Union

import numpy as np
import polars as pl

_EVENT_COLUMNS: dict[str, pl.DataType] = {
    "type": pl.Utf8,
    "MMSI": pl.Int64,
    "ts": pl.Datetime,
    "delta_deg": pl.Float64,
    "duration_min": pl.Float64,
    "delta_m": pl.Float64,
    "gap_s": pl.Int64,
}


def _empty_events_frame() -> pl.DataFrame:
    """Return an empty event table with a stable schema."""
    return pl.DataFrame({name: pl.Series(name, [], dtype=dtype) for name, dtype in _EVENT_COLUMNS.items()})


def _normalise_event_rows(rows: Iterable[Dict[str, Any]]) -> pl.DataFrame:
    """Create a stable-schema Polars frame from event dictionaries.

    Polars infers column dtypes when a list of dictionaries is passed directly
    to ``pl.DataFrame``. For heterogeneous event rows this can fail on large AIS
    dumps, e.g. when early ``delta_m`` values are null/integer-like and a later
    draught jump is a floating-point value such as ``4.8``. Build each column as
    an explicitly typed series instead, so mixed event types always produce the
    same output schema.
    """
    rows = list(rows)
    if not rows:
        return _empty_events_frame()

    columns: dict[str, pl.Series] = {}
    for name, dtype in _EVENT_COLUMNS.items():
        values = [row.get(name) for row in rows]
        columns[name] = pl.Series(name, values, dtype=dtype, strict=False)
    return pl.DataFrame(columns).select(list(_EVENT_COLUMNS.keys()))


def _to_datetime_frame(df: pl.DataFrame) -> pl.DataFrame:
    """Ensure a datetime ``ts`` column exists when possible."""
    if "ts" in df.columns:
        if df.schema.get("ts") == pl.Utf8:
            return df.with_columns(pl.col("ts").str.to_datetime(strict=False).alias("ts"))
        return df.with_columns(pl.col("ts").cast(pl.Datetime, strict=False).alias("ts"))

    if "BaseDateTime" in df.columns:
        if df.schema.get("BaseDateTime") == pl.Utf8:
            ts_expr = pl.coalesce(
                [
                    pl.col("BaseDateTime").str.strptime(pl.Datetime, strict=False),
                    pl.col("BaseDateTime").str.to_datetime(strict=False),
                ]
            )
        else:
            ts_expr = pl.col("BaseDateTime").cast(pl.Datetime, strict=False)
        return df.with_columns(ts_expr.alias("ts"))

    return df


def _series_to_float_numpy(series: pl.Series) -> np.ndarray:
    """Convert a Polars series to a float NumPy array with nulls as NaN."""
    return series.cast(pl.Float64, strict=False).to_numpy()


def _detect_events_single_vessel(
    df: pl.DataFrame,
    *,
    vessel_id: Optional[int],
    turn_deg: float,
    stop_sog: float,
    stop_min: int,
    draft_jump_m: float,
    gap_s: int,
) -> list[Dict[str, Any]]:
    """Detect events within one already-sorted vessel trajectory."""
    events: list[Dict[str, Any]] = []
    if df.height == 0 or "ts" not in df.columns:
        return events

    df = df.drop_nulls("ts").sort("ts")
    if df.height == 0:
        return events

    base: Dict[str, Any] = {"MMSI": vessel_id}

    # Sharp turns: minimal wrapped COG difference in degrees, computed only within this vessel.
    if "COG" in df.columns and df.height >= 2:
        cog = _series_to_float_numpy(df["COG"].fill_null(strategy="forward"))
        valid = np.isfinite(cog)
        if valid.sum() >= 2:
            cog_rad = np.radians(cog)
            # atan2 wrapping avoids false 359->1 degree jumps.
            delta = np.abs(np.degrees(np.arctan2(np.sin(np.diff(cog_rad)), np.cos(np.diff(cog_rad)))))
            for i, val in enumerate(delta):
                if np.isfinite(val) and val >= turn_deg:
                    events.append({**base, "type": "sharp_turn", "ts": df["ts"][i + 1], "delta_deg": round(float(val), 3)})

    # Prolonged stops: consecutive points below threshold in this vessel only.
    if "SOG" in df.columns:
        sog = _series_to_float_numpy(df["SOG"].fill_null(strategy="forward"))
        ts = df["ts"].to_numpy().astype("datetime64[ns]")
        mask = np.isfinite(sog) & (sog < stop_sog)
        if mask.any():
            idx = np.where(mask)[0]
            for group in np.split(idx, np.where(np.diff(idx) != 1)[0] + 1):
                if group.size > 1:
                    start_idx = int(group[0])
                    end_idx = int(group[-1])
                    duration_ms = (ts[end_idx] - ts[start_idx]) / np.timedelta64(1, "ms")
                    if duration_ms >= stop_min * 60_000:
                        events.append(
                            {
                                **base,
                                "type": "stop",
                                "ts": df["ts"][end_idx],
                                "duration_min": round(float(duration_ms) / 60_000.0, 2),
                            }
                        )

    # Draught changes: consecutive differences in this vessel only.
    if "Draft" in df.columns and df.height >= 2:
        draft = _series_to_float_numpy(df["Draft"].fill_null(strategy="forward"))
        diff = np.abs(np.diff(draft))
        for i, val in enumerate(diff):
            if np.isfinite(val) and val >= draft_jump_m:
                events.append({**base, "type": "draft_change", "ts": df["ts"][i + 1], "delta_m": round(float(val), 3)})

    # AIS signal gaps: timestamp differences in this vessel only.
    if df.height >= 2:
        gaps = df["ts"].diff().dt.total_seconds()
        for i, seconds in enumerate(gaps):
            if seconds is not None and seconds > gap_s:
                events.append({**base, "type": "gap", "ts": df["ts"][i], "gap_s": int(seconds)})

    return events


def detect_events_df(
    df: pl.DataFrame,
    turn_deg: float = 30.0,
    stop_sog: float = 0.5,
    stop_min: int = 15,
    draft_jump_m: float = 0.3,
    gap_s: int = 600,
    group_col: str = "MMSI",
) -> pl.DataFrame:
    """
    Detect navigational AIS events from a Polars DataFrame.

    Event classes implemented:
      - ``sharp_turn``: wrapped course-over-ground change above ``turn_deg``;
      - ``stop``: speed over ground below ``stop_sog`` sustained for at least
        ``stop_min`` minutes;
      - ``draft_change``: absolute draught change above ``draft_jump_m``;
      - ``gap``: temporal gap between consecutive timestamps above ``gap_s``.

    The detector is vessel-aware. When ``MMSI`` is present, all consecutive
    differences are computed independently per vessel. This prevents false events
    caused by comparing the last record of one vessel with the first record of
    another vessel, which is critical for large multi-vessel AIS dumps.

    Parameters
    ----------
    df : polars.DataFrame
        AIS records. Expected columns are ``ts`` or ``BaseDateTime`` and optional
        ``MMSI``, ``COG``, ``SOG`` and ``Draft``.
    turn_deg : float, default=30.0
        Minimum wrapped COG change in degrees for ``sharp_turn``.
    stop_sog : float, default=0.5
        SOG threshold in knots for stop detection.
    stop_min : int, default=15
        Minimum stop duration in minutes.
    draft_jump_m : float, default=0.3
        Draught-change threshold in metres.
    gap_s : int, default=600
        AIS signal gap threshold in seconds.
    group_col : str, default="MMSI"
        Vessel identifier column used for independent trajectory processing.

    Returns
    -------
    polars.DataFrame
        Stable-schema event table with columns ``type``, ``MMSI``, ``ts``,
        ``delta_deg``, ``duration_min``, ``delta_m`` and ``gap_s``.
    """
    if df.height == 0:
        return _empty_events_frame()

    df = _to_datetime_frame(df)
    if "ts" not in df.columns:
        return _empty_events_frame()

    cast_exprs: list[pl.Expr] = []
    for col in ("COG", "SOG", "Draft"):
        if col in df.columns:
            cast_exprs.append(pl.col(col).cast(pl.Float64, strict=False).alias(col))
    if group_col in df.columns:
        cast_exprs.append(pl.col(group_col).cast(pl.Int64, strict=False).alias(group_col))
    if cast_exprs:
        df = df.with_columns(cast_exprs)

    rows: list[Dict[str, Any]] = []
    if group_col in df.columns:
        df = df.drop_nulls(group_col).sort([group_col, "ts"])
        for key, vessel_df in df.group_by(group_col, maintain_order=True):
            # Polars returns tuple keys for grouped iteration in recent versions.
            vessel_id_raw = key[0] if isinstance(key, tuple) else key
            vessel_id = int(vessel_id_raw) if vessel_id_raw is not None else None
            rows.extend(
                _detect_events_single_vessel(
                    vessel_df,
                    vessel_id=vessel_id,
                    turn_deg=turn_deg,
                    stop_sog=stop_sog,
                    stop_min=stop_min,
                    draft_jump_m=draft_jump_m,
                    gap_s=gap_s,
                )
            )
    else:
        rows.extend(
            _detect_events_single_vessel(
                df.sort("ts"),
                vessel_id=None,
                turn_deg=turn_deg,
                stop_sog=stop_sog,
                stop_min=stop_min,
                draft_jump_m=draft_jump_m,
                gap_s=gap_s,
            )
        )

    out = _normalise_event_rows(rows)
    if out.height > 0:
        out = out.sort([c for c in ["MMSI", "ts", "type"] if c in out.columns])
    return out
