from __future__ import annotations

from typing import Dict, Literal, Optional, Union

import numpy as np
import polars as pl
from numpy.typing import NDArray

from .utils import haversine_km

AggLevel = Literal["mmsi"]  # extend later if you add "trip", "day", etc.


def compute_stats_df(df: pl.DataFrame, level: AggLevel = "mmsi") -> pl.DataFrame:
    """
    Compute basic trajectory statistics from an AIS frame (Polars).

    The function derives segment-wise great-circle distances (haversine) and aggregates
    them into summary metrics such as total distance, straight-line distance,
    tortuosity (total/straight), a simple turn index from COG unwrapping, and basic
    SOG summaries (max/avg). It supports:
      - per-MMSI aggregation (``level="mmsi"``) when column ``"MMSI"`` is present,
      - single-frame aggregation otherwise (returns one-row DataFrame).

    Parameters
    ----------
    df : pl.DataFrame
        Polars DataFrame with AIS points. Expected columns:
        - Required for geometry: ``"LAT"``, ``"LON"`` (degrees).
        - Optional kinematics: ``"SOG"`` (knots), ``"COG"`` (degrees).
        - Optional identifiers/time: ``"MMSI"``, ``"ts"`` (timestamp sortable).
    level : {"mmsi"}
        Aggregation level. If ``"mmsi"`` and the column ``"MMSI"`` exists,
        compute one row per MMSI; otherwise compute a single global row.

    Returns
    -------
    pl.DataFrame
        If ``level="mmsi"`` and ``"MMSI"`` exists → one row per MMSI with:
        ``["MMSI","points","distance_km","straight_km","tortuosity","turn_index_deg","max_sog","avg_sog"]``.
        Otherwise → a single-row DataFrame without ``"MMSI"``.

    Notes
    -----
    - If ``"LAT"`` or ``"LON"`` is missing, returns an **empty** DataFrame.
    - Distance uses a spherical Earth with mean radius (see ``utils.haversine_km``).
    - ``turn_index_deg`` is a simple cumulative absolute heading change (degrees)
      computed from unwrapped ``COG`` (NaNs forward-filled before unwrap).
    - ``tortuosity`` is ``total_distance / straight_line_distance``; when the
      straight distance is ~0, we divide by a small epsilon (``1e-6``) to avoid
      division by zero.

    Examples
    --------
    >>> import polars as pl
    >>> df = pl.DataFrame({"MMSI":[1,1,1], "LAT":[54.3,54.301,54.302], "LON":[18.6,18.601,18.602], "SOG":[10,11,10]})
    >>> out = compute_stats_df(df, level="mmsi")
    >>> out.columns
    ['MMSI', 'points', 'distance_km', 'straight_km', 'tortuosity', 'turn_index_deg', 'max_sog', 'avg_sog']
    """
    required = {"LAT", "LON"}
    if not required.issubset(set(df.columns)):
        return pl.DataFrame()

    def _per(pdf: pl.DataFrame) -> Dict[str, Union[int, float, None]]:
        lat: NDArray[np.float64] = pdf["LAT"].to_numpy()
        lon: NDArray[np.float64] = pdf["LON"].to_numpy()

        max_sog: Optional[float] = None
        avg_sog: Optional[float] = None
        if "SOG" in pdf.columns:
            sog_col = pdf["SOG"]
            max_val = sog_col.max()
            mean_val = sog_col.mean()
            max_sog = float(max_val) if max_val is not None else None
            avg_sog = float(mean_val) if mean_val is not None else None

        if lat.size < 2:
            return {
                "points": int(lat.size),
                "distance_km": 0.0,
                "straight_km": 0.0,
                "tortuosity": 1.0,
                "turn_index_deg": None,
                "max_sog": max_sog,
                "avg_sog": avg_sog,
            }

        # Segment distances and straight-line distance
        dist = haversine_km(lat[:-1], lon[:-1], lat[1:], lon[1:])
        total_km = float(np.nansum(dist))
        straight_km = float(haversine_km(lat[0], lon[0], lat[-1], lon[-1]))
        tort = total_km / max(straight_km, 1e-6)

        # Simple cumulative turn index from unwrapped COG (degrees)
        turn_index: Optional[float] = None
        if "COG" in pdf.columns:
            cog = pdf["COG"].fill_null(strategy="forward").to_numpy()
            cog = np.radians(cog)
            d = np.degrees(np.abs(np.diff(np.unwrap(cog))))
            turn_index = float(np.nansum(d))

        return {
            "points": int(lat.size),
            "distance_km": round(total_km, 3),
            "straight_km": round(straight_km, 3),
            "tortuosity": round(tort, 3),
            "turn_index_deg": round(turn_index, 1) if turn_index is not None else None,
            "max_sog": max_sog,
            "avg_sog": avg_sog,
        }

    # Sort if timestamp is present; keep per-vessel order if MMSI exists
    if "ts" in df.columns:
        df = df.sort(["MMSI", "ts"]) if "MMSI" in df.columns else df.sort("ts")

    if level == "mmsi" and "MMSI" in df.columns:
        rows: list[Dict[str, Union[int, float, None]]] = []
        # Polars GroupBy is iterable: (key, subframe)
        for _key, pdf in df.group_by("MMSI", maintain_order=True):
            stats = _per(pdf)
            first_mmsi = pdf["MMSI"][0]
            if first_mmsi is None or (
                isinstance(first_mmsi, (float, np.floating)) and np.isnan(first_mmsi)
            ):
                stats["MMSI"] = None
            else:
                stats["MMSI"] = int(first_mmsi)
            rows.append(stats)
        return pl.DataFrame(rows)

    # Fallback: compute a single global row
    return pl.DataFrame([_per(df)])
