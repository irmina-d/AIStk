from __future__ import annotations

from typing import Literal, Optional

import dask.dataframe as dd
import numpy as np
import pandas as pd

AggLevel = Literal["mmsi"]
EARTH_RADIUS_KM: float = 6371.0088


def _haversine_km_np(
    lat1: np.ndarray | float,
    lon1: np.ndarray | float,
    lat2: np.ndarray | float,
    lon2: np.ndarray | float,
) -> np.ndarray | float:
    """
    Vectorized haversine great-circle distance in kilometres (NumPy).

    Parameters
    ----------
    lat1, lon1 : array-like or float
        Latitude/longitude of the first point(s) in degrees.
    lat2, lon2 : array-like or float
        Latitude/longitude of the second point(s) in degrees.

    Returns
    -------
    numpy.ndarray or float
        Distance(s) in kilometres. Supports NumPy broadcasting.

    Notes
    -----
    - Uses a spherical Earth with mean radius ``EARTH_RADIUS_KM = 6371.0088``.
    - ``np.clip`` is applied to maintain the ``arcsin`` argument within ``[0, 1]``.
    """
    rlat1 = np.radians(lat1)
    rlon1 = np.radians(lon1)
    rlat2 = np.radians(lat2)
    rlon2 = np.radians(lon2)
    dlat = rlat2 - rlat1
    dlon = rlon2 - rlon1
    a = np.sin(dlat / 2.0) ** 2 + np.cos(rlat1) * np.cos(rlat2) * (np.sin(dlon / 2.0) ** 2)
    a = np.clip(a, 0.0, 1.0)
    return 2.0 * EARTH_RADIUS_KM * np.arcsin(np.sqrt(a))


def _per_mmsi(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Compute per-vessel trajectory statistics on a single Pandas partition.

    Parameters
    ----------
    pdf : pandas.DataFrame
        Partition with one vessel (single MMSI) or mixed MMSIs (will use first).

    Returns
    -------
    pandas.DataFrame
        One-row DataFrame with columns:
        ``["MMSI","points","distance_km","straight_km","tortuosity","turn_index_deg","avg_sog","max_sog"]``.
    """
    if len(pdf) == 0:
        return pd.DataFrame(
            [
                {
                    "MMSI": np.nan,
                    "points": 0,
                    "distance_km": 0.0,
                    "straight_km": 0.0,
                    "tortuosity": 1.0,
                    "turn_index_deg": np.nan,
                    "avg_sog": np.nan,
                    "max_sog": np.nan,
                }
            ]
        )

    # Ensure sort for correct segment/turn computation if ts present
    if "ts" in pdf.columns:
        pdf = pdf.sort_values("ts", kind="mergesort")

    mmsi_val = np.nan
    if "MMSI" in pdf.columns and len(pdf["MMSI"]):
        first_mmsi = pdf["MMSI"].iloc[0]
        if not pd.isna(first_mmsi):
            mmsi_val = int(first_mmsi)
    points = int(len(pdf))

    if "LAT" not in pdf.columns or "LON" not in pdf.columns or points < 2:
        return pd.DataFrame(
            [
                {
                    "MMSI": mmsi_val,
                    "points": points,
                    "distance_km": 0.0,
                    "straight_km": 0.0,
                    "tortuosity": 1.0,
                    "turn_index_deg": np.nan,
                    "avg_sog": float(pdf["SOG"].mean()) if "SOG" in pdf.columns else np.nan,
                    "max_sog": float(pdf["SOG"].max()) if "SOG" in pdf.columns else np.nan,
                }
            ]
        )

    lat = pdf["LAT"].to_numpy()
    lon = pdf["LON"].to_numpy()
    seg = _haversine_km_np(lat[:-1], lon[:-1], lat[1:], lon[1:])
    total = float(np.nansum(seg))
    straight = float(_haversine_km_np(lat[0], lon[0], lat[-1], lon[-1]))
    tort = total / max(straight, 1e-6)

    turn = np.nan
    if "COG" in pdf.columns:
        cog = pdf["COG"].ffill().to_numpy()
        cog = np.unwrap(np.radians(cog))
        turn = float(np.sum(np.degrees(np.abs(np.diff(cog)))))

    row = {
        "MMSI": mmsi_val,
        "points": points,
        "distance_km": round(total, 3),
        "straight_km": round(straight, 3),
        "tortuosity": round(tort, 3),
        "turn_index_deg": np.round(turn, 1) if not (isinstance(turn, float) and np.isnan(turn)) else np.nan,
        "avg_sog": float(pdf["SOG"].mean()) if "SOG" in pdf.columns else np.nan,
        "max_sog": float(pdf["SOG"].max()) if "SOG" in pdf.columns else np.nan,
    }
    return pd.DataFrame([row])


def compute_stats_dask(
    ddf: dd.DataFrame,
    level: AggLevel = "mmsi",
    assume_sorted: bool = False,
) -> pd.DataFrame:
    """
    Compute trajectory statistics on **Dask DataFrame** (out-of-core / distributed).

    The function groups by MMSI (if requested) and applies a Pandas partition
    function to compute total great-circle distance, straight-line distance,
    tortuosity, a simple turn index, and SOG summaries. Designed for datasets
    that do not fit in RAM (>10 GB).

    Parameters
    ----------
    ddf : dask.dataframe.DataFrame
        Input AIS frame. Expected columns:
        - required: ``LAT``, ``LON``
        - optional: ``MMSI``, ``ts`` (timestamp), ``SOG``, ``COG``.
    level : {"mmsi"}, default="mmsi"
        Aggregation level. Currently only per-MMSI aggregation is supported.
    assume_sorted : bool, default=False
        If ``True``, the data is assumed already sorted by ``ts`` within each MMSI.
        If ``False`` and ``ts`` exists, a sort within partitions is attempted.

    Returns
    -------
    pandas.DataFrame
        Aggregated metrics. For ``level="mmsi"`` one row per MMSI with:
        ``["MMSI","points","distance_km","straight_km","tortuosity","turn_index_deg","avg_sog","max_sog"]``.

    Notes
    -----
    - For best performance, read data with block partitioning:
      ``dd.read_parquet("...")`` or ``dd.read_csv("...", blocksize="256MB")``.
    - Sorting across partitions is expensive; prefer ingested data partitioned
      by MMSI/time or use `set_index("ts")` pre-processing if feasible.
    - Turn index is computed from unwrapped COG differences (degrees).

    Examples
    --------
    >>> import dask.dataframe as dd
    >>> from aistk.backends.dask_backend import compute_stats_dask
    >>> ddf = dd.read_parquet("lake/clean/2024/*.parquet")
    >>> out = compute_stats_dask(ddf, level="mmsi")
    >>> out.head()
    """
    # Basic requirements
    if "LAT" not in ddf.columns or "LON" not in ddf.columns:
        return pd.DataFrame()

    if level != "mmsi":
        raise ValueError("Only level='mmsi' is currently supported by the Dask backend.")

    # Optional stabilizing sort by ts (within partitions) to ensure correct deltas
    if "ts" in ddf.columns and not assume_sorted:
        # Sort within partitions (does not guarantee global order, but reduces errors for diffs)
        ddf = ddf.map_partitions(lambda pdf: pdf.sort_values("ts", kind="mergesort"))

    if "MMSI" not in ddf.columns:
        # No MMSI: compute a single global summary by using a single partition
        pdf = ddf.compute()
        return _per_mmsi(pdf)

    meta = pd.DataFrame(
        {
            "MMSI": pd.Series(dtype="int64"),
            "points": pd.Series(dtype="int64"),
            "distance_km": pd.Series(dtype="float64"),
            "straight_km": pd.Series(dtype="float64"),
            "tortuosity": pd.Series(dtype="float64"),
            "turn_index_deg": pd.Series(dtype="float64"),
            "avg_sog": pd.Series(dtype="float64"),
            "max_sog": pd.Series(dtype="float64"),
        }
    )

    # Apply per MMSI using Pandas function; Dask will handle partitioning/shuffle.
    cols = ddf.columns.tolist()
    grouped = ddf.groupby("MMSI", dropna=False)[cols].apply(_per_mmsi, meta=meta)

    # Compute the final pandas DataFrame
    return grouped.compute()
