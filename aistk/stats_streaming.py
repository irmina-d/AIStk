# aistk/stats_streaming.py
from __future__ import annotations

import numpy as np
import polars as pl


def _rad(expr: pl.Expr) -> pl.Expr:
    """
    Convert a Polars expression from degrees to radians.

    Parameters
    ----------
    expr : pl.Expr
        Expression with angle values in degrees.

    Returns
    -------
    pl.Expr
        Expression with angle values in radians.
    """
    return expr * (np.pi / 180.0)


def _haversine_km_expr(lat1: pl.Expr, lon1: pl.Expr, lat2: pl.Expr, lon2: pl.Expr) -> pl.Expr:
    """
    Vectorized haversine great-circle distance (in kilometres) as a Polars expression.

    Parameters
    ----------
    lat1, lon1 : pl.Expr
        Expressions for latitude and longitude of the first point (degrees).
    lat2, lon2 : pl.Expr
        Expressions for latitude and longitude of the second point (degrees).

    Returns
    -------
    pl.Expr
        Expression yielding distance in kilometres.

    Notes
    -----
    - Assumes a spherical Earth with mean radius 6371.0088 km.
    - Includes numerical clipping to keep the input of ``arcsin`` in [0, 1].
    """
    R = 6371.0088
    rlat1, rlon1 = _rad(lat1), _rad(lon1)
    rlat2, rlon2 = _rad(lat2), _rad(lon2)
    dlat = rlat2 - rlat1
    dlon = rlon2 - rlon1
    sin_dlat_sq = (dlat / 2).sin().pow(2)
    sin_dlon_sq = (dlon / 2).sin().pow(2)
    a = sin_dlat_sq + rlat1.cos() * rlat2.cos() * sin_dlon_sq
    a = pl.when(a < 0).then(0).when(a > 1).then(1).otherwise(a)
    return a.sqrt().arcsin() * (2 * R)


def _angle_diff_deg_wrap(cur: pl.Expr, prev: pl.Expr) -> pl.Expr:
    """
    Compute minimal wrapped angular difference between consecutive bearings.

    Parameters
    ----------
    cur : pl.Expr
        Current course/bearing (degrees).
    prev : pl.Expr
        Previous course/bearing (degrees).

    Returns
    -------
    pl.Expr
        Absolute angular difference in degrees, wrapped to [0, 180].

    Notes
    -----
    - Uses the ``atan2`` trick to ensure minimal signed difference.
    - Suitable for course-over-ground (COG) change detection.
    """
    cur_r = _rad(cur)
    prev_r = _rad(prev)
    delta = cur_r - prev_r
    sin_delta = delta.sin().fill_null(0.0)
    cos_delta = delta.cos().fill_null(1.0)
    d = pl.struct([sin_delta.alias("sin"), cos_delta.alias("cos")]).map_elements(
        lambda row: np.arctan2(row["sin"], row["cos"]), return_dtype=pl.Float64
    )
    return d.degrees().abs()


def compute_stats_lazy(lf: pl.LazyFrame, level: str = "mmsi") -> pl.LazyFrame:
    """
    Compute streaming-friendly trajectory statistics on a Polars LazyFrame.

    This avoids materialization into NumPy arrays and instead leverages Polars
    expressions, making it suitable for very large AIS datasets processed with
    ``collect(engine="streaming")``.

    Parameters
    ----------
    lf : pl.LazyFrame
        LazyFrame containing AIS records. Required columns:
        - ``LAT`` : latitude (degrees),
        - ``LON`` : longitude (degrees).
        Optional columns:
        - ``MMSI`` : vessel identifier,
        - ``ts`` : timestamp (datetime),
        - ``SOG`` : speed over ground,
        - ``COG`` : course over ground.
    level : {"mmsi"}, default="mmsi"
        Aggregation level. If ``"mmsi"`` and ``MMSI`` column exists, group by vessel.
        Otherwise compute a single global summary.

    Returns
    -------
    pl.LazyFrame
        LazyFrame with aggregated metrics:
        - ``points`` : number of points,
        - ``distance_km`` : cumulative great-circle distance,
        - ``straight_km`` : straight-line distance between first and last point,
        - ``tortuosity`` : ratio ``distance_km / straight_km``,
        - ``turn_index_deg`` : cumulative wrapped course change (degrees),
        - ``avg_sog`` : mean speed over ground,
        - ``max_sog`` : maximum speed over ground.

    Notes
    -----
    - Use ``.collect(engine=\"streaming\")`` on the returned LazyFrame for large datasets.
    - Distances are computed with haversine approximation on a spherical Earth.
    - Turn index is a simple cumulative heading change proxy.

    Examples
    --------
    >>> import polars as pl
    >>> from aistk.stats_streaming import compute_stats_lazy
    >>> lf = pl.LazyFrame({"MMSI":[1,1], "LAT":[54.3,54.31], "LON":[18.6,18.61], "SOG":[10,12], "COG":[45,50]})
    >>> out = compute_stats_lazy(lf).collect(engine="streaming")
    >>> out.columns
    ['MMSI','points','distance_km','straight_km','tortuosity','turn_index_deg','avg_sog','max_sog']
    """
    schema_names = set(lf.collect_schema().names())
    has_mmsi = "MMSI" in schema_names
    has_ts = "ts" in schema_names

    # Sort for correct lag/lead
    lf = (
        lf.sort(["MMSI", "ts"])
        if has_mmsi and has_ts
        else (lf.sort("ts") if has_ts else lf)
    )

    # Per-row segment distance to next point
    same_group_next = pl.lit(True) if not has_mmsi else (pl.col("MMSI") == pl.col("MMSI").shift(-1))
    seg_km = pl.when(same_group_next).then(
        _haversine_km_expr(pl.col("LAT"), pl.col("LON"), pl.col("LAT").shift(-1), pl.col("LON").shift(-1))
    ).otherwise(0.0).alias("seg_km")

    # Straight-line distance (first→last)
    first_lat, first_lon = pl.col("LAT").first(), pl.col("LON").first()
    last_lat, last_lon = pl.col("LAT").last(), pl.col("LON").last()
    straight_km = _haversine_km_expr(first_lat, first_lon, last_lat, last_lon).alias("straight_km")

    # Turn index
    turn_deg = _angle_diff_deg_wrap(pl.col("COG"), pl.col("COG").shift(1)).fill_null(0.0).alias("turn_deg")

    base = lf.with_columns([seg_km, turn_deg])

    group_keys = ["MMSI"] if level == "mmsi" and has_mmsi else []
    agg = base.group_by(group_keys) if group_keys else base.group_by([])

    result = agg.agg(
        [
            pl.len().alias("points"),
            pl.sum("seg_km").alias("distance_km"),
            straight_km,
            pl.sum("turn_deg").alias("turn_index_deg"),
            pl.mean("SOG").alias("avg_sog"),
            pl.max("SOG").alias("max_sog"),
        ]
    )

    result = result.with_columns(
        (
            pl.col("distance_km")
            / pl.when(pl.col("straight_km") <= 1e-6)
            .then(1e-6)
            .otherwise(pl.col("straight_km"))
        ).alias("tortuosity")
    )

    # Rounding for consistency
    result = result.with_columns(
        [
            pl.col("distance_km").round(3),
            pl.col("straight_km").round(3),
            pl.col("tortuosity").round(3),
            pl.col("turn_index_deg").round(1),
        ]
    )

    return result
