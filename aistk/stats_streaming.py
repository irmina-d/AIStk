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


def _optional_numeric_expr(column: str, fallback: float = 0.0) -> pl.Expr:
    """Return a numeric expression if ``column`` exists later, otherwise a literal."""
    return pl.col(column).cast(pl.Float64, strict=False) if column else pl.lit(fallback)


def compute_stats_lazy(lf: pl.LazyFrame, level: str = "mmsi") -> pl.LazyFrame:
    """
    Compute streaming-friendly trajectory statistics on a Polars LazyFrame.

    The implementation keeps all consecutive-point operations vessel-aware. When
    ``MMSI`` is available, segment distances and course changes are calculated
    only within the same MMSI, preventing cross-vessel artefacts in large AIS
    files. Use ``.collect(engine="streaming")`` on the returned LazyFrame.
    """
    schema_names = set(lf.collect_schema().names())
    required = {"LAT", "LON"}
    if not required.issubset(schema_names):
        return pl.LazyFrame()

    has_mmsi = "MMSI" in schema_names
    has_ts = "ts" in schema_names
    has_cog = "COG" in schema_names
    has_sog = "SOG" in schema_names

    # Sort for correct lag/lead. The following group guards prevent the last row
    # of one vessel from being connected to the first row of another vessel.
    lf = lf.sort(["MMSI", "ts"]) if has_mmsi and has_ts else (lf.sort("ts") if has_ts else lf)

    if has_mmsi:
        same_group_next = pl.col("MMSI") == pl.col("MMSI").shift(-1)
        same_group_prev = pl.col("MMSI") == pl.col("MMSI").shift(1)
    else:
        same_group_next = pl.lit(True)
        same_group_prev = pl.lit(True)

    seg_km = (
        pl.when(same_group_next)
        .then(_haversine_km_expr(pl.col("LAT"), pl.col("LON"), pl.col("LAT").shift(-1), pl.col("LON").shift(-1)))
        .otherwise(0.0)
        .fill_null(0.0)
        .alias("seg_km")
    )

    if has_cog:
        turn_deg = (
            pl.when(same_group_prev)
            .then(_angle_diff_deg_wrap(pl.col("COG"), pl.col("COG").shift(1)))
            .otherwise(0.0)
            .fill_null(0.0)
            .alias("turn_deg")
        )
    else:
        turn_deg = pl.lit(None, dtype=pl.Float64).alias("turn_deg")

    base = lf.with_columns([seg_km, turn_deg])

    group_keys = ["MMSI"] if level == "mmsi" and has_mmsi else []
    agg = base.group_by(group_keys) if group_keys else base.group_by([])

    first_lat, first_lon = pl.col("LAT").first(), pl.col("LON").first()
    last_lat, last_lon = pl.col("LAT").last(), pl.col("LON").last()
    straight_km = _haversine_km_expr(first_lat, first_lon, last_lat, last_lon).alias("straight_km")

    aggregations = [
        pl.len().alias("points"),
        pl.sum("seg_km").alias("distance_km"),
        straight_km,
        pl.sum("turn_deg").alias("turn_index_deg"),
    ]
    if has_sog:
        aggregations.extend([
            pl.mean("SOG").alias("avg_sog"),
            pl.max("SOG").alias("max_sog"),
        ])
    else:
        aggregations.extend([
            pl.lit(None, dtype=pl.Float64).alias("avg_sog"),
            pl.lit(None, dtype=pl.Float64).alias("max_sog"),
        ])

    result = agg.agg(aggregations)

    result = result.with_columns(
        (
            pl.col("distance_km")
            / pl.when(pl.col("straight_km") <= 1e-6)
            .then(1e-6)
            .otherwise(pl.col("straight_km"))
        ).alias("tortuosity")
    )

    result = result.with_columns(
        [
            pl.col("distance_km").round(3),
            pl.col("straight_km").round(3),
            pl.col("tortuosity").round(3),
            pl.col("turn_index_deg").round(1),
        ]
    )

    return result
