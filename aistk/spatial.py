from __future__ import annotations

from typing import Any, Union

import polars as pl

# Optional types for clarity
try:
    from shapely.geometry import Polygon
except ImportError:
    Polygon = Any  # fallback if shapely not installed


def grid_features(df: pl.DataFrame, resolution: int = 7) -> pl.DataFrame:
    """
    Assign AIS points to H3 cells at a given resolution and compute aggregates.

    This function attaches an ``"h3"`` index column (hexagon ID) and
    aggregates per cell: point count, mean speed over ground, and mean course.

    Parameters
    ----------
    df : pl.DataFrame
        Polars DataFrame with AIS records. Must contain:
        - ``"LAT"`` : latitude in degrees,
        - ``"LON"`` : longitude in degrees,
        - optionally ``"SOG"``, ``"COG"`` for averaging.
    resolution : int, default=7
        H3 resolution level (0 = coarsest, ~global; 15 = finest, ~1 m).

    Returns
    -------
    pl.DataFrame
        DataFrame with one row per H3 cell. Columns include:
        - ``"h3"`` : H3 index (str),
        - ``"points"`` : count of AIS records in cell,
        - ``"avg_sog"`` : mean speed over ground,
        - ``"avg_cog"`` : mean course over ground.

    Raises
    ------
    RuntimeError
        If the ``h3`` library is not installed.
    ValueError
        If required columns ``LAT``/``LON`` are missing.

    Notes
    -----
    - Requires `h3` Python bindings (`pip install h3`).
    - Aggregates are simple averages; for weighted stats extend accordingly.

    Examples
    --------
    >>> import polars as pl
    >>> df = pl.DataFrame({"LAT":[54.3,54.31], "LON":[18.6,18.61], "SOG":[10,12], "COG":[45,50]})
    >>> out = grid_features(df, resolution=7)
    >>> out.columns
    ['h3','points','avg_sog','avg_cog']
    """
    try:
        import h3
    except ImportError:
        raise RuntimeError("Install h3: pip install h3")

    if hasattr(h3, "geo_to_h3"):
        to_cell = lambda lat, lon, res: h3.geo_to_h3(lat, lon, res)
    elif hasattr(h3, "latlng_to_cell"):
        to_cell = lambda lat, lon, res: h3.latlng_to_cell(lat, lon, res)
    else:
        raise RuntimeError("Unsupported h3 API; upgrade the h3 package")

    if not {"LAT", "LON"}.issubset(df.columns):
        raise ValueError("LAT/LON required for H3 gridding")

    df = df.with_columns(
        pl.struct(["LAT", "LON"]).map_elements(
            lambda row: to_cell(row["LAT"], row["LON"], resolution),
            return_dtype=pl.Utf8,
        ).alias("h3")
    )

    aggregations: list[pl.Expr] = [pl.len().alias("points")]
    if "SOG" in df.columns:
        aggregations.append(pl.mean("SOG").alias("avg_sog"))
    if "COG" in df.columns:
        aggregations.append(pl.mean("COG").alias("avg_cog"))

    grouped = df.group_by("h3").agg(aggregations)
    return grouped


def geofence(df: pl.DataFrame, polygon: Polygon) -> pl.DataFrame:
    """
    Filter AIS points to those lying inside a polygon.

    Parameters
    ----------
    df : pl.DataFrame
        Polars DataFrame with AIS records. Must contain:
        - ``"LAT"`` : latitude in degrees,
        - ``"LON"`` : longitude in degrees.
    polygon : shapely.geometry.Polygon
        Polygon in geographic coordinates (same CRS as LAT/LON).

    Returns
    -------
    pl.DataFrame
        Subset of `df` containing only points inside `polygon`.

    Raises
    ------
    RuntimeError
        If the `shapely` library is not installed.

    Notes
    -----
    - Uses `shapely.geometry.Point.contains`.
    - Not vectorized; for large datasets consider `geopandas.sjoin`.

    Examples
    --------
    >>> from shapely.geometry import Polygon
    >>> poly = Polygon([(18.5,54.2),(18.7,54.2),(18.7,54.4),(18.5,54.4)])
    >>> out = geofence(df, poly)
    """
    try:
        from shapely.geometry import Point
    except ImportError:
        raise RuntimeError("Install shapely: pip install shapely")

    mask = [polygon.contains(Point(x, y)) for x, y in zip(df["LON"], df["LAT"])]
    return df.filter(mask)
