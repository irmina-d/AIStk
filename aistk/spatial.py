"""
Spatial utilities for AIS data:
- H3 gridding
- geofencing with polygons
- trajectory segmentation
"""
import polars as pl

def grid_features(df: pl.DataFrame, resolution: int = 7):
    """
    Assign AIS points to H3 cells at given resolution and compute aggregates.
    Requires `h3` library.
    """
    try:
        import h3
    except ImportError:
        raise RuntimeError("Install h3: pip install h3")
    
    if not {"LAT","LON"}.issubset(df.columns):
        raise ValueError("LAT/LON required for H3 gridding")

    df = df.with_columns(
        df.apply(lambda row: h3.geo_to_h3(row["LAT"], row["LON"], resolution)).alias("h3")
    )
    grouped = df.group_by("h3").agg([
        pl.count().alias("points"),
        pl.mean("SOG").alias("avg_sog"),
        pl.mean("COG").alias("avg_cog"),
    ])
    return grouped

def geofence(df: pl.DataFrame, polygon):
    """
    Filter AIS points inside a polygon (shapely.geometry.Polygon).
    """
    try:
        from shapely.geometry import Point
    except ImportError:
        raise RuntimeError("Install shapely: pip install shapely")
    
    mask = [polygon.contains(Point(x,y)) for x,y in zip(df["LON"], df["LAT"])]
    return df.filter(mask)
