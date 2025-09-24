from __future__ import annotations

from typing import Literal, Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.column import Column
from pyspark.sql.types import DoubleType

AggLevel = Literal["mmsi"]
EARTH_RADIUS_KM: float = 6371.0088


@F.udf(DoubleType())
def haversine_km_udf(
    lat1: Optional[float],
    lon1: Optional[float],
    lat2: Optional[float],
    lon2: Optional[float],
) -> Optional[float]:
    """
    Great-circle distance using the haversine formula (kilometres).

    Parameters
    ----------
    lat1, lon1 : float or None
        Latitude/longitude of the first point in degrees.
    lat2, lon2 : float or None
        Latitude/longitude of the second point in degrees.

    Returns
    -------
    float or None
        Distance in kilometres, or ``None`` if any input is null.

    Notes
    -----
    - Uses spherical Earth with mean radius ``EARTH_RADIUS_KM``.
    - The output is safe-clipped numerically for ``asin`` domain.
    """
    import math

    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
        return None

    rlat1, rlon1 = math.radians(lat1), math.radians(lon1)
    rlat2, rlon2 = math.radians(lat2), math.radians(lon2)
    dlat, dlon = rlat2 - rlat1, rlon2 - rlon1
    a = math.sin(dlat / 2.0) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2.0) ** 2
    a = min(1.0, max(0.0, a))  # numerical safety
    return 2.0 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))


def _angle_diff_deg_wrap(cur_deg: Column, prev_deg: Column) -> Column:
    """
    Minimal wrapped angular difference (degrees) between two bearings.

    Parameters
    ----------
    cur_deg : pyspark.sql.Column
        Current bearing/COG in degrees.
    prev_deg : pyspark.sql.Column
        Previous bearing/COG in degrees.

    Returns
    -------
    pyspark.sql.Column
        Absolute minimal angle difference in degrees (range [0, 180]).
    """
    cur = F.radians(cur_deg)
    prev = F.radians(prev_deg)
    d = F.atan2(F.sin(cur - prev), F.cos(cur - prev))
    return F.abs(F.degrees(d))


def compute_stats_spark(df: DataFrame, level: AggLevel = "mmsi") -> DataFrame:
    """
    Compute trajectory statistics on a **PySpark** DataFrame (distributed).

    The routine:
      1) Sorts records within each MMSI by timestamp,
      2) Computes per-row segment distance (prev → current) with haversine UDF,
      3) Aggregates per MMSI: total distance, straight-line distance (first→last),
         tortuosity (total/straight), cumulative wrapped turn index from COG,
         and SOG summaries (avg, max).

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Input AIS frame. Expected columns:
        - required: ``LAT`` (float), ``LON`` (float)
        - optional: ``MMSI`` (long/int), ``ts`` (timestamp), ``SOG`` (float), ``COG`` (float)
    level : {"mmsi"}, default="mmsi"
        Aggregation level. Currently only per-MMSI aggregation is supported.

    Returns
    -------
    pyspark.sql.DataFrame
        One row per MMSI with:
        ``["MMSI","points","distance_km","straight_km","tortuosity","turn_index_deg","avg_sog","max_sog"]``.

    Raises
    ------
    ValueError
        If required columns are missing or unsupported level requested.

    Notes
    -----
    - For best performance, ensure data are **partitioned/sorted** by ``MMSI, ts``.
    - If ``ts`` is missing, order of points is undefined; distances will ignore ordering.
    - ``turn_index_deg`` requires ``COG``; otherwise yields 0.0.
    """
    # Validate required columns
    req = {"LAT", "LON"}
    if not req.issubset(set(df.columns)):
        raise ValueError(f"Missing required columns: {req - set(df.columns)}")

    if level != "mmsi":
        raise ValueError("Only level='mmsi' is currently supported by the Spark backend.")

    # If MMSI is missing, we can force a single group, but per-MMSI metrics need MMSI
    if "MMSI" not in df.columns:
        df = df.withColumn("MMSI", F.lit(0))

    # Window for lag/first/last within each MMSI
    # Use a stable order: by ts if present, otherwise leave as-is (distance may be inaccurate)
    if "ts" in df.columns:
        w = Window.partitionBy("MMSI").orderBy(F.col("ts").asc())
    else:
        w = Window.partitionBy("MMSI").orderBy(F.monotonically_increasing_id())

    df2 = (
        df.withColumn("lat_prev", F.lag("LAT").over(w))
        .withColumn("lon_prev", F.lag("LON").over(w))
        .withColumn("seg_km", haversine_km_udf(F.col("lat_prev"), F.col("lon_prev"), F.col("LAT"), F.col("LON")))
    )

    # Turn index (requires COG)
    if "COG" in df2.columns:
        df2 = df2.withColumn("cog_prev", F.lag("COG").over(w)).withColumn(
            "turn_deg", _angle_diff_deg_wrap(F.col("COG"), F.col("cog_prev"))
        )
    else:
        df2 = df2.withColumn("turn_deg", F.lit(0.0))

    # First/last coordinates for straight-line distance
    first_last = df2.groupBy("MMSI").agg(
        F.count(F.lit(1)).alias("points"),
        F.sum("seg_km").alias("distance_km"),
        F.first("LAT", ignorenulls=True).alias("lat0"),
        F.first("LON", ignorenulls=True).alias("lon0"),
        F.last("LAT", ignorenulls=True).alias("lat1"),
        F.last("LON", ignorenulls=True).alias("lon1"),
        F.sum("turn_deg").alias("turn_index_deg"),
        F.avg("SOG").alias("avg_sog"),
        F.max("SOG").alias("max_sog"),
    )

    # Straight-line distance and tortuosity
    out = (
        first_last.withColumn("straight_km", haversine_km_udf("lat0", "lon0", "lat1", "lon1"))
        .withColumn(
            "tortuosity",
            F.col("distance_km")
            / F.when(F.col("straight_km") <= F.lit(1e-6), F.lit(1e-6)).otherwise(F.col("straight_km")),
        )
        .drop("lat0", "lon0", "lat1", "lon1")
    )

    # Consistent rounding (optional; comment out if you prefer raw doubles)
    out = out.select(
        "MMSI",
        "points",
        F.round("distance_km", 3).alias("distance_km"),
        F.round("straight_km", 3).alias("straight_km"),
        F.round("tortuosity", 3).alias("tortuosity"),
        F.round("turn_index_deg", 1).alias("turn_index_deg"),
        "avg_sog",
        "max_sog",
    )

    return out
