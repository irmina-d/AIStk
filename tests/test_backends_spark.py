import pytest

spark_mod = pytest.importorskip("pyspark.sql", reason="pyspark not installed; skipping spark backend tests")
from pyspark.sql import SparkSession
from aistk.backends.spark_backend import compute_stats_spark


def test_compute_stats_spark_smoke():
    spark = SparkSession.builder.appName("aistk-test").getOrCreate()
    sdf = spark.createDataFrame(
        [
            (1, 54.3, 18.6, 10.0, 0.0, "2024-01-01 00:00:00"),
            (1, 54.31, 18.61, 11.0, 10.0, "2024-01-01 00:10:00"),
        ],
        ["MMSI", "LAT", "LON", "SOG", "COG", "ts"],
    )
    out = compute_stats_spark(sdf, level="mmsi")
    assert "distance_km" in out.columns
    spark.stop()
