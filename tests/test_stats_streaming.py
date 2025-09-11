import polars as pl
from aistk.stats_streaming import compute_stats_lazy
from aistk.stats import compute_stats_df


def test_compute_stats_lazy_matches_eager(df_two_points: pl.DataFrame):
    eager = compute_stats_df(df_two_points, level="mmsi")
    out = compute_stats_lazy(df_two_points.lazy(), level="mmsi").collect(streaming=True)
    assert out["distance_km"][0] == eager["distance_km"][0]
    assert out["straight_km"][0] == eager["straight_km"][0]
