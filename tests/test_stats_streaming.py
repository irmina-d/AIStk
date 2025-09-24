import polars as pl
import pytest

from aistk.stats_streaming import compute_stats_lazy
from aistk.stats import compute_stats_df
from tests.conftests import df_two_points as _df_two_points_fixture


@pytest.fixture
def df_two_points() -> pl.DataFrame:
    return _df_two_points_fixture.__wrapped__()


def test_compute_stats_lazy_matches_eager(df_two_points: pl.DataFrame):
    eager = compute_stats_df(df_two_points, level="mmsi")
    out = compute_stats_lazy(df_two_points.lazy(), level="mmsi").collect(engine="streaming")
    assert out["distance_km"][0] == eager["distance_km"][0]
    assert out["straight_km"][0] == eager["straight_km"][0]


def test_compute_stats_lazy_collect_matches_eager(df_two_points: pl.DataFrame):
    eager = compute_stats_df(df_two_points, level="mmsi")
    out = compute_stats_lazy(df_two_points.lazy(), level="mmsi").collect()
    assert out["distance_km"][0] == eager["distance_km"][0]
    assert out["straight_km"][0] == eager["straight_km"][0]
    assert out["tortuosity"][0] == eager["tortuosity"][0]
