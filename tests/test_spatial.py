import pytest
import polars as pl

pytest.importorskip("h3", reason="h3 not installed; skipping H3 tests")
pytest.importorskip("shapely", reason="shapely not installed; skipping geofence tests")

from shapely.geometry import Polygon
from aistk.spatial import grid_features, geofence


def test_grid_features_h3():
    df = pl.DataFrame({"LAT": [54.3, 54.31], "LON": [18.6, 18.61], "SOG": [10, 12], "COG": [45, 50]})
    out = grid_features(df, resolution=7)
    assert set(["h3", "points"]).issubset(set(out.columns))


def test_geofence_polygon():
    df = pl.DataFrame({"LAT": [54.3, 54.31], "LON": [18.6, 18.61]})
    poly = Polygon([(18.5, 54.2), (18.8, 54.2), (18.8, 54.5), (18.5, 54.5)])
    inside = geofence(df, poly)
    assert inside.height >= 1
