import pytest
import pandas as pd

dd = pytest.importorskip("dask.dataframe", reason="dask not installed; skipping dask backend tests")

from aistk.backends.dask_backend import compute_stats_dask


def test_compute_stats_dask_smoke():
    pdf = pd.DataFrame({
        "MMSI": [1, 1, 1],
        "LAT": [54.3, 54.31, 54.32],
        "LON": [18.6, 18.61, 18.62],
        "SOG": [10, 11, 12],
        "COG": [0, 10, 20],
        "ts": pd.date_range("2024-01-01", periods=3, freq="10min"),
    })
    ddf = dd.from_pandas(pdf, npartitions=1)
    out = compute_stats_dask(ddf, level="mmsi")
    assert not out.empty
    assert "distance_km" in out.columns
