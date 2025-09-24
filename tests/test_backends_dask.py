import numpy as np
import pandas as pd
import pytest

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


def test_compute_stats_dask_handles_nan_mmsi():
    pdf = pd.DataFrame(
        {
            "MMSI": [np.nan, np.nan, 123456789],
            "LAT": [10.0, 10.001, 10.002],
            "LON": [20.0, 20.001, 20.002],
            "ts": pd.date_range("2024-01-01", periods=3, freq="5min"),
        }
    )

    ddf = dd.from_pandas(pdf, npartitions=2)
    out = compute_stats_dask(ddf, level="mmsi")

    nan_rows = out[out["MMSI"].isna()]
    assert not nan_rows.empty
    assert int(nan_rows["points"].iloc[0]) == 2
