
import os
import shutil

import polars as pl

from aistk.core import AISDataset
from aistk.stats import compute_stats_df

def test_stats(tmp_path):
    src = os.path.join(os.path.dirname(__file__), "data", "mini_ais.csv")
    shutil.copy(src, tmp_path / "mini_ais.csv")
    ds = (AISDataset(str(tmp_path))
          .with_columns(["MMSI","BaseDateTime","LAT","LON","SOG","COG","Draft"])
          .between("2024-01-01","2024-01-02"))
    stats = ds.stats()
    assert stats.height == 1
    assert "distance_km" in stats.columns


def test_compute_stats_handles_all_null_sog():
    df = pl.DataFrame(
        {
            "LAT": [10.0, 10.001],
            "LON": [20.0, 20.001],
            "SOG": pl.Series("SOG", [None, None], dtype=pl.Float64),
        }
    )

    stats = compute_stats_df(df)

    assert stats.height == 1
    assert stats["max_sog"][0] is None
    assert stats["avg_sog"][0] is None
