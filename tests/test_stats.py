
import polars as pl, os, shutil
from aistk.core import AISDataset

def test_stats(tmp_path):
    src = os.path.join(os.path.dirname(__file__), "data", "mini_ais.csv")
    shutil.copy(src, tmp_path / "mini_ais.csv")
    ds = (AISDataset(str(tmp_path))
          .with_columns(["MMSI","BaseDateTime","LAT","LON","SOG","COG","Draft"])
          .between("2024-01-01","2024-01-02"))
    stats = ds.stats()
    assert stats.height == 1
    assert "distance_km" in stats.columns
