
import polars as pl
from aistk.core import AISDataset

def test_collect(tmp_path):
    # Use the sample CSV provided in tests/data
    ds = AISDataset(str(tmp_path), pattern="*.csv")
    # create a copy of mini csv file
    import shutil, os
    src = os.path.join(os.path.dirname(__file__), "data", "mini_ais.csv")
    shutil.copy(src, tmp_path / "mini_ais.csv")
    df = ds.with_columns(["MMSI","BaseDateTime","LAT","LON","SOG","COG","Draft"]).between("2024-01-01","2024-01-02").collect()
    assert df.height == 3
    assert set(["MMSI","LAT","LON"]).issubset(df.columns)
