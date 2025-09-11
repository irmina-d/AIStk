import polars as pl
from aistk.io import write_parquet


def test_write_parquet(tmp_dir):
    df = pl.DataFrame({"A": [1, 2, 3]})
    out = write_parquet(df, tmp_dir / "x.parquet")
    assert out.exists()
