
import polars as pl

def write_parquet(df: pl.DataFrame, path: str) -> None:
    df.write_parquet(path)
