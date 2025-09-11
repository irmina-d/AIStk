import polars as pl
from aistk.schema import normalize_columns


def test_normalize_columns_aliases():
    df = pl.DataFrame({"Longitude": [18.6], "Latitude": [54.3], "Speed": [12.0], "Course": [45.0]})
    out = normalize_columns(df)
    assert set(["LON", "LAT", "SOG", "COG"]).issubset(set(out.columns))
