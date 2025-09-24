import polars as pl
import pytest


@pytest.fixture
def df_two_points() -> pl.DataFrame:
    # dwa punkty ~1 km od siebie (do prostych statystyk)
    return pl.DataFrame({
        "MMSI": [111111111, 111111111],
        "LAT": [54.3000, 54.3009],
        "LON": [18.6000, 18.6009],
        "SOG": [10.0, 12.0],
        "COG": [10.0, 20.0],
        "Draft": [8.0, 8.0],
        "BaseDateTime": ["2024-01-01T00:00:00", "2024-01-01T00:10:00"],
    })


@pytest.fixture
def df_events() -> pl.DataFrame:
    # Ramka, która wywoła: sharp_turn, stop (>=15 min), gap (>600s), draft_change
    ts = pl.datetime_range(
        start=pl.datetime(2024, 1, 1, 0, 0, 0),
        end=pl.datetime(2024, 1, 1, 2, 0, 0),
        interval="10m",
        eager=True,
    )
    df = pl.DataFrame({
        "MMSI": [222222222] * len(ts),
        "LAT": [54.3 + i * 0.001 for i in range(len(ts))],
        "LON": [18.6 + i * 0.001 for i in range(len(ts))],
        "SOG": [10.0] * len(ts),
        "COG": [0.0] * len(ts),
        "Draft": [8.0] * len(ts),
        "BaseDateTime": ts,
    })
    # ostre skręcenie przy idx=1 (0 -> 40 deg)
    df = df.with_columns(pl.when(pl.arange(0, pl.len()) == 1).then(40.0).otherwise(pl.col("COG")).alias("COG"))
    # stop na idx 6..7 (20 min)
    df = df.with_columns(pl.when(pl.arange(0, pl.len()).is_in([6, 7])).then(0.0).otherwise(pl.col("SOG")).alias("SOG"))
    # skok zanurzenia przy idx=8 (+0.5 m)
    df = df.with_columns(pl.when(pl.arange(0, pl.len()) == 8).then(8.5).otherwise(pl.col("Draft")).alias("Draft"))
    # luka w czasie: wytnij idx 10 -> odstęp 20 minut
    df = df.filter(pl.arange(0, pl.len()) != 10)
    return df


@pytest.fixture
def tmp_dir(tmp_path):
    (tmp_path / "out").mkdir(parents=True, exist_ok=True)
    return tmp_path
