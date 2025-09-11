import polars as pl
from aistk.core import AISDataset
from aistk.events import detect_events_df
from aistk.stats import compute_stats_df


def test_aisdataset_collect_and_filters(tmp_dir):
    df = pl.DataFrame({
        "MMSI": [1, 1, 2, 2],
        "LAT": [54.3, 54.31, 54.32, 54.33],
        "LON": [18.6, 18.61, 18.62, 18.63],
        "SOG": [10.0, 0.0, 11.0, 12.0],
        "COG": [0.0, 40.0, 10.0, 15.0],
        "Draft": [8.0, 8.0, 8.0, 8.5],
        "BaseDateTime": [
            "2024-01-01T00:00:00", "2024-01-01T00:20:00",
            "2024-01-01T01:00:00", "2024-01-01T01:30:00",
        ],
    })
    p1 = tmp_dir / "part1.csv"
    p2 = tmp_dir / "part2.csv"
    df.slice(0, 2).write_csv(p1)
    df.slice(2, 2).write_csv(p2)

    ds = AISDataset(str(tmp_dir), pattern="*.csv").between("2024-01-01", "2024-01-02").filter(mmsi=[1])
    out = ds.collect()
    assert "ts" in out.columns
    assert set(out["MMSI"].unique().to_list()) == {1}

    stats = compute_stats_df(out, level="mmsi")
    assert stats.height == 1

    events = detect_events_df(out)
    assert events.height >= 0  # smoke
