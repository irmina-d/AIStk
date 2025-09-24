import polars as pl
from typer.testing import CliRunner

from aistk.cli import app


def _write_sample_csvs(tmp_dir):
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
    df.slice(0, 2).write_csv(tmp_dir / "a.csv")
    df.slice(2, 2).write_csv(tmp_dir / "b.csv")


def test_cli_scan_stats_events(tmp_dir):
    _write_sample_csvs(tmp_dir)
    runner = CliRunner()

    # scan → parquet
    res1 = runner.invoke(app, [
        "scan", str(tmp_dir), "--from", "2024-01-01", "--to", "2024-01-02",
        "--to-parquet", str(tmp_dir / "out" / "data.parquet"),
    ])
    assert res1.exit_code == 0

    # stats (polars-stream)
    res2 = runner.invoke(app, [
        "stats", str(tmp_dir), "--from", "2024-01-01", "--to", "2024-01-02",
        "--engine", "polars-stream", "--out", str(tmp_dir / "out" / "stats.parquet"),
    ])
    assert res2.exit_code == 0

    # events
    res3 = runner.invoke(app, [
        "events", str(tmp_dir), "--from", "2024-01-01", "--to", "2024-01-02",
        "--out", str(tmp_dir / "out" / "events.parquet"),
    ])
    assert res3.exit_code == 0
