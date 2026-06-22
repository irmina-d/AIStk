import os
import subprocess
import sys
from pathlib import Path

from typer.testing import CliRunner

from aistk.cli import app


def test_readme_quickstart_python_api_runs():
    from aistk import AISDataset

    ds = (
        AISDataset("data/sample", pattern="ais_sample.csv")
        .with_columns(["MMSI", "BaseDateTime", "LAT", "LON", "SOG", "COG", "Draft"])
        .between("2024-01-01", "2024-01-02")
    )

    df = ds.collect()
    stats = ds.stats()
    events = ds.detect_events()

    assert df.height == 7
    assert stats.height == 2
    assert events.height > 0


def test_quickstart_script_runs_from_project_root():
    script = Path("examples/01_quickstart.py")
    env = {**os.environ, "PYTHONPATH": str(Path.cwd())}
    result = subprocess.run([sys.executable, str(script)], capture_output=True, text=True, check=False, env=env)
    assert result.returncode == 0, result.stderr
    assert "Rows:" in result.stdout
    assert "Trajectory statistics" in result.stdout


def test_readme_cli_commands_run_on_bundled_sample(tmp_path):
    runner = CliRunner()
    out_dir = tmp_path / "out"

    scan = runner.invoke(
        app,
        [
            "scan",
            "data/sample",
            "--pattern",
            "ais_sample.csv",
            "--from",
            "2024-01-01",
            "--to",
            "2024-01-02",
            "--cols",
            "MMSI,BaseDateTime,LAT,LON,SOG,COG,Draft",
            "--to-parquet",
            str(out_dir / "sample.parquet"),
            "--no-sort-output",
        ],
    )
    assert scan.exit_code == 0, scan.output
    assert (out_dir / "sample.parquet").exists()

    stats = runner.invoke(
        app,
        [
            "stats",
            "data/sample",
            "--pattern",
            "ais_sample.csv",
            "--from",
            "2024-01-01",
            "--to",
            "2024-01-02",
            "--engine",
            "polars-stream",
            "--out",
            str(out_dir / "sample_stats.parquet"),
        ],
    )
    assert stats.exit_code == 0, stats.output
    assert (out_dir / "sample_stats.parquet").exists()

    events = runner.invoke(
        app,
        [
            "events",
            "data/sample",
            "--pattern",
            "ais_sample.csv",
            "--from",
            "2024-01-01",
            "--to",
            "2024-01-02",
            "--out",
            str(out_dir / "sample_events.parquet"),
        ],
    )
    assert events.exit_code == 0, events.output
    assert (out_dir / "sample_events.parquet").exists()
