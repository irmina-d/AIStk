"""Batch event detection on the bundled AIS sample."""

from pathlib import Path

from aistk import AISDataset


def main() -> None:
    out_dir = Path("out")
    out_dir.mkdir(exist_ok=True)

    ds = AISDataset("data/sample", pattern="ais_sample.csv").between("2024-01-01", "2024-01-02")
    ev = ds.detect_events(turn_deg=30.0, stop_sog=0.5, stop_min=15, draft_jump_m=0.3)
    print(ev)

    out_path = out_dir / "sample_events.parquet"
    ev.write_parquet(out_path)
    print("Wrote:", out_path)


if __name__ == "__main__":
    main()
