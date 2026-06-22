"""Getting started: load sample AIS data, compute stats, and write Parquet."""

from pathlib import Path

from aistk import AISDataset


def main() -> None:
    out_dir = Path("out")
    out_dir.mkdir(exist_ok=True)

    ds = (
        AISDataset("data/sample", pattern="ais_sample.csv")
        .with_columns(["MMSI", "BaseDateTime", "LAT", "LON", "SOG", "COG", "Draft"])
        .between("2024-01-01", "2024-01-02")
    )

    df = ds.collect()
    print("Rows:", df.height)

    stats = ds.stats(level="mmsi")
    print(stats)

    out_path = out_dir / "sample.parquet"
    ds.write_parquet(out_path, sort_rows=False)
    print("Written:", out_path)


if __name__ == "__main__":
    main()
