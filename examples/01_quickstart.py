"""Runnable quickstart using the bundled sample AIS CSV file."""

from aistk import AISDataset


def main() -> None:
    ds = (
        AISDataset("data/sample", pattern="ais_sample.csv")
        .with_columns(["MMSI", "BaseDateTime", "LAT", "LON", "SOG", "COG", "Draft"])
        .between("2024-01-01", "2024-01-02")
    )

    df = ds.collect()
    stats = ds.stats()
    events = ds.detect_events()

    print("Rows:", df.height)
    print("Data preview:")
    print(df.head())
    print("\nTrajectory statistics:")
    print(stats)
    print("\nDetected events:")
    print(events)


if __name__ == "__main__":
    main()
