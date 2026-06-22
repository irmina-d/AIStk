"""Streaming-friendly trajectory statistics with Polars LazyFrame."""

from aistk import AISDataset


def main() -> None:
    ds = AISDataset("data/sample", pattern="ais_sample.csv").between("2024-01-01", "2024-01-02")
    res = ds.stats(level="mmsi")
    print(res.sort("distance_km", descending=True))


if __name__ == "__main__":
    main()
