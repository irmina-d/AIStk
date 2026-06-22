"""Quick Folium map preview for a selected MMSI using sample data."""

from pathlib import Path

from aistk import AISDataset


def main() -> None:
    Path("out").mkdir(exist_ok=True)
    ds = AISDataset("data/sample", pattern="ais_sample.csv").filter(mmsi=[123456789])
    html = ds.plot_map("out/sample_track.html", mmsi=123456789)
    print("Saved:", html)


if __name__ == "__main__":
    main()
