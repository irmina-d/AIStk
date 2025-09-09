# 03_detect_events_and_map.py
# Detect events (sharp turns, stops, gaps, draft changes) and export a map.

from aistk import AISDataset
import polars as pl

def main():
    data_root = "tests/data"           # change to your AIS directory
    pattern = "mini_ais.csv"

    ds = (AISDataset(data_root, pattern=pattern)
          .with_columns(["MMSI","BaseDateTime","LAT","LON","SOG","COG","Draft"])
          .between("2024-01-01","2024-01-02")
          .filter(mmsi=123))

    events = ds.detect_events(turn_deg=15.0, stop_sog=0.5, stop_min=10, draft_jump_m=0.2)
    print(events)

    # Save events for inspection
    if isinstance(events, pl.DataFrame) and events.height > 0:
        events.write_csv("examples/out_events.csv")
        print("Saved examples/out_events.csv")

    # Plot HTML map (requires folium)
    try:
        ds.plot_map("examples/track_demo.html", mmsi=123)
        print("Saved examples/track_demo.html")
    except Exception as e:
        print("Map not generated:", e)

if __name__ == "__main__":
    main()
