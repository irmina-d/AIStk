"""
Batch event detection on a filtered subset.
"""
from aistk.core import AISDataset
from aistk.events import detect_events_df

def main():
    ds = AISDataset("data/ais").filter(mmsi=[244660000]).between("2024-01-01","2024-01-07")
    df = ds.collect()
    ev = detect_events_df(df, turn_deg=30.0, stop_sog=0.5, stop_min=15, draft_jump_m=0.3)
    print(ev.head())
    ev.write_parquet("out/events_week.parquet")
    print("Wrote: out/events_week.parquet")

if __name__ == "__main__":
    main()
