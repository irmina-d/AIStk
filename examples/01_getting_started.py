"""
Getting started: load AIS data, filter by MMSI & time, compute stats, write Parquet.
"""
from aistk.core import AISDataset
from aistk.stats import compute_stats_df

def main():
    ds = AISDataset("data/ais", pattern="*.csv")
    ds = ds.filter(mmsi=[244660000]).between("2024-01-01","2024-02-01")
    df = ds.collect()
    print("Rows:", df.height)

    # Stats (eager Polars DataFrame path)
    out = compute_stats_df(df, level="mmsi")
    print(out)

    # Persist
    ds.write_parquet("out/ais_jan.parquet")
    print("Written: out/ais_jan.parquet")

if __name__ == "__main__":
    main()
