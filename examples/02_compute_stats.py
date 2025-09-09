# 02_compute_stats.py
# Compute trajectory metrics and save to CSV/Parquet.

from aistk import AISDataset
import polars as pl

def main():
    data_root = "tests/data"           # change to your AIS directory
    pattern = "mini_ais.csv"

    ds = (AISDataset(data_root, pattern=pattern)
          .with_columns(["MMSI","BaseDateTime","LAT","LON","SOG","COG","Draft"])
          .between("2024-01-01","2024-01-02"))

    stats_df = ds.stats(level="mmsi")
    print(stats_df)

    stats_df.write_csv("examples/out_stats.csv")
    stats_df.write_parquet("examples/out_stats.parquet")
    print("Saved examples/out_stats.csv and examples/out_stats.parquet")

if __name__ == "__main__":
    main()
