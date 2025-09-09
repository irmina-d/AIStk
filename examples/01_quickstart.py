# 01_quickstart.py
# Minimal end-to-end: load CSV, filter by date/MMSI, collect, print head.

from aistk import AISDataset

def main():
    # For demo purposes we reuse the tiny CSV from tests/data
    data_root = "tests/data"           # change to your AIS directory
    pattern = "mini_ais.csv"           # e.g., "AIS_2024_*.csv" in production

    ds = (AISDataset(data_root, pattern=pattern)
          .with_columns(["MMSI","BaseDateTime","LAT","LON","SOG","COG","Draft"])
          .between("2024-01-01","2024-01-02")
          .filter(mmsi=123))

    df = ds.collect()
    print(df.head())

if __name__ == "__main__":
    main()
