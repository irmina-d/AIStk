"""
Streaming-friendly stats with Polars LazyFrame to handle large files efficiently.
"""
import polars as pl
from aistk.core import AISDataset
from aistk.stats_streaming import compute_stats_lazy

def main():
    ds = AISDataset("data/ais")  # pattern defaults to *.csv
    lf = ds._build()
    res = compute_stats_lazy(lf, level="mmsi").collect(streaming=True)
    print(res.sort("distance_km", descending=True).head())

if __name__ == "__main__":
    main()
