"""
Out-of-core statistics on very large datasets using Dask.
"""
import dask.dataframe as dd
from aistk.backends.dask_backend import compute_stats_dask

def main():
    # Parquet is preferred; CSV is possible with blocksize
    ddf = dd.read_parquet("lake/clean/2024/*.parquet")
    res = compute_stats_dask(ddf, level="mmsi")
    print(res.head())

if __name__ == "__main__":
    main()
