from __future__ import annotations

from pathlib import Path
from typing import Union

import polars as pl

PathLike = Union[str, Path]


def write_parquet(df: pl.DataFrame, path: PathLike) -> Path:
    """
    Write a Polars DataFrame to Parquet.

    Parameters
    ----------
    df : pl.DataFrame
        Polars DataFrame to be serialized.
    path : str or pathlib.Path
        Target path for the Parquet file. Parent directories must exist.

    Returns
    -------
    pathlib.Path
        Path to the written Parquet file.

    Raises
    ------
    OSError
        If the file cannot be written (e.g., permission denied).

    Notes
    -----
    - This is a thin wrapper around ``DataFrame.write_parquet``.
    - The function does not partition by default; for larger datasets consider
      `scan_parquet` and partitioned storage.
    - Ensures return type is always a `Path` for consistency.

    Examples
    --------
    >>> import polars as pl
    >>> from aistk.io import write_parquet
    >>> df = pl.DataFrame({"MMSI":[1,2], "LAT":[54.3,54.4], "LON":[18.6,18.7]})
    >>> path = write_parquet(df, "out.parquet")
    >>> path
    PosixPath('out.parquet')
    """
    out_path = Path(path)
    df.write_parquet(out_path)
    return out_path
