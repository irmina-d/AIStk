""" Schema definitions for AIS decoded CSV data. Includes expected columns, dtypes, and alias mappings. """

from __future__ import annotations

from typing import Dict
import polars as pl

# Canonical schema definition (column → dtype string)
DEFAULT_COLUMNS: Dict[str, str] = {
    "MMSI": "int",
    "BaseDateTime": "datetime",
    "LAT": "float",
    "LON": "float",
    "SOG": "float",
    "COG": "float",
    "Heading": "float",
    "IMO": "str",
    "CallSign": "str",
    "VesselName": "str",
    "VesselType": "str",
    "Status": "int",
    "Length": "float",
    "Width": "float",
    "Draft": "float",
    "Cargo": "int",
    "TransceiverClass": "str",
}

# Vendor-specific alias → canonical
ALIASES: Dict[str, str] = {
    "Longitude": "LON",
    "Latitude": "LAT",
    "Speed": "SOG",
    "Course": "COG",
}


def normalize_columns(df: pl.DataFrame) -> pl.DataFrame:
    """
    Normalize AIS DataFrame columns to canonical schema.

    This function renames known vendor-specific aliases (see ``ALIASES``)
    to the canonical names used throughout the library. It does not enforce
    dtypes or add missing columns; use `DEFAULT_COLUMNS` as a guide for
    downstream validation.

    Parameters
    ----------
    df : pl.DataFrame
        Polars DataFrame with decoded AIS messages. Column names may include
        vendor-specific alternatives such as ``"Longitude"`` instead of ``"LON"``.

    Returns
    -------
    pl.DataFrame
        DataFrame with column names aligned to canonical AIS schema.

    Notes
    -----
    - Only renames columns; does not cast dtypes or fill missing values.
    - If both the alias and canonical column exist, the canonical column is preserved.
    - Use `DEFAULT_COLUMNS` externally for stricter validation.

    Examples
    --------
    >>> import polars as pl
    >>> from aistk.schema import normalize_columns
    >>> df = pl.DataFrame({"Longitude":[18.6], "Latitude":[54.3], "Speed":[12]})
    >>> df2 = normalize_columns(df)
    >>> df2.columns
    ['LON','LAT','SOG']
    """
    for old, new in ALIASES.items():
        if old in df.columns and new not in df.columns:
            df = df.rename({old: new})
    return df
