"""
Schema definitions for AIS decoded CSV data.
Includes expected columns, dtypes, and alias mappings.
"""

DEFAULT_COLUMNS = {
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

ALIASES = {
    "Longitude": "LON",
    "Latitude": "LAT",
    "Speed": "SOG",
    "Course": "COG",
}

def normalize_columns(df):
    """Rename aliases and ensure required columns exist."""
    for old, new in ALIASES.items():
        if old in df.columns and new not in df.columns:
            df = df.rename({old: new})
    return df
