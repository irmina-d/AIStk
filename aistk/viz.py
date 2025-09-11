from __future__ import annotations

from pathlib import Path
from typing import Union
import polars as pl

PathLike = Union[str, Path]


def plot_track_html(df: pl.DataFrame, out_html: PathLike) -> str:
    """
    Plot a vessel trajectory from an AIS DataFrame as an interactive HTML map.

    This function uses Folium (Leaflet) to generate an interactive map with
    the vessel's track drawn as a polyline. Requires columns ``"LAT"`` and
    ``"LON"`` in the input frame.

    Parameters
    ----------
    df : pl.DataFrame
        Polars DataFrame containing AIS records. Must include columns
        ``"LAT"`` and ``"LON"`` (float degrees).
    out_html : str or pathlib.Path
        Path where the generated HTML file will be saved.

    Returns
    -------
    str
        Path to the saved HTML file (as a string).

    Raises
    ------
    RuntimeError
        If `folium` is not installed.
    ValueError
        If required columns are missing or if no coordinates are available.

    Notes
    -----
    - Map center is initialized at the mean latitude/longitude of the data.
    - Polyline is drawn in the order of records in `df`.
    - Designed for quick visual inspection, not high-precision cartography.

    Examples
    --------
    >>> import polars as pl
    >>> from aistk.viz import plot_track_html
    >>> df = pl.DataFrame({"LAT": [54.3, 54.4], "LON": [18.6, 18.7]})
    >>> plot_track_html(df, "track.html")
    'track.html'
    """
    try:
        import folium
    except ImportError as e:
        raise RuntimeError("Install folium to enable map plotting: pip install folium") from e

    if not {"LAT", "LON"}.issubset(df.columns):
        raise ValueError("LAT/LON columns are required for plotting")

    lat = df["LAT"].to_list()
    lon = df["LON"].to_list()
    if not lat or not lon:
        raise ValueError("No coordinates to plot")

    m = folium.Map(location=[sum(lat) / len(lat), sum(lon) / len(lon)], zoom_start=8)
    folium.PolyLine(list(zip(lat, lon)), weight=3, opacity=0.9).add_to(m)
    m.save(str(out_html))
    return str(out_html)
