import pytest
import polars as pl

pytest.importorskip("folium", reason="folium not installed; skipping map test")

from aistk.viz import plot_track_html


def test_plot_track_html(tmp_dir):
    df = pl.DataFrame({"LAT": [54.3, 54.31, 54.32], "LON": [18.6, 18.61, 18.62]})
    out = plot_track_html(df, str(tmp_dir / "map.html"))
    assert out.endswith(".html")
