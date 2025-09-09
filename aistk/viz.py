
import polars as pl

def plot_track_html(df: pl.DataFrame, out_html: str) -> str:
    try:
        import folium
    except ImportError as e:
        raise RuntimeError("Install folium to enable map plotting: pip install folium") from e
    if not {"LAT","LON"}.issubset(df.columns):
        raise ValueError("LAT/LON columns are required for plotting")
    lat = df["LAT"].to_list(); lon = df["LON"].to_list()
    if not lat or not lon:
        raise ValueError("No coordinates to plot")
    m = folium.Map(location=[sum(lat)/len(lat), sum(lon)/len(lon)], zoom_start=8)
    folium.PolyLine(list(zip(lat,lon)), weight=3, opacity=0.9).add_to(m)
    m.save(out_html)
    return out_html
