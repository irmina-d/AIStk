# viz.py — Visualization (Maps & Exploratory Plots)

## Purpose
Convenience plotting tools for trajectories, heatmaps, and event overlays using Folium/Plotly.

## Responsibilities
- Quick base maps with sampled AIS points or tracks.
- Density/heat visualization via H3 or kernel density.
- Interactive HTML exports for sharing.

## Interactions with Other Modules
- spatial.py (H3, polygons)
- events.py (overlays)
- io.py (HTML/GeoJSON outputs)

## Usage Example
```python
from aisdataset import viz

m = viz.plot_map(df.sample(10000), lat_col="lat", lon_col="lon")
m.save("out/map.html")
```

## Public API (Outline)
**Functions**
- `plot_track_html(df, out_html)`

## Notes & Design Considerations
- Assumes canonical AIS columns after `schema.validate_columns()`.
- Keep I/O and analytics separated for testability.
- Prefer vectorized operations; avoid per-row Python loops where possible.