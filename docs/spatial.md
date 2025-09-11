# spatial.py — Spatial Processing (H3, Geofencing, Segmentation)

## Purpose
Transforms AIS points into spatial indices, segments tracks, and supports geofencing analyses.

## Responsibilities
- Project coordinates and compute distances/segments.
- Aggregate into H3 hexagons for heatmaps/load analysis.
- Check points-in-polygon for port areas or fairways.

## Interactions with Other Modules
- viz.py (map layers)
- events.py (event-in-area)
- io.py (GeoJSON)

## Usage Example
```python
from aisdataset import spatial

h3_df = spatial.to_h3(df, resolution=7)
in_port = spatial.point_in_polygon(df, polygons_gdf=ports_gdf)
```

## Public API (Outline)
**Functions**
- `grid_features(df, resolution=...)` — Assign AIS points to H3 cells at given resolution and compute aggregates.
- `geofence(df, polygon)` — Filter AIS points inside a polygon (shapely.geometry.Polygon).

## Notes & Design Considerations
- Assumes canonical AIS columns after `schema.validate_columns()`.
- Keep I/O and analytics separated for testability.
- Prefer vectorized operations; avoid per-row Python loops where possible.