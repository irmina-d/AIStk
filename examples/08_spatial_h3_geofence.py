"""
Spatial utilities: H3 gridding & polygon geofencing.
"""
from shapely.geometry import Polygon
from aistk.core import AISDataset
from aistk.spatial import grid_features, geofence

def main():
    ds = AISDataset("data/ais").filter(mmsi=[244660000]).between("2024-01-01","2024-01-02")
    df = ds.collect()

    # H3 aggregates
    h3df = grid_features(df, resolution=7)
    print(h3df.head())

    # Geofence (simple rectangle)
    poly = Polygon([(18.5,54.2),(18.8,54.2),(18.8,54.5),(18.5,54.5)])
    inside = geofence(df, poly)
    print("Points inside polygon:", inside.height)

if __name__ == "__main__":
    main()
