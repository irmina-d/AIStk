"""
Quick Folium map preview for a selected MMSI.
"""
from aistk.core import AISDataset

def main():
    ds = AISDataset("data/ais").filter(mmsi=[244660000]).between("2024-01-03","2024-01-05")
    html = ds.plot_map("out/track.html")
    print("Saved:", html)

if __name__ == "__main__":
    main()
