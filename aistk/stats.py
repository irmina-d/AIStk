
import polars as pl
import numpy as np
from .utils import haversine_km

def compute_stats_df(df: pl.DataFrame, level: str = "mmsi"):
    required = {"LAT", "LON"}
    if not required.issubset(set(df.columns)):
        return pl.DataFrame()

    def _per(pdf: pl.DataFrame):
        lat = pdf["LAT"].to_numpy()
        lon = pdf["LON"].to_numpy()
        if lat.size < 2:
            return {
                "points": int(lat.size), "distance_km": 0.0, "straight_km": 0.0,
                "tortuosity": 1.0, "max_sog": float(pdf["SOG"].max()) if "SOG" in pdf.columns else None,
                "avg_sog": float(pdf["SOG"].mean()) if "SOG" in pdf.columns else None,
            }
        dist = haversine_km(lat[:-1], lon[:-1], lat[1:], lon[1:])
        total_km = float(np.nansum(dist))
        straight_km = float(haversine_km(lat[0], lon[0], lat[-1], lon[-1]))
        tort = total_km / max(straight_km, 1e-6)
        turn_index = None
        if "COG" in pdf.columns:
            cog = np.unwrap(np.radians(pdf["COG"].fill_null(strategy="forward").to_numpy()))
            d = np.degrees(np.abs(np.diff(cog)))
            turn_index = float(np.nansum(d))
        return {
            "points": int(lat.size),
            "distance_km": round(total_km, 3),
            "straight_km": round(straight_km, 3),
            "tortuosity": round(tort, 3),
            "turn_index_deg": round(turn_index, 1) if turn_index is not None else None,
            "max_sog": float(pdf["SOG"].max()) if "SOG" in pdf.columns else None,
            "avg_sog": float(pdf["SOG"].mean()) if "SOG" in pdf.columns else None,
        }

    if "ts" in df.columns:
        df = df.sort(["MMSI","ts"]) if "MMSI" in df.columns else df.sort("ts")

    if level == "mmsi" and "MMSI" in df.columns:
        rows = []
        for g, pdf in df.group_by("MMSI", maintain_order=True):
            stats = _per(pdf)
            stats["MMSI"] = int(pdf["MMSI"][0])
            rows.append(stats)
        return pl.DataFrame(rows)
    else:
        return pl.DataFrame([_per(df)])
