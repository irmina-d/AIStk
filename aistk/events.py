
import polars as pl
import numpy as np

def detect_events_df(df: pl.DataFrame, turn_deg: float = 30.0, stop_sog: float = 0.5,
                     stop_min: int = 15, draft_jump_m: float = 0.3) -> pl.DataFrame:
    events = []

    if {"COG","ts"}.issubset(df.columns):
        cog = np.unwrap(np.radians(df["COG"].fill_null(strategy="forward").to_numpy()))
        d = np.degrees(np.abs(np.diff(cog)))
        for i, val in enumerate(d):
            if val >= turn_deg:
                events.append({"type":"sharp_turn", "ts": df["ts"][i+1], "delta_deg": float(val)})

    if {"SOG","ts"}.issubset(df.columns):
        sog = df["SOG"].fill_null(strategy="forward").to_numpy()
        ts = df["ts"].to_numpy()
        mask = sog < stop_sog
        if mask.any():
            idx = np.where(mask)[0]
            splits = np.split(idx, np.where(np.diff(idx)!=1)[0]+1)
            for g in splits:
                if g.size>1:
                    dt_ms = (ts[g[-1]].item() - ts[g[0]].item())/1e6  # ns→ms
                    if dt_ms >= stop_min*60*1000:
                        events.append({"type":"stop", "ts": df["ts"][g[-1]], "duration_min": round(dt_ms/60000,2)})

    if {"Draft","ts"}.issubset(df.columns):
        dr = df["Draft"].fill_null(strategy="forward").to_numpy()
        dd = np.abs(np.diff(dr))
        for i, val in enumerate(dd):
            if val >= draft_jump_m:
                events.append({"type":"draft_change", "ts": df["ts"][i+1], "delta_m": float(val)})

    if "ts" in df.columns:
        gaps = df.sort("ts")["ts"].diff().dt.seconds()
        for i,sec in enumerate(gaps):
            if sec is not None and sec > 600:
                events.append({"type":"gap", "ts": df["ts"][i], "gap_s": int(sec)})

    return pl.DataFrame(events) if events else pl.DataFrame({"type":[], "ts": []})
