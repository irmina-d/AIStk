from __future__ import annotations

from typing import List, Dict, Union

import numpy as np
import polars as pl


def detect_events_df(
    df: pl.DataFrame,
    turn_deg: float = 30.0,
    stop_sog: float = 0.5,
    stop_min: int = 15,
    draft_jump_m: float = 0.3,
) -> pl.DataFrame:
    """
    Detect navigational events from an AIS Polars DataFrame.

    Event classes implemented:
      - **sharp_turn**: heading change (COG) exceeds ``turn_deg`` between consecutive points.
      - **stop**: speed over ground (SOG) below ``stop_sog`` sustained for at least
        ``stop_min`` minutes.
      - **draft_change**: absolute draught change between consecutive points
        exceeds ``draft_jump_m``.
      - **gap**: temporal gap between consecutive timestamps exceeds 600 seconds.

    Parameters
    ----------
    df : pl.DataFrame
        AIS records. Expected columns:
        - ``"ts"`` (timestamp, ns resolution),
        - optionally ``"COG"``, ``"SOG"``, ``"Draft"``.
    turn_deg : float, default=30.0
        Minimum heading change (degrees) to flag a "sharp_turn".
    stop_sog : float, default=0.5
        SOG threshold (knots). Speeds strictly below this count towards a "stop".
    stop_min : int, default=15
        Minimum stop duration (minutes).
    draft_jump_m : float, default=0.3
        Draught change threshold (metres).

    Returns
    -------
    pl.DataFrame
        DataFrame with detected events. Typical columns:
        - ``"type"`` : event type string,
        - ``"ts"`` : event timestamp,
        - plus event-specific attributes (e.g., ``delta_deg``, ``duration_min``, ``delta_m``, ``gap_s``).

        Returns an empty frame with columns ``["type","ts"]`` if no events are detected.

    Notes
    -----
    - COG is unwrapped (via ``numpy.unwrap``) before computing differences.
    - SOG is forward-filled before stop detection.
    - Timestamps are assumed to be Polars datetime (ns). For stops,
      duration is derived as ``(t_end - t_start)/1e6`` → ms.
    - Gaps are defined as ``diff(ts) > 600 s``.

    Examples
    --------
    >>> import polars as pl
    >>> from aistk.events import detect_events_df
    >>> df = pl.DataFrame({
    ...     "ts": pl.datetime_range("2024-01-01", "2024-01-01 01:00", interval="5m", eager=True),
    ...     "COG": [0, 20, 70, 80, 85, 90],
    ...     "SOG": [10, 0.2, 0.1, 10, 11, 12],
    ...     "Draft": [8.0, 8.0, 8.5, 8.5, 9.0, 9.0],
    ... })
    >>> ev = detect_events_df(df)
    >>> ev.columns
    ['type', 'ts', ...]
    """
    events: List[Dict[str, Union[str, float, int]]] = []

    # sharp turns
    if {"COG", "ts"}.issubset(df.columns):
        cog = np.unwrap(np.radians(df["COG"].fill_null(strategy="forward").to_numpy()))
        d = np.degrees(np.abs(np.diff(cog)))
        for i, val in enumerate(d):
            if val >= turn_deg:
                events.append(
                    {"type": "sharp_turn", "ts": df["ts"][i + 1], "delta_deg": float(val)}
                )

    # prolonged stops
    if {"SOG", "ts"}.issubset(df.columns):
        sog = df["SOG"].fill_null(strategy="forward").to_numpy()
        ts = df["ts"].to_numpy()
        if not np.issubdtype(ts.dtype, np.datetime64):
            ts = ts.astype("datetime64[ns]")
        else:
            ts = ts.astype("datetime64[ns]", copy=False)
        mask = sog < stop_sog
        if mask.any():
            idx = np.where(mask)[0]
            splits = np.split(idx, np.where(np.diff(idx) != 1)[0] + 1)
            for g in splits:
                if g.size > 1:
                    end_idx = int(g[-1])
                    start_idx = int(g[0])
                    dt_ms = (ts[end_idx] - ts[start_idx]) / np.timedelta64(1, "ms")
                    if dt_ms >= stop_min * 60 * 1000:
                        events.append(
                            {
                                "type": "stop",
                                "ts": df["ts"][end_idx],
                                "duration_min": round(float(dt_ms) / 60000, 2),
                            }
                        )

    # draught changes
    if {"Draft", "ts"}.issubset(df.columns):
        dr = df["Draft"].fill_null(strategy="forward").to_numpy()
        dd = np.abs(np.diff(dr))
        for i, val in enumerate(dd):
            if val >= draft_jump_m:
                events.append(
                    {
                        "type": "draft_change",
                        "ts": df["ts"][i + 1],
                        "delta_m": float(val),
                    }
                )

    # gaps
    if "ts" in df.columns:
        gaps = df.sort("ts")["ts"].diff().dt.total_seconds()
        for i, sec in enumerate(gaps):
            if sec is not None and sec > 600:
                events.append({"type": "gap", "ts": df["ts"][i], "gap_s": int(sec)})

    return pl.DataFrame(events) if events else pl.DataFrame({"type": [], "ts": []})
