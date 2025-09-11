from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, Iterator, Optional, TypedDict, Union


class AISRecord(TypedDict, total=False):
    """
    Minimal typed mapping for online AIS records used by the streaming detector.

    Keys (all optional because data may arrive partially):
    - MMSI: int
    - ts: int | float  (event time; milliseconds since epoch are recommended)
    - LAT: float
    - LON: float
    - COG: float       (degrees)
    - SOG: float       (knots)
    - Draft: float     (metres)
    """
    MMSI: int
    ts: Union[int, float]
    LAT: float
    LON: float
    COG: float
    SOG: float
    Draft: float


@dataclass
class VesselState:
    """Mutable per-MMSI state for online event detection."""
    last_ts_ms: Optional[int] = None
    last_cog_rad: Optional[float] = None
    stop_start_ms: Optional[int] = None
    last_draft: Optional[float] = None


def _to_ms(ts: Union[int, float]) -> int:
    """
    Normalize a timestamp (ms or ns or s) to **milliseconds** as integer.

    Heuristic:
      - ts >= 1e12 and < 1e15 → assume **ms**
      - ts >= 1e15            → assume **ns** → convert to ms
      - else                  → assume **s**  → convert to ms
    """
    if ts >= 1e15:
        return int(ts / 1e6)  # ns → ms
    if ts >= 1e12:
        return int(ts)        # ms
    return int(ts * 1e3)      # s → ms


def process_stream(
    records: Iterable[AISRecord],
    *,
    turn_deg: float = 30.0,
    stop_sog: float = 0.5,
    stop_min: int = 15,
    gap_s: int = 600,
    draft_jump_m: float = 0.3,
) -> Iterator[dict]:
    """
    Online (incremental) event detection over an iterable stream of AIS records.

    This generator maintains a small state per MMSI and yields events **as soon
    as they are detectable**, without buffering the entire dataset.

    Parameters
    ----------
    records : Iterable[dict]
        Stream of AIS records (e.g., from a socket, Kafka consumer, or file tail).
        Expected keys (optional due to partial inputs): see `AISRecord`.
        At minimum, `MMSI` and `ts` are required to maintain state.
    turn_deg : float, default=30.0
        Threshold (degrees) for a **sharp_turn** event (ΔCOG).
    stop_sog : float, default=0.5
        SOG threshold (knots) below which points are considered stationary.
    stop_min : int, default=15
        Minimum duration (minutes) for a **stop** event.
    gap_s : int, default=600
        Temporal gap threshold (seconds) to emit a **gap** event.
    draft_jump_m : float, default=0.3
        Absolute draught change threshold (metres) for a **draft_change** event.

    Yields
    ------
    dict
        Event dictionaries with at least:
        - "type": str      (sharp_turn | stop | gap | draft_change)
        - "MMSI": int
        - "ts": int        (milliseconds since epoch)
        Plus event-specific fields:
        - sharp_turn: "delta_deg": float
        - stop:       "duration_min": float
        - gap:        "gap_s": int
        - draft_change: "delta_m": float

    Notes
    -----
    - Timestamps in input may be in seconds, milliseconds, or nanoseconds; the
      function normalizes them internally to **milliseconds**.
    - The detector is **order-sensitive** per MMSI; for out-of-order arrivals,
      consider buffering/ordering upstream if needed.
    - State is in-memory (a dict per MMSI). For very high cardinality or
      distributed processing, store state externally (e.g., Redis/KV) or shard
      by MMSI hash.

    Examples
    --------
    >>> recs = [
    ...     {"MMSI": 1, "ts": 1710000000000, "COG": 10.0, "SOG": 10.0, "Draft": 8.0},
    ...     {"MMSI": 1, "ts": 1710000005000, "COG": 50.0, "SOG": 0.2,  "Draft": 8.0},
    ...     {"MMSI": 1, "ts": 1710000900000, "COG": 55.0, "SOG": 0.1,  "Draft": 8.5},
    ... ]
    >>> list(process_stream(recs, stop_min=1))
    [{'type': 'sharp_turn', 'MMSI': 1, 'ts': 1710000005000, 'delta_deg': 40.0},
     {'type': 'gap', 'MMSI': 1, 'ts': 1710000900000, 'gap_s': 850},
     {'type': 'draft_change', 'MMSI': 1, 'ts': 1710000900000, 'delta_m': 0.5}]
    """
    import math

    state: Dict[int, VesselState] = {}
    stop_ms_threshold = stop_min * 60_000
    gap_ms_threshold = gap_s * 1000

    for r in records:
        # Mandatory fields to maintain state:
        if "MMSI" not in r or "ts" not in r:
            continue

        mmsi = int(r["MMSI"])
        ts_ms = _to_ms(float(r["ts"]))
        sog = r.get("SOG")
        cog = r.get("COG")
        draft = r.get("Draft")

        st = state.setdefault(mmsi, VesselState())

        # --- GAP detection (previous timestamp vs. current) ---
        if st.last_ts_ms is not None:
            gap_ms = ts_ms - st.last_ts_ms
            if gap_ms > gap_ms_threshold:
                yield {"type": "gap", "MMSI": mmsi, "ts": ts_ms, "gap_s": int(gap_ms / 1000)}

        # --- SHARP TURN detection (wrapped ΔCOG) ---
        if cog is not None:
            cur_rad = math.radians(float(cog))
            if st.last_cog_rad is not None:
                d = abs(math.degrees(math.atan2(math.sin(cur_rad - st.last_cog_rad),
                                                math.cos(cur_rad - st.last_cog_rad))))
                if d >= turn_deg:
                    yield {"type": "sharp_turn", "MMSI": mmsi, "ts": ts_ms, "delta_deg": float(round(d, 2))}
            st.last_cog_rad = cur_rad

        # --- STOP detection (running window below SOG) ---
        if sog is not None:
            if float(sog) < stop_sog:
                # start or continue a stop
                if st.stop_start_ms is None:
                    st.stop_start_ms = ts_ms
            else:
                # leaving a stop zone; if it lasted long enough, emit event
                if st.stop_start_ms is not None:
                    dur_ms = ts_ms - st.stop_start_ms
                    if dur_ms >= stop_ms_threshold:
                        yield {
                            "type": "stop",
                            "MMSI": mmsi,
                            "ts": ts_ms,
                            "duration_min": round(dur_ms / 60_000.0, 2),
                        }
                st.stop_start_ms = None

        # --- DRAFT change detection ---
        if draft is not None:
            cur_draft = float(draft)
            if st.last_draft is not None:
                delta = abs(cur_draft - st.last_draft)
                if delta >= draft_jump_m:
                    yield {"type": "draft_change", "MMSI": mmsi, "ts": ts_ms, "delta_m": float(round(delta, 3))}
            st.last_draft = cur_draft

        # update last timestamp
        st.last_ts_ms = ts_ms
