import pytest

from aistk.streaming.events_online import process_stream


def test_process_stream_emits_events():
    recs = [
        {"MMSI": 1, "ts": 1710000000000, "COG": 10.0, "SOG": 10.0, "Draft": 8.0},
        {"MMSI": 1, "ts": 1710000005000, "COG": 50.0, "SOG": 0.2,  "Draft": 8.0},   # sharp turn + start stop
        {"MMSI": 1, "ts": 1710000900000, "COG": 55.0, "SOG": 0.1,  "Draft": 8.5},   # gap + draft change
        {"MMSI": 1, "ts": 1710000960000, "COG": 55.0, "SOG": 5.0,  "Draft": 8.5},   # leave stop (>=10 min configured)
    ]
    events = list(process_stream(recs, stop_min=10))
    types = {e["type"] for e in events}
    assert {"sharp_turn", "gap", "draft_change"}.issubset(types)
    assert "stop" in types

    stop_event = next(e for e in events if e["type"] == "stop")
    assert stop_event["duration_min"] == pytest.approx(15.92, rel=1e-3)
