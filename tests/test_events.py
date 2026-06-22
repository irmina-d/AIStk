import math

import polars as pl
import pytest

from aistk.events import detect_events_df
from tests import conftests as _conftests


@pytest.fixture
def df_events() -> pl.DataFrame:
    return _conftests.df_events.__wrapped__()


def test_detect_events_df(df_events: pl.DataFrame):
    df = df_events.sort("BaseDateTime")
    if df.schema.get("BaseDateTime") == pl.Utf8:
        ts_expr = pl.coalesce([
            pl.col("BaseDateTime").str.strptime(pl.Datetime, strict=False),
            pl.col("BaseDateTime").str.to_datetime(strict=False),
        ])
    else:
        ts_expr = pl.col("BaseDateTime").cast(pl.Datetime)
    df = df.with_columns(ts_expr.alias("ts"))

    stop_min = 10
    ev = detect_events_df(
        df, turn_deg=30.0, stop_sog=0.5, stop_min=stop_min, draft_jump_m=0.3
    )
    types = set(ev["type"].to_list())
    assert {"sharp_turn", "stop", "gap", "draft_change"}.issubset(types)
    stop_events = ev.filter(pl.col("type") == "stop")
    assert stop_events.height >= 1
    assert math.isclose(stop_events[0, "duration_min"], float(stop_min), abs_tol=1e-9)


def test_detect_events_df_is_vessel_aware_no_cross_mmsi_events():
    df = pl.DataFrame(
        {
            "MMSI": [111111111, 222222222],
            "ts": [
                "2024-01-01T00:00:00",
                "2024-01-01T01:00:00",
            ],
            "COG": [0.0, 180.0],
            "SOG": [10.0, 10.0],
            "Draft": [7.0, 9.0],
        }
    ).with_columns(pl.col("ts").str.to_datetime(strict=False))

    ev = detect_events_df(df, turn_deg=30.0, draft_jump_m=0.3, gap_s=600)
    assert ev.height == 0


def test_detect_events_df_outputs_mmsi_for_detected_events(df_events: pl.DataFrame):
    df = df_events.with_columns(pl.col("BaseDateTime").cast(pl.Datetime).alias("ts"))
    ev = detect_events_df(df, turn_deg=30.0, stop_sog=0.5, stop_min=10, draft_jump_m=0.3)
    assert "MMSI" in ev.columns
    assert set(ev.drop_nulls("MMSI")["MMSI"].unique().to_list()) == {222222222}


def test_detect_events_df_mixed_event_schema_allows_float_draft_delta():
    """Regression for large-file benchmark failures with mixed event rows.

    The event table contains different event types with nullable metric columns.
    Polars must not infer a too-narrow schema before a floating draft delta such
    as 4.8 appears.
    """
    df = pl.DataFrame(
        {
            "MMSI": [333333333, 333333333, 333333333, 333333333],
            "ts": [
                "2024-01-01T00:00:00",
                "2024-01-01T00:05:00",
                "2024-01-01T00:30:00",
                "2024-01-01T00:40:00",
            ],
            "COG": [0.0, 180.0, 181.0, 182.0],
            "SOG": [10.0, 0.1, 0.1, 10.0],
            "Draft": [1.0, 1.0, 5.8, 5.8],
        }
    ).with_columns(pl.col("ts").str.to_datetime(strict=False))

    ev = detect_events_df(df, turn_deg=30.0, stop_sog=0.5, stop_min=15, draft_jump_m=0.3, gap_s=600)

    assert ev.schema["delta_m"] == pl.Float64
    assert ev.filter((pl.col("type") == "draft_change") & (pl.col("delta_m") == 4.8)).height == 1
    assert {"sharp_turn", "stop", "gap", "draft_change"}.issubset(set(ev["type"].to_list()))
