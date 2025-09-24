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
