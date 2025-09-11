import polars as pl
from aistk.events import detect_events_df


def test_detect_events_df(df_events: pl.DataFrame):
    df = df_events.sort("BaseDateTime").with_columns([
        pl.coalesce([
            pl.col("BaseDateTime").str.strptime(pl.Datetime, strict=False),
            pl.col("BaseDateTime").str.to_datetime(strict=False),
        ]).alias("ts")
    ])
    ev = detect_events_df(df, turn_deg=30.0, stop_sog=0.5, stop_min=15, draft_jump_m=0.3)
    types = set(ev["type"].to_list())
    assert {"sharp_turn", "stop", "gap", "draft_change"}.issubset(types)
