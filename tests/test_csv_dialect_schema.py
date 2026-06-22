import polars as pl

from aistk.core import AISDataset, _infer_csv_separator


def test_semicolon_lowercase_schema_is_normalized(tmp_dir):
    path = tmp_dir / "ais_semicolon.csv"
    path.write_text(
        "mmsi;basedatetime;lat;lon;sog;cog;draft\n"
        "1;2024-01-01T00:00:00;54.3;18.6;10.0;0.0;8.0\n"
        "1;2024-01-01T00:05:00;54.4;18.7;11.0;1.0;8.1\n",
        encoding="utf-8",
    )

    assert _infer_csv_separator(path) == ";"
    out = AISDataset(str(tmp_dir), pattern="*.csv").between("2024-01-01", "2024-01-02").collect()

    assert {"MMSI", "BaseDateTime", "LAT", "LON", "SOG", "COG", "Draft", "ts"}.issubset(out.columns)
    assert out["MMSI"].dtype == pl.Int64
    assert out["LAT"].dtype == pl.Float64
    assert out.height == 2


def test_missing_requested_columns_fails_informatively(tmp_dir):
    path = tmp_dir / "bad.csv"
    path.write_text("foo,bar\n1,2\n", encoding="utf-8")

    ds = AISDataset(str(tmp_dir), pattern="*.csv").with_columns(["MMSI", "BaseDateTime"])
    try:
        ds.collect()
    except ValueError as exc:
        msg = str(exc)
        assert "None of the requested columns" in msg
        assert "foo" in msg and "bar" in msg
    else:
        raise AssertionError("Expected an informative ValueError")


def test_transceiver_alias_is_normalized(tmp_dir):
    path = tmp_dir / "ais_transceiver.csv"
    path.write_text(
        "mmsi,base_date_time,longitude,latitude,sog,cog,transceiver\n"
        "1,2024-01-01T00:00:00,18.6,54.3,10.0,0.0,A\n",
        encoding="utf-8",
    )

    out = AISDataset(str(tmp_dir), pattern="*.csv").collect()

    assert "TransceiverClass" in out.columns
    assert "transceiver" not in out.columns
    assert out["TransceiverClass"].to_list() == ["A"]
