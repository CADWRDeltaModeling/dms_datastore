"""Tests for download_cimis.py.

Unit tests run offline. The live API test needs CIMIS_API_KEY and is
marked ``integration``.
"""

import os
from datetime import date, timedelta

import pandas as pd
import pytest
from click.testing import CliRunner

from dms_datastore import download_cimis as dc


def _days(start, end):
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


@pytest.fixture
def fake_api(monkeypatch):
    """Replace request_records with a stub returning one row per day."""
    calls = []

    def fake(station_number, start_date, end_date, api_key, timeout, freq="D"):
        calls.append((start_date, end_date))
        return [{"Date": d.isoformat(), "Station": str(station_number)} for d in _days(start_date, end_date)]

    monkeypatch.setattr(dc, "request_records", fake)
    return calls


def test_flatten_record_nested():
    rec = {"a": {"b": 1, "c": None}, "d": [1, 2], "e": True}
    assert dc.flatten_record(rec) == {"a.b": "1", "a.c": "", "d": "[1,2]", "e": "true"}


def test_extract_records_from_providers():
    payload = {
        "Data": {
            "Providers": [
                {"Records": [
                    {"Date": "2024-01-02T00:00:00", "DayAirTmpAvg": {"Value": "50.1"}},
                    {"Date": "2024-01-01", "DayAirTmpAvg": {"Value": "49.0"}},
                ]}
            ]
        }
    }
    records = dc.extract_records(payload)
    assert [r["Date"] for r in records] == ["2024-01-02", "2024-01-01"]
    assert records[0]["DayAirTmpAvg.Value"] == "50.1"


def test_extract_records_api_error():
    with pytest.raises(ValueError, match="CIMIS API error 400"):
        dc.extract_records({"Status": {"StatusCode": "400", "Message": "bad"}})


def test_find_date_missing():
    with pytest.raises(ValueError):
        dc.find_date({"Station": "140"})


def _read(path):
    return pd.read_csv(path, index_col="datetime", parse_dates=True, dtype=str)


@pytest.mark.parametrize(
    "freq, start, end, expected",
    [
        ("D", date(1985, 3, 1), date(2012, 5, 5), [
            (date(1985, 3, 1), date(1989, 12, 31)),
            (date(1990, 1, 1), date(1999, 12, 31)),
            (date(2000, 1, 1), date(2009, 12, 31)),
            (date(2010, 1, 1), date(2012, 5, 5)),
        ]),
        ("D", date(2024, 1, 1), date(2024, 1, 3), [(date(2024, 1, 1), date(2024, 1, 3))]),
        ("H", date(2024, 3, 1), date(2025, 8, 2), [
            (date(2024, 3, 1), date(2024, 6, 30)),
            (date(2024, 7, 1), date(2024, 12, 31)),
            (date(2025, 1, 1), date(2025, 6, 30)),
            (date(2025, 7, 1), date(2025, 8, 2)),
        ]),
    ],
)
def test_chunk_windows(freq, start, end, expected):
    assert dc.chunk_windows(start, end, freq) == expected


def test_chunk_windows_stay_under_record_limit():
    daily = dc.chunk_windows(date(1982, 1, 1), date(2026, 10, 12), "D")
    hourly = dc.chunk_windows(date(2020, 1, 1), date(2026, 10, 12), "H")
    assert max((e - s).days + 1 for s, e in daily) < 5000
    assert max(((e - s).days + 1) * 24 for s, e in hourly) < 5000


def test_records_to_frame_hourly_hour_ending():
    records = [
        {"Date": "2024-01-01", "Hour": "2400", "x": "2"},
        {"Date": "2024-01-01", "Hour": "0100", "x": "1"},
    ]
    df = dc.records_to_frame(records, "H")
    assert list(df.index) == [pd.Timestamp("2024-01-01 01:00"), pd.Timestamp("2024-01-02 00:00")]
    assert list(df.columns) == ["x"]


@pytest.mark.parametrize("given, expected", [("D", "D"), ("d", "D"), (" h ", "H"), ("H", "H")])
def test_normalize_freq(given, expected):
    assert dc.normalize_freq(given) == expected


@pytest.mark.parametrize("bad", ["E", "M", "daily", ""])
def test_normalize_freq_rejects(bad):
    with pytest.raises(ValueError):
        dc.normalize_freq(bad)


def _process(tmp_path, start, end, endfile=2024, overwrite=False, freq="D"):
    return dc.process_station(
        "cms140", 140, tmp_path, start, end, endfile, "key", 10, overwrite, freq
    )


def test_process_station_writes_window(tmp_path, fake_api):
    assert _process(tmp_path, date(2024, 1, 1), date(2024, 1, 3)) == "success"
    assert fake_api == [(date(2024, 1, 1), date(2024, 1, 3))]
    df = _read(tmp_path / "cimis_cms140_140_2024_2024.csv")
    assert list(df.index) == list(pd.date_range("2024-01-01", "2024-01-03"))
    assert "Date" not in df.columns


def test_process_station_splices_decade_chunks(tmp_path, fake_api):
    _process(tmp_path, date(1999, 12, 30), date(2000, 1, 2), endfile=2000)
    assert fake_api == [
        (date(1999, 12, 30), date(1999, 12, 31)),
        (date(2000, 1, 1), date(2000, 1, 2)),
    ]
    df = _read(tmp_path / "cimis_cms140_140_1999_2000.csv")
    assert list(df.index) == list(pd.date_range("1999-12-30", "2000-01-02"))


def test_process_station_skips_empty_chunks(tmp_path, monkeypatch):
    def fake(station_number, start_date, end_date, api_key, timeout, freq="D"):
        if start_date.year < 2000:
            return []
        return [{"Date": d.isoformat(), "x": "1"} for d in _days(start_date, end_date)]

    monkeypatch.setattr(dc, "request_records", fake)
    _process(tmp_path, date(1995, 1, 1), date(2000, 1, 2), endfile=2000)
    df = _read(tmp_path / "cimis_cms140_140_1995_2000.csv")
    assert list(df.index) == list(pd.date_range("2000-01-01", "2000-01-02"))


def test_process_station_open_ended_name(tmp_path, fake_api):
    _process(tmp_path, date(2024, 1, 1), date(2024, 1, 2), endfile=9999)
    assert (tmp_path / "cimis_cms140_140_2024_9999.csv").exists()


def test_process_station_hourly_keeps_every_hour(tmp_path, monkeypatch):
    seen = []

    def fake(station_number, start_date, end_date, api_key, timeout, freq="D"):
        seen.append(freq)
        return [
            {"Date": d.isoformat(), "Hour": f"{h:02d}00", "x": "1"}
            for d in _days(start_date, end_date)
            for h in range(1, 25)
        ]

    monkeypatch.setattr(dc, "request_records", fake)
    _process(tmp_path, date(2024, 6, 30), date(2024, 7, 1), freq="H")
    assert seen == ["H", "H"]
    df = _read(tmp_path / "cimis_cms140_140_2024_2024.csv")
    assert len(df) == 48
    assert df.index[0] == pd.Timestamp("2024-06-30 01:00")
    assert df.index[-1] == pd.Timestamp("2024-07-02 00:00")


def _existing(path):
    pd.DataFrame({"x": ["old"]}, index=pd.DatetimeIndex(["2023-12-31"], name="datetime")).to_csv(path)


def test_process_station_skips_existing(tmp_path, fake_api):
    path = tmp_path / "cimis_cms140_140_2024_2024.csv"
    _existing(path)
    assert _process(tmp_path, date(2024, 1, 1), date(2024, 1, 3)) == "exists"
    assert fake_api == []
    assert list(_read(path)["x"]) == ["old"]


def test_process_station_overwrite_replaces(tmp_path, fake_api):
    path = tmp_path / "cimis_cms140_140_2024_2024.csv"
    _existing(path)
    _process(tmp_path, date(2024, 1, 1), date(2024, 1, 2), overwrite=True)
    assert list(_read(path).index) == list(pd.date_range("2024-01-01", "2024-01-02"))


def test_process_station_no_records_writes_nothing(tmp_path, monkeypatch):
    monkeypatch.setattr(dc, "request_records", lambda *args: [])
    assert _process(tmp_path, date(2010, 1, 1), date(2010, 6, 1)) == "no_data"
    assert not list(tmp_path.iterdir())


def test_cimis_download_no_data_is_not_a_failure(tmp_path, monkeypatch):
    monkeypatch.setattr(dc, "request_records", lambda *args: [])
    dc.cimis_download(STATIONS, tmp_path, api_key="key", start=date(2010, 1, 1), end=date(2010, 6, 1))
    assert not list(tmp_path.iterdir())


def test_process_station_request_error_propagates(tmp_path, monkeypatch):
    def fake(*args):
        raise dc.RequestTimeoutError("slow")

    monkeypatch.setattr(dc, "request_records", fake)
    with pytest.raises(dc.RequestTimeoutError):
        _process(tmp_path, date(2024, 1, 1), date(2024, 1, 4))
    assert not list(tmp_path.iterdir())


def test_cimis_stations_from_registry():
    stations = dc.cimis_stations()
    assert (stations["agency"] == "cimis").all()
    assert stations["station_id"].str.startswith("cms").all()
    row = dc.cimis_stations(["cms140"]).iloc[0]
    assert row["agency_id"] == "140"


def test_cimis_stations_unknown_id():
    with pytest.raises(ValueError, match="cms99999"):
        dc.cimis_stations(["cms140", "cms99999"])


def test_cimis_stations_accepts_number_or_registry_id():
    stations = dc.cimis_stations(["140", "cms140", " CMS242 ", 6, "0247"])
    assert list(stations["station_id"]) == sorted(
        ["cms6", "cms140", "cms242", "cms247"], key=lambda s: int(s[3:])
    )
    with pytest.raises(ValueError, match="cms99999"):
        dc.cimis_stations(["99999"])


@pytest.fixture
def capture_process(monkeypatch):
    calls = []

    def fake(station_id, station_number, dest_dir, start, end, endfile, *args):
        calls.append(dict(station_id=station_id, start=start, end=end, endfile=endfile, freq=args[-1]))
        if station_id == "cms2":
            raise RuntimeError("boom")
        return "ok"

    monkeypatch.setattr(dc, "process_station", fake)
    return calls


STATIONS = pd.DataFrame({"station_id": ["cms1"], "agency_id": ["1"]})


def test_cimis_download_defaults(tmp_path, capture_process):
    dc.cimis_download(STATIONS, tmp_path, api_key="key")
    assert capture_process == [
        dict(station_id="cms1", start=dc.CIMIS_RECORD_START, end=date.today(), endfile=9999, freq="D")
    ]


def test_cimis_download_lowercase_hourly(tmp_path, capture_process):
    dc.cimis_download(STATIONS, tmp_path, api_key="key", freq="h")
    assert capture_process[0]["freq"] == "H"


def test_cimis_download_explicit_window(tmp_path, capture_process):
    dc.cimis_download(STATIONS, tmp_path, api_key="key", start=date(2020, 5, 1), end=date(2021, 2, 1))
    assert capture_process[0]["endfile"] == 2021
    assert capture_process[0]["start"] == date(2020, 5, 1)


def test_cimis_download_reports_failures(tmp_path, capture_process):
    stations = pd.DataFrame({"station_id": ["cms1", "cms2"], "agency_id": ["1", "2"]})
    with pytest.raises(RuntimeError, match=r"\['cms2'\]"):
        dc.cimis_download(stations, tmp_path, api_key="key")
    assert len(capture_process) == 2


@pytest.mark.parametrize(
    "kwargs",
    [
        {"api_key": ""},
        {"api_key": "key", "end": date.today() + timedelta(days=1)},
        {"api_key": "key", "start": date(2024, 2, 1), "end": date(2024, 1, 1)},
        {"api_key": "key", "freq": "E"},
    ],
)
def test_cimis_download_validates_arguments(tmp_path, capture_process, kwargs):
    with pytest.raises(ValueError):
        dc.cimis_download(STATIONS, tmp_path, **kwargs)
    assert capture_process == []


def test_cli_help():
    result = CliRunner().invoke(dc.download_cimis_cli, ["-h"])
    assert result.exit_code == 0
    assert "--dest" in result.output


def test_cli_requires_api_key(monkeypatch):
    monkeypatch.delenv("CIMIS_API_KEY", raising=False)
    result = CliRunner().invoke(dc.download_cimis_cli, ["--stations", "cms140"])
    assert result.exit_code == 2
    assert "CIMIS_API_KEY" in result.output


def test_cli_passes_arguments(tmp_path, monkeypatch):
    captured = {}
    monkeypatch.setenv("CIMIS_API_KEY", "key")
    monkeypatch.setattr(dc, "cimis_download", lambda **kw: captured.update(kw))
    result = CliRunner().invoke(
        dc.download_cimis_cli,
        ["--dest", str(tmp_path), "--start", "2024-01-01", "--end", "2024-01-31", "--stations", "cms140", "--overwrite", "--freq", "h"],
    )
    assert result.exit_code == 0, result.output
    assert captured["freq"] == "H"
    assert captured["start"] == date(2024, 1, 1)
    assert captured["end"] == date(2024, 1, 31)
    assert captured["overwrite"] is True
    assert captured["dest_dir"] == tmp_path
    assert list(captured["stations"]["station_id"]) == ["cms140"]


@pytest.mark.integration
@pytest.mark.skipif(not os.environ.get("CIMIS_API_KEY"), reason="CIMIS_API_KEY not set")
def test_live_request_records():
    records = dc.request_records(
        140, date(2024, 1, 1), date(2024, 1, 3), os.environ["CIMIS_API_KEY"], 60
    )
    assert [r["Date"] for r in records] == ["2024-01-01", "2024-01-02", "2024-01-03"]


@pytest.mark.integration
@pytest.mark.skipif(not os.environ.get("CIMIS_API_KEY"), reason="CIMIS_API_KEY not set")
def test_live_request_records_hourly():
    records = dc.request_records(
        140, date(2024, 1, 1), date(2024, 1, 1), os.environ["CIMIS_API_KEY"], 60, "H"
    )
    assert [r["Hour"] for r in records] == [f"{h:02d}00" for h in range(1, 25)]
