"""Tests that each active downloader continues past per-station failures and
returns a properly-formatted failures list instead of raising."""

import asyncio
import pandas as pd
import pytest
from pathlib import Path

import dms_datastore.download_nwis as download_nwis
import dms_datastore.download_cdec as download_cdec
import dms_datastore.download_des as download_des
import dms_datastore.download_noaa as download_noaa
import dms_datastore.download_ncro as download_ncro


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_FAILURE_KEYS = {"agency", "station_id", "agency_id", "param", "subloc", "exc_type", "message"}


def _two_row_stationlist(**extra):
    """Two-row stationlist: row 0 = 'bad', row 1 = 'good'."""
    bad = {
        "station_id": "bad",
        "agency_id": "00000",
        "src_var_id": "00060",
        "param": "flow",
        "subloc": "default",
    }
    good = {
        "station_id": "good",
        "agency_id": "99999",
        "src_var_id": "00060",
        "param": "flow",
        "subloc": "default",
    }
    bad.update(extra)
    good.update(extra)
    return pd.DataFrame([bad, good])


# ---------------------------------------------------------------------------
# NWIS
# ---------------------------------------------------------------------------

def test_nwis_continues_past_station_failure(tmp_path, monkeypatch):
    """nwis_download returns failures list; does not raise on per-station error.

    download_station() is called in a ThreadPoolExecutor.  When it raises,
    future.result() re-raises and the outer except block converts the exception
    into a failure dict rather than propagating it.
    """
    calls = []

    def _fake_download_station(row, dest_dir, start, end, param, overwrite, endfile,
                                successes, failures, skips):
        calls.append(row.station_id)
        if row.station_id == "bad":
            raise RuntimeError("simulated network failure")
        # good station: do nothing (no files written)

    monkeypatch.setattr(download_nwis, "download_station", _fake_download_station)

    stations = _two_row_stationlist()
    result = download_nwis.nwis_download(
        stations, str(tmp_path), pd.Timestamp(2020, 1, 1)
    )

    assert isinstance(result, list)
    assert len(result) == 1
    f = result[0]
    assert _FAILURE_KEYS.issubset(f.keys()), f"Missing keys in failure dict: {f}"
    assert "bad" in calls and "good" in calls  # both stations were attempted


# ---------------------------------------------------------------------------
# CDEC
# ---------------------------------------------------------------------------

def test_cdec_continues_past_station_failure(tmp_path, monkeypatch):
    """cdec_download wraps each download_station_data call; a raised exception
    is caught, the station recorded as a failure, and processing continues."""
    calls = []

    def _fake_download_station_data(row, dest_dir, start, end, endfile, param,
                                     overwrite, freq, failures, skips):
        calls.append(row.station_id)
        if row.station_id == "bad":
            raise RuntimeError("simulated CDEC error")

    monkeypatch.setattr(download_cdec, "download_station_data", _fake_download_station_data)

    stations = _two_row_stationlist()  # src_var_id="00060", subloc="default"
    result = download_cdec.cdec_download(
        stations, str(tmp_path), pd.Timestamp(2020, 1, 1)
    )

    assert isinstance(result, list)
    # The outer try/except appends (row.station_id, row.param) as a tuple that
    # gets normalised to a dict before being returned.
    assert len(result) == 1
    assert _FAILURE_KEYS.issubset(result[0].keys())
    # Both rows must have been attempted
    assert "good" in calls


# ---------------------------------------------------------------------------
# DES — non-integer agency_id triggers a graceful per-station failure
# ---------------------------------------------------------------------------

def test_des_invalid_agency_id_does_not_raise(tmp_path, monkeypatch):
    """A station whose agency_id cannot be converted to an integer is recorded
    as a failure immediately without raising or making network calls."""

    # Build a minimal inventory DataFrame so that des_download never calls the
    # real inventory() network endpoint.
    fake_inventory = pd.DataFrame(
        columns=[
            "result_id", "station_id", "station_name", "station_active",
            "analyte_name", "unit_name", "equipment_name", "aggregate_name",
            "interval_name", "cdec_code", "probe_depth", "start_date",
            "end_date", "program_id", "rank_name",
        ]
    )

    monkeypatch.setattr(download_des, "inventory", lambda *a, **kw: fake_inventory)

    stations = pd.DataFrame([{
        "station_id": "tst",
        "agency_id": "NOT-AN-INT",  # triggers the int() failure path
        "src_var_id": "flow",
        "param": "flow",
        "subloc": "default",
    }])

    result = download_des.des_download(
        stations, str(tmp_path), pd.Timestamp(2020, 1, 1)
    )

    assert isinstance(result, list)
    assert len(result) == 1
    assert _FAILURE_KEYS.issubset(result[0].keys())


# ---------------------------------------------------------------------------
# NOAA
# ---------------------------------------------------------------------------

def test_noaa_continues_past_station_failure(tmp_path, monkeypatch):
    """noaa_download collects failures from future.result() exceptions
    and returns them instead of raising."""
    calls = []

    def _fake_download_station_data(row, dest_dir, start, end, param, overwrite,
                                     endfile, skips, verbose):
        calls.append(row.station_id)
        if row.station_id == "bad":
            raise RuntimeError("simulated NOAA error")

    monkeypatch.setattr(download_noaa, "download_station_data", _fake_download_station_data)
    # Patch subprogram so the station-type filter passes for both rows.
    monkeypatch.setattr(
        download_noaa, "subprogram",
        lambda df: pd.Series(["tidecurrent"] * len(df), index=df.index)
    )

    stations = _two_row_stationlist(name="test station")

    result = download_noaa.noaa_download(
        stations, str(tmp_path), pd.Timestamp(2020, 1, 1), param="elev"
    )

    assert isinstance(result, list)
    assert len(result) == 1
    assert _FAILURE_KEYS.issubset(result[0].keys())


# ---------------------------------------------------------------------------
# NCRO
# ---------------------------------------------------------------------------

def test_ncro_continues_past_trace_failure(tmp_path, monkeypatch):
    """ncro_download records exceptions returned by asyncio.gather
    (return_exceptions=True) as failure dicts rather than raising."""

    async def _fake_one_trace_to_csv(
        client, semaphore, station_id, agency_id, paramname,
        site, trace, dest_dir, stime, etime, overwrite,
    ):
        raise RuntimeError("simulated NCRO trace failure")

    monkeypatch.setattr(
        download_ncro, "_async_download_one_trace_to_csv", _fake_one_trace_to_csv
    )

    fake_inventory = pd.DataFrame({
        "site": ["BADSIT"],
        "trace": ["T1"],
        "param": ["flow"],
        "start_time": [pd.Timestamp(2019, 1, 1)],
        "end_time": [pd.Timestamp(2025, 1, 1)],
    })
    monkeypatch.setattr(download_ncro, "load_inventory", lambda **kw: fake_inventory)
    monkeypatch.setattr(
        download_ncro, "similar_ncro_station_names", lambda x: ["BADSIT"]
    )
    monkeypatch.setattr(
        download_ncro.dstore_config, "station_dbase", lambda: pd.DataFrame()
    )

    stations = pd.DataFrame([{
        "station_id": "tst",
        "agency_id": "BADSIT",
        "src_var_id": "flow",
        "param": "flow",
    }])

    result = download_ncro.ncro_download(
        stations, str(tmp_path), pd.Timestamp(2020, 1, 1)
    )

    assert isinstance(result, list)
    assert len(result) == 1
    assert _FAILURE_KEYS.issubset(result[0].keys())
