"""Tests that populate_main collects per-agency and per-station failures
and writes them to a CSV file without propagating exceptions."""

import pandas as pd
import pytest
from pathlib import Path

import dms_datastore.populate_repo as populate_repo_mod


_FAILURE_KEYS = {"agency", "station_id", "agency_id", "param", "subloc", "exc_type", "message"}

_SAMPLE_FAILURE = {
    "agency": "usgs",
    "station_id": "bad_sta",
    "agency_id": "11111111",
    "param": "flow",
    "subloc": None,
    "exc_type": "RuntimeError",
    "message": "Simulated download failure",
}


def test_populate_main_collects_station_failures(tmp_path, monkeypatch):
    """Station-level failures returned by populate() must appear in the
    failures CSV written by populate_main()."""

    dest = tmp_path / "raw"
    dest.mkdir()
    failures_file = tmp_path / "failures.csv"

    def _fake_populate(dest_arg, agency=None, varlist=None, partial_update=False):
        return [_SAMPLE_FAILURE]

    # Patch all the post-processing calls inside populate_main that would
    # fail with no real repository.
    monkeypatch.setattr(populate_repo_mod, "populate", _fake_populate)
    monkeypatch.setattr(populate_repo_mod, "rationalize_time_partitions", lambda *a, **kw: None)
    monkeypatch.setattr(populate_repo_mod, "revise_filename_syear_eyear", lambda *a, **kw: None)

    populate_repo_mod.populate_main(
        str(dest),
        agencies=["usgs"],
        failures_file=str(failures_file),
    )

    assert failures_file.exists()
    df = pd.read_csv(failures_file)
    assert len(df) == 1
    row = df.iloc[0]
    assert row["station_id"] == "bad_sta"
    assert row["agency"] == "usgs"


def test_populate_main_writes_empty_csv_on_clean_run(tmp_path, monkeypatch):
    """When no failures occur, populate_main must still write a valid (header-
    only) CSV at the designated path."""

    dest = tmp_path / "raw"
    dest.mkdir()
    failures_file = tmp_path / "clean_failures.csv"

    monkeypatch.setattr(populate_repo_mod, "populate", lambda *a, **kw: [])
    monkeypatch.setattr(populate_repo_mod, "rationalize_time_partitions", lambda *a, **kw: None)
    monkeypatch.setattr(populate_repo_mod, "revise_filename_syear_eyear", lambda *a, **kw: None)

    populate_repo_mod.populate_main(
        str(dest),
        agencies=["usgs"],
        failures_file=str(failures_file),
    )

    assert failures_file.exists()
    df = pd.read_csv(failures_file)
    assert len(df) == 0
    assert list(df.columns) == ["agency", "station_id", "agency_id", "param", "subloc", "exc_type", "message"]


def test_populate_main_captures_agency_level_exception(tmp_path, monkeypatch):
    """If populate() raises an exception rather than returning a list, the
    exception must be caught and recorded as a failure row in the CSV."""

    dest = tmp_path / "raw"
    dest.mkdir()
    failures_file = tmp_path / "agency_fail.csv"

    def _exploding_populate(dest_arg, agency=None, varlist=None, partial_update=False):
        raise RuntimeError("whole agency exploded")

    monkeypatch.setattr(populate_repo_mod, "populate", _exploding_populate)
    monkeypatch.setattr(populate_repo_mod, "rationalize_time_partitions", lambda *a, **kw: None)
    monkeypatch.setattr(populate_repo_mod, "revise_filename_syear_eyear", lambda *a, **kw: None)

    # Should NOT raise
    populate_repo_mod.populate_main(
        str(dest),
        agencies=["usgs"],
        failures_file=str(failures_file),
    )

    assert failures_file.exists()
    df = pd.read_csv(failures_file)
    assert len(df) == 1
    assert df.iloc[0]["exc_type"] == "RuntimeError"
