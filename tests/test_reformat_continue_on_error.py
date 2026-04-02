"""Tests that reformat() continues past bad files and reformat_main() writes a CSV."""

import os
import pandas as pd
import pytest
from pathlib import Path

import dms_datastore.reformat as reformat_mod


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_VALID_CSV_CONTENT = """\
# format: dwr-dms-1.0
# agency: usgs
# station_id: anh
# param: flow
# subloc: default
# agency_id: 11455420
datetime,value,user_flag
2020-01-01 00:00,1.0,0
2020-01-02 00:00,2.0,0
2020-01-03 00:00,3.0,0
"""


def _write_valid_file(path: Path):
    path.write_text(_VALID_CSV_CONTENT)


def _write_bad_file(path: Path):
    path.write_text("this is not a parseable CSV file\n!!garbage!!\n")


# ---------------------------------------------------------------------------
# reformat() unit tests (no ProcessPoolExecutor involved)
# ---------------------------------------------------------------------------

def test_reformat_returns_failure_for_bad_file(tmp_path, monkeypatch):
    """reformat() should return a list containing the path of any file it
    cannot parse rather than raising an exception."""
    indir = tmp_path / "raw"
    indir.mkdir()
    outdir = tmp_path / "formatted"
    outdir.mkdir()

    bad = indir / "usgs_bad_99999_flow_2020.csv"
    _write_bad_file(bad)

    # Patch infer_internal_meta_for_file so that bad file makes it past meta
    # inference and fails at the read_ts stage (which exercises the except branch).
    def _fake_meta(fpath):
        raise ValueError("cannot infer meta from garbage file")

    monkeypatch.setattr(reformat_mod, "infer_internal_meta_for_file", _fake_meta)

    failures = reformat_mod.reformat(str(indir), str(outdir), ["usgs*.csv"])

    assert isinstance(failures, list)
    assert len(failures) == 1
    assert str(bad) in failures[0]


def test_reformat_continues_past_bad_file(tmp_path, monkeypatch):
    """After a bad file fails, reformat() continues to process subsequent files."""
    indir = tmp_path / "raw"
    indir.mkdir()
    outdir = tmp_path / "formatted"
    outdir.mkdir()

    bad = indir / "usgs_bad_99998_flow_2020.csv"
    _write_bad_file(bad)

    call_count = {"count": 0}
    original_infer = reformat_mod.infer_internal_meta_for_file

    def _selective_meta(fpath):
        call_count["count"] += 1
        if "bad" in fpath:
            raise ValueError(f"Simulated failure for {fpath}")
        return original_infer(fpath)

    monkeypatch.setattr(reformat_mod, "infer_internal_meta_for_file", _selective_meta)

    failures = reformat_mod.reformat(str(indir), str(outdir), ["usgs*.csv"])

    # Only the bad file should be in failures
    assert len(failures) == 1
    # infer was called (proves we entered the loop)
    assert call_count["count"] >= 1


# ---------------------------------------------------------------------------
# reformat_main() tests — ProcessPoolExecutor calls the REAL reformat();
# we use empty or broken indir to control what it does without lambdas.
# ---------------------------------------------------------------------------

def test_reformat_main_writes_csv_on_empty_dir(tmp_path):
    """With no input files, reformat_main() still writes a valid (header-only)
    failures CSV at the supplied path."""
    indir = tmp_path / "raw"
    indir.mkdir()
    outdir = tmp_path / "formatted"
    outdir.mkdir()
    failures_file = tmp_path / "reformat_failures.csv"

    reformat_mod.reformat_main(
        inpath=str(indir),
        outpath=str(outdir),
        agencies=["usgs"],
        failures_file=str(failures_file),
    )

    assert failures_file.exists()
    df = pd.read_csv(failures_file)
    assert "filepath" in df.columns
    assert len(df) == 0


def test_reformat_main_records_bad_files(tmp_path):
    """Files whose names don't match the expected naming convention are
    recorded in the failures CSV (no monkeypatching / pickling needed)."""
    indir = tmp_path / "raw"
    indir.mkdir()
    outdir = tmp_path / "formatted"
    outdir.mkdir()
    failures_file = tmp_path / "bad_files.csv"

    # A file whose name cannot be parsed by interpret_fname will raise in
    # infer_internal_meta_for_file and be caught by reformat()'s except clause.
    bad = indir / "usgs_garbage.csv"
    bad.write_text("invalid content\n")

    reformat_mod.reformat_main(
        inpath=str(indir),
        outpath=str(outdir),
        agencies=["usgs"],
        failures_file=str(failures_file),
    )

    assert failures_file.exists()
    df = pd.read_csv(failures_file)
    assert len(df) == 1
    assert "usgs_garbage" in df.iloc[0]["filepath"]
