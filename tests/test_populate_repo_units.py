import importlib
import importlib.util
from pathlib import Path

import pandas as pd
import pytest


# Prefer the installed/package module in the user's environment.
# Fall back to the sidecar replacement file when running this test standalone.
try:
    pr = importlib.import_module("dms_datastore.populate_repo")
except Exception:
    mod_path = Path(__file__).with_name("populate_repo_modernized.py")
    spec = importlib.util.spec_from_file_location("populate_repo_modernized", mod_path)
    pr = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(pr)


def test_raw_meta_from_fname_single_year():
    meta = pr._raw_meta_from_fname("usgs_anh@north_11303500_flow_2024.csv")
    assert meta["agency"] == "usgs"
    assert meta["station_id"] == "anh"
    assert meta["subloc"] == "north"
    assert meta["agency_id"] == "11303500"
    assert meta["param"] == "flow"
    assert meta["year"] == "2024"


def test_raw_meta_from_fname_span_years():
    meta = pr._raw_meta_from_fname("cdec_ccf@radial_b95020_height_2020_9999.csv")
    assert meta["agency"] == "cdec"
    assert meta["station_id"] == "ccf"
    assert meta["subloc"] == "radial"
    assert meta["agency_id"] == "b95020"
    assert meta["param"] == "height"
    assert meta["syear"] == "2020"
    assert meta["eyear"] == "9999"


def test_rename_with_meta_force_uses_template(monkeypatch, tmp_path):
    src = tmp_path / "usgs_anh_11303500_flow_2024.csv"
    src.write_text("dummy")

    calls = []

    def fake_replace(old, new):
        calls.append((old, new))

    monkeypatch.setattr(pr.os, "replace", fake_replace)

    new_meta = {
        "agency": "usgs",
        "station_id": "anh",
        "subloc": None,
        "agency_id": "11303500",
        "param": "flow",
        "year": "2025",
    }

    out = pr._rename_with_meta(str(src), new_meta, force=True)
    expected = str(tmp_path / "usgs_anh_11303500_flow_2025.csv")
    assert out == expected
    assert calls == [(str(src), expected)]


def test_existing_stations_uses_template_parsing(monkeypatch):
    files = [
        "/tmp/usgs_anh@north_11303500_flow_2024.csv",
        "/tmp/cdec_ccf@radial_b95020_height_2020_9999.csv",
        "/tmp/usgs_anh@south_11303500_flow_2024.csv",
    ]
    monkeypatch.setattr(pr.glob, "glob", lambda pat: files)
    assert pr.existing_stations("ignored") == {"anh", "ccf"}


def test_list_ncro_stations_extracts_fields(monkeypatch):
    files = [
        "/tmp/ncro_anh_b9542100_ec_2020_9999.csv",
        "/tmp/ncro_mab_b1234567_temp_2024.csv",
    ]
    monkeypatch.setattr(pr.glob, "glob", lambda pat: files)
    got = pr.list_ncro_stations("/tmp")

    assert list(got.columns) == ["station_id", "param", "agency", "agency_id_from_file"]
    assert got.to_dict("records") == [
        {"station_id": "anh", "param": "ec", "agency": "cdec", "agency_id_from_file": "b9542100"},
        {"station_id": "mab", "param": "temp", "agency": "cdec", "agency_id_from_file": "b1234567"},
    ]


def test_revise_filename_syears_single_year(monkeypatch, tmp_path):
    src = tmp_path / "usgs_anh_11303500_flow_2024.csv"
    src.write_text("dummy")

    monkeypatch.setattr(pr.glob, "glob", lambda pat: [str(src)])
    monkeypatch.setattr(
        pr,
        "read_ts",
        lambda *args, **kwargs: pd.DataFrame(
            {"value": [1.0, 2.0]},
            index=pd.to_datetime(["2025-01-01", "2025-01-02"]),
        ),
    )

    renames = []
    monkeypatch.setattr(pr, "_write_renames", lambda rows, outfile: renames.extend(rows))

    moved = []
    monkeypatch.setattr(pr.os, "replace", lambda old, new: moved.append((old, new)))

    pr.revise_filename_syears(str(tmp_path / "*.csv"), force=True, outfile="ignore.csv")

    expected = str(tmp_path / "usgs_anh_11303500_flow_2025.csv")
    assert moved == [(str(src), expected)]
    assert renames == [(str(src), expected)]


def test_revise_filename_syear_eyear_updates_both_years(monkeypatch, tmp_path):
    src = tmp_path / "usgs_anh_11303500_flow_2020_2024.csv"
    src.write_text("dummy")

    monkeypatch.setattr(pr.glob, "glob", lambda pat: [str(src)])
    monkeypatch.setattr(
        pr,
        "read_ts",
        lambda *args, **kwargs: pd.DataFrame(
            {"value": [1.0, 2.0]},
            index=pd.to_datetime(["2021-03-01", "2023-07-01"]),
        ),
    )

    renames = []
    monkeypatch.setattr(pr, "_write_renames", lambda rows, outfile: renames.extend(rows))

    moved = []
    monkeypatch.setattr(pr.os, "replace", lambda old, new: moved.append((old, new)))

    pr.revise_filename_syear_eyear(str(tmp_path / "*.csv"), force=True, outfile="ignore.csv")

    expected = str(tmp_path / "usgs_anh_11303500_flow_2021_2023.csv")
    assert moved == [(str(src), expected)]
    assert renames == [(str(src), expected)]


def test_revise_filename_syear_eyear_preserves_open_end_9999(monkeypatch, tmp_path):
    src = tmp_path / "ncro_anh_b9542100_ec_2020_9999.csv"
    src.write_text("dummy")

    monkeypatch.setattr(pr.glob, "glob", lambda pat: [str(src)])
    monkeypatch.setattr(
        pr,
        "read_ts",
        lambda *args, **kwargs: pd.DataFrame(
            {"value": [1.0, 2.0]},
            index=pd.to_datetime(["2022-05-01", "2024-07-01"]),
        ),
    )

    renames = []
    monkeypatch.setattr(pr, "_write_renames", lambda rows, outfile: renames.extend(rows))

    moved = []
    monkeypatch.setattr(pr.os, "replace", lambda old, new: moved.append((old, new)))

    pr.revise_filename_syear_eyear(str(tmp_path / "*.csv"), force=True, outfile="ignore.csv")

    expected = str(tmp_path / "ncro_anh_b9542100_ec_2022_9999.csv")
    assert moved == [(str(src), expected)]
    assert renames == [(str(src), expected)]
