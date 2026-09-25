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


@pytest.fixture(autouse=True)
def _isolate_raw_repo_root(monkeypatch, tmp_path):
    # _raw_meta_from_fname only needs the raw repo's filename_templates, but
    # repo_config() also checks that "root" exists on disk; stub it to a
    # tmp_path so parsing doesn't depend on the real network repo being reachable.
    raw_spec = dict(pr.dstore_config.config["repos"]["raw"])
    raw_spec["root"] = str(tmp_path)
    monkeypatch.setitem(pr.dstore_config.config["repos"], "raw", raw_spec)
    monkeypatch.setattr(pr.dstore_config, "_repo_cache", None)


def test_raw_meta_from_fname_single_year():
    meta = pr._raw_meta_from_fname("usgs_anh@north_11303500_flow_2024.csv")
    assert meta["source"] == "usgs"
    assert meta["station_id"] == "anh"
    assert meta["subloc"] == "north"
    assert meta["agency_id"] == "11303500"
    assert meta["param"] == "flow"
    assert meta["year"] == "2024"


def test_raw_meta_from_fname_span_years():
    meta = pr._raw_meta_from_fname("cdec_ccf@radial_b95020_height_2020_9999.csv")
    assert meta["source"] == "cdec"
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
        "source": "usgs",
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


def test_source_availability_uses_blank_variable_for_all_station_variables(tmp_path):
    policy = tmp_path / "source_availability.csv"
    policy.write_text(
        "# A comment is allowed.\n"
        "station_id,source,variable,available_from\n"
        "c51,cdec,,2025-01-01\n"
        "c51,cdec,temp,2025-02-01\n"
    )
    availability = pr._load_source_availability(policy)
    stationlist = pd.DataFrame(
        {
            "station_id": ["c51", "c51", "untouched"],
            "param": ["flow", "temp", "flow"],
        }
    )

    groups = pr._apply_source_availability(
        stationlist,
        "cdec",
        pd.Timestamp("2024-01-01"),
        None,
        availability,
    )

    assert [(start, group.station_id.tolist()) for start, group in groups] == [
        (pd.Timestamp("2025-01-01"), ["c51"]),
        (pd.Timestamp("2025-02-01"), ["c51"]),
        (pd.Timestamp("2024-01-01"), ["untouched"]),
    ]


def test_source_availability_skips_historical_window_after_handoff():
    stationlist = pd.DataFrame({"station_id": ["c51"], "param": ["flow"]})
    availability = pd.DataFrame(
        {
            "station_id": ["c51"],
            "source": ["cdec"],
            "variable": [""],
            "available_from": pd.to_datetime(["2025-01-01"]),
        }
    )

    groups = pr._apply_source_availability(
        stationlist,
        "cdec",
        pd.Timestamp("2000-01-01"),
        pd.Timestamp("2019-12-31"),
        availability,
    )

    assert groups == []


def test_source_availability_allows_source_without_policy():
    stationlist = pd.DataFrame({"station_id": ["dwr_ncro"], "param": ["flow"]})
    availability = pd.DataFrame(
        {
            "station_id": ["c51"],
            "source": ["cdec"],
            "variable": [""],
            "available_from": pd.to_datetime(["2025-01-01"]),
        }
    )
    start = pd.Timestamp("1980-01-01")

    groups = pr._apply_source_availability(
        stationlist, "dwr_ncro", start, None, availability
    )

    assert [(effective_start, group.to_dict("records")) for effective_start, group in groups] == [
        (start, [{"station_id": "dwr_ncro", "param": "flow"}])
    ]


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


def test_populate_ncro_realtime_uses_integer_year(monkeypatch):
    ncrodf = pd.DataFrame({"station_id": ["anh"], "param": ["ec"]})
    calls = []
    monkeypatch.setattr(pr, "list_ncro_stations", lambda dest: ncrodf)
    monkeypatch.setattr(
        pr,
        "supplement_ncro_with_cdec",
        lambda *args, **kwargs: calls.append((args, kwargs)),
    )

    pr.populate_ncro_realtime("incoming", 2024)

    assert calls == [
        ((ncrodf, "incoming", pd.Timestamp("2024-01-01")), {"overwrite": True})
    ]


def test_populate_ncro_realtime_defaults_to_two_years_ago(monkeypatch):
    calls = []
    monkeypatch.setattr(pr, "list_ncro_stations", lambda dest: pd.DataFrame())
    monkeypatch.setattr(
        pr,
        "supplement_ncro_with_cdec",
        lambda *args, **kwargs: calls.append((args, kwargs)),
    )

    pr.populate_ncro_realtime("incoming")

    expected = pd.Timestamp(pd.Timestamp.today().year - 2, 1, 1)
    assert calls[0][0][2] == expected


def test_populate_ncro_realtime_rejects_non_january_start():
    with pytest.raises(ValueError, match="integer year or January 1"):
        pr.populate_ncro_realtime("incoming", pd.Timestamp("2024-02-01"))


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
