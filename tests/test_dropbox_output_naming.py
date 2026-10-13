from pathlib import Path

import pandas as pd
import pytest

from dms_datastore import dstore_config
import dms_datastore.dropbox_data as dropbox_module
from dms_datastore.dropbox_data import _validated_output_filename, get_spec
from dms_datastore.read_ts import read_ts


@pytest.mark.parametrize("entry_index, expected", [(0, [1.5, 2.5]), (1, [59.0, 60.0])])
def test_cimis_recipe_reads_only_datetime_and_selected_value(tmp_path, entry_index, expected):
    recipe_path = (
        Path(__file__).resolve().parents[1]
        / "dms_datastore/dropbox_recipes/dropbox_cimis_hourly.yaml"
    )
    entry = get_spec(recipe_path)["data"][entry_index]
    collect = entry["collect"]
    source = tmp_path / "cimis.csv"
    source.write_text(
        "datetime,HlyWindSpd.Value,HlyAirTmp.Value,HlyWindDir.Unit\n"
        "2020-01-01 01:00:00,1.5,59,degree\n"
        "2020-01-01 02:00:00,2.5,60,0\n",
        encoding="utf-8",
    )

    result = read_ts(
        str(source),
        selector=collect["selector"],
        freq=entry["metadata"]["freq"],
        **collect["reader_args"],
    )

    assert collect["reader_args"]["usecols"] == ["datetime", collect["selector"]]
    assert list(result.columns) == [collect["selector"]]
    assert result.iloc[:, 0].tolist() == expected
    assert result.index.freqstr == "h"


def test_cimis_recipe_reconciles_actual_annual_writer_outputs(tmp_path, monkeypatch):
    recipe_path = (
        Path(__file__).resolve().parents[1]
        / "dms_datastore/dropbox_recipes/dropbox_cimis_hourly.yaml"
    )
    spec = get_spec(recipe_path)
    incoming = tmp_path / "incoming"
    staging = tmp_path / "staging"
    repo = tmp_path / "repo"
    incoming.mkdir()
    repo.mkdir()
    timestamps = pd.date_range("2019-12-31", periods=48, freq="h")
    source = incoming / "cimis_cms140_140_hourly_2019_2020.csv"
    source.write_text(
        "datetime,HlyWindSpd.Value,HlyAirTmp.Value\n"
        + "".join(f"{timestamp},1.5,59\n" for timestamp in timestamps),
        encoding="utf-8",
    )
    config = {
        "name": "atmospheric",
        "root": str(repo),
        "registry": "atmospheric",
        "regular": True,
        "provider_key": "agency",
        "filename_templates": [
            "{agency}_{station_id@subloc}_{agency_id}_{param}_{year}.csv"
        ],
    }
    registry = pd.DataFrame(
        [{
            "station_id": "cms140",
            "agency_id": "140",
            "agency": "cimis",
            "lat": 38.121739,
            "lon": -121.674455,
            "x": 616189.1,
            "y": 4220152.2,
            "agency_lat": 38.121739,
            "agency_lon": -121.674455,
        }]
    ).set_index("station_id", drop=False)
    monkeypatch.setattr(dstore_config, "repo_config", lambda name, require_root=True: config)
    monkeypatch.setattr(dropbox_module, "repo_config", lambda name: config)
    monkeypatch.setattr(dropbox_module, "repo_registry", lambda name: registry)
    for entry in spec["data"]:
        entry["collect"]["location"] = str(incoming)
        entry["output"]["staging"]["dir"] = str(staging)
        entry["output"]["repo_data_dir"] = str(repo)
        entry["output"]["reconcile"] = {"prefer": "staged", "allow_new_series": True}

    dropbox_module.apply_dropbox_workflow(spec)

    expected = {
        f"cimis_cms140_140_{param}_{year}.csv"
        for param in ("windspd", "airtemp")
        for year in (2019, 2020)
    }
    assert {path.name for path in staging.glob("*.csv")} == expected
    assert {path.name for path in repo.glob("*.csv")} == expected


@pytest.fixture
def esmr_metadata():
    return {
        "source": "esmr",
        "station_id": "bwwtp",
        "agency_id": "bwwtp",
        "param": "ec",
    }


def test_formatted_output_rejects_unsharded_filename(esmr_metadata) -> None:
    with pytest.raises(ValueError, match="potw_ec: output naming is incompatible"):
        _validated_output_filename(
            esmr_metadata,
            "formatted",
            {"chunk_years": False},
            "potw_ec",
        )


@pytest.mark.integration
def test_daily_formatted_output_accepts_unsharded_filename(esmr_metadata) -> None:
    result = _validated_output_filename(
        esmr_metadata,
        "daily_formatted",
        {"chunk_years": False},
        "potw_ec",
    )

    assert result == "esmr_bwwtp_bwwtp_ec.csv"
