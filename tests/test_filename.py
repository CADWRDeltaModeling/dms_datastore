import pytest

from dms_datastore.filename import (
    naming_spec,
    build_repo_globs,
    interpret_fname,
)

def test_interpret_fname_legacy_no_backend():
    meta = interpret_fname("usgs_anh@north_11303500_flow_2024.csv")
    assert meta["agency"] == "usgs"
    assert meta["station_id"] == "anh"
    assert meta["subloc"] == "north"
    assert meta["agency_id"] == "11303500"
    assert meta["param"] == "flow"
    assert meta["year"] == "2024"


def test_interpret_fname_template_with_oneoff_naming_spec():
    spec = naming_spec(
        templates="{source}_{key@subloc}_{param@modifier}_{year}.csv",
        style="template",
    )
    meta = interpret_fname("model_jer@upper_ec@daily_2025.csv", naming=spec)
    assert meta["agency"] == "model"
    assert meta["station_id"] == "jer"
    assert meta["subloc"] == "upper"
    assert meta["param"] == "ec"
    assert meta["modifier"] == "daily"
    assert meta["year"] == "2025"


def test_build_repo_globs_no_backend():
    repo_cfg = {
        "name": "processed",
        "filename_templates": [
            "{source}_{key@subloc}_{param@modifier}_{year}.csv",
            "{source}_{key@subloc}_{param}_{year}.csv",
        ],
    }

    pats = build_repo_globs(
        repo_cfg,
        key="jer",
        subloc="upper",
        param="ec",
        modifier="daily",
        sources=["schism", "dsm2"],
        year="2025",
    )

    assert pats == [
        "schism_jer@upper_ec@daily_2025.csv",
        "schism_jer@upper_ec_2025.csv",
        "dsm2_jer@upper_ec@daily_2025.csv",
        "dsm2_jer@upper_ec_2025.csv",
    ]