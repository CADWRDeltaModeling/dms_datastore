import pytest

from dms_datastore.filename import (
    naming_spec,
    build_repo_globs,
    interpret_fname,
    meta_to_filename,
)


def test_interpret_fname_no_backend():
    spec = naming_spec(
        templates="{source}_{key@subloc}_{agency_id}_{param}_{year}.csv",
    )
    meta = interpret_fname(
        "usgs_anh@north_11303500_flow_2024.csv",
        naming=spec,
    )
    assert meta["source"] == "usgs"
    assert meta["station_id"] == "anh"
    assert meta["subloc"] == "north"
    assert meta["agency_id"] == "11303500"
    assert meta["param"] == "flow"
    assert meta["year"] == "2024"

def test_interpret_fname_template_with_oneoff_naming_spec():
    spec = naming_spec(
        templates="{source}_{key@subloc}_{param@modifier}_{year}.csv"
    )
    meta = interpret_fname("model_jer@upper_ec@daily_2025.csv", naming=spec)
    assert meta["source"] == "model"
    assert meta["station_id"] == "jer"
    assert meta["subloc"] == "upper"
    assert meta["param"] == "ec"
    assert meta["modifier"] == "daily"
    assert meta["year"] == "2025"



def test_interpret_fname_template_with_agency_slot():
    spec = naming_spec(
        templates="{agency}_{key@subloc}_{agency_id}_{param}_{year}.csv"
    )
    meta = interpret_fname("usbr_jer@upper_11303500_flow_2025.csv", naming=spec)
    assert meta["agency"] == "usbr"
    assert meta["station_id"] == "jer"
    assert meta["subloc"] == "upper"
    assert meta["agency_id"] == "11303500"
    assert meta["param"] == "flow"
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



def test_meta_to_filename_template_round_trip_source():
    spec = naming_spec(
        templates="{source}_{key@subloc}_{param@modifier}_{year}.csv"
    )
    meta = {
        "source": "model",
        "station_id": "jer",
        "subloc": "upper",
        "param": "ec",
        "modifier": "daily",
        "year": "2025",
    }

    fname = meta_to_filename(meta, naming=spec)
    assert fname == "model_jer@upper_ec@daily_2025.csv"
    parsed = interpret_fname(fname, naming=spec)
    assert parsed["source"] == "model"
    assert parsed["station_id"] == "jer"
    assert parsed["subloc"] == "upper"
    assert parsed["param"] == "ec"
    assert parsed["modifier"] == "daily"
    assert parsed["year"] == "2025"



def test_meta_to_filename_template_round_trip_agency():
    spec = naming_spec(
        templates="{agency}_{key@subloc}_{agency_id}_{param}_{year}.csv"
    )
    meta = {
        "agency": "usbr",
        "station_id": "jer",
        "subloc": "upper",
        "agency_id": "11303500",
        "param": "flow",
        "year": "2025",
    }

    fname = meta_to_filename(meta, naming=spec)
    assert fname == "usbr_jer@upper_11303500_flow_2025.csv"
    parsed = interpret_fname(fname, naming=spec)
    assert parsed["agency"] == "usbr"
    assert parsed["station_id"] == "jer"
    assert parsed["subloc"] == "upper"
    assert parsed["agency_id"] == "11303500"
    assert parsed["param"] == "flow"
    assert parsed["year"] == "2025"



def test_meta_to_filename_omits_default_subloc_suffix():
    spec = naming_spec(
        templates="{source}_{key@subloc}_{param}_{year}.csv"
    )
    meta = {
        "source": "model",
        "station_id": "jer",
        "subloc": "default",
        "param": "ec",
        "year": "2025",
    }

    fname = meta_to_filename(meta, naming=spec)
    assert fname == "model_jer_ec_2025.csv"
    parsed = interpret_fname(fname, naming=spec)
    assert parsed["station_id"] == "jer"
    assert parsed["subloc"] is None



def test_meta_to_filename_omits_missing_modifier_suffix():
    spec = naming_spec(
        templates="{source}_{key@subloc}_{param@modifier}_{year}.csv"
    )
    meta = {
        "source": "model",
        "station_id": "jer",
        "subloc": "upper",
        "param": "ec",
        "year": "2025",
    }

    fname = meta_to_filename(meta, naming=spec)
    assert fname == "model_jer@upper_ec_2025.csv"
    parsed = interpret_fname(fname, naming=spec)
    assert parsed["param"] == "ec"
    assert "modifier" not in parsed



def test_meta_to_filename_prefers_richest_compatible_template_with_modifier():
    spec = naming_spec(
        templates=[
            "{source}_{key@subloc}_{param}_{year}.csv",
            "{source}_{key@subloc}_{param@modifier}_{year}.csv",
        ]
    )
    meta = {
        "source": "model",
        "station_id": "jer",
        "subloc": "upper",
        "param": "ec",
        "modifier": "daily",
        "year": "2025",
    }

    fname = meta_to_filename(meta, naming=spec)
    assert fname == "model_jer@upper_ec@daily_2025.csv"



def test_meta_to_filename_prefers_yearless_template_when_year_missing():
    spec = naming_spec(
        templates=[
            "{source}_{key@subloc}_{param}_{year}.csv",
            "{source}_{key@subloc}_{param}.csv",
        ]
    )
    meta = {
        "source": "model",
        "station_id": "jer",
        "subloc": "upper",
        "param": "ec",
    }

    fname = meta_to_filename(meta, naming=spec)
    assert fname == "model_jer@upper_ec.csv"



def test_raw_parse_and_formatted_render_keep_first_slot_semantics_distinct():
    raw_spec = naming_spec(
        templates="{agency}_{key@subloc}_{agency_id}_{param}_{syear}_{eyear}.csv"
    )
    formatted_spec = naming_spec(
        templates="{source}_{key@subloc}_{agency_id}_{param}_{year}.csv"
    )

    raw_meta = interpret_fname(
        "usbr_jer@upper_11303500_flow_2020_2025.csv",
        naming=raw_spec,
    )
    assert raw_meta["agency"] == "usbr"
    assert "source" not in raw_meta

    formatted_meta = {
        "source": "cdec",
        "station_id": raw_meta["station_id"],
        "subloc": raw_meta["subloc"],
        "agency_id": raw_meta["agency_id"],
        "param": raw_meta["param"],
        "year": "2025",
    }
    formatted_fname = meta_to_filename(formatted_meta, naming=formatted_spec)
    assert formatted_fname == "cdec_jer@upper_11303500_flow_2025.csv"
