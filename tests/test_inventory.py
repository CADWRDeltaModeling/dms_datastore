import pandas as pd
from dms_datastore import inventory


def test_to_wildcard_single_year():
    got = inventory.to_wildcard("usgs_anh@north_11303500_flow_2024.csv", remove_provider=False)
    assert got == "usgs_anh@north_11303500_flow_*.csv"


def test_to_wildcard_remove_provider():
    got = inventory.to_wildcard(
        "usgs_anh@north_11303500_flow_2024.csv",
        remove_provider=True,
    )
    assert got == "*_anh@north_11303500_flow_*.csv"


def test_series_id_from_meta_file_level():
    meta = {
        "agency": "usgs",
        "station_id": "anh",
        "subloc": "north",
        "param": "flow",
    }
    got = inventory.series_id_from_meta(meta, remove_provider=False)
    assert got == "usgs|anh|north|flow"


def test_series_id_from_meta_data_level():
    meta = {
        "agency": "usgs",
        "station_id": "anh",
        "subloc": "north",
        "param": "flow",
    }
    got = inventory.series_id_from_meta(meta, remove_provider=True)
    assert got == "anh|north|flow"
    
    