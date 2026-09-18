import pandas as pd

from dms_datastore.download_cdec import filter_sublocation_sensor_codes
from dms_datastore.process_station_variable import attach_subloc


def test_filter_sublocation_sensor_codes_keeps_global_lower_sensor_codes():
    stations = pd.DataFrame(
        {
            "subloc": [
                "default",
                "default",
                "default",
                "lower",
                "lower",
                "lower",
                "lower",
            ],
            "src_var_id": ["100", "296", "25", "100", "295", "296", "297"],
        }
    )

    actual = filter_sublocation_sensor_codes(stations)

    assert actual[["subloc", "src_var_id"]].to_dict("records") == [
        {"subloc": "default", "src_var_id": "100"},
        {"subloc": "default", "src_var_id": "25"},
        {"subloc": "lower", "src_var_id": "295"},
        {"subloc": "lower", "src_var_id": "296"},
        {"subloc": "lower", "src_var_id": "297"},
    ]


def test_attach_subloc_expands_explicit_default_and_lower_entries():
    request = pd.DataFrame({"station_id": ["c31"], "param": ["ec"]})
    lookup = pd.DataFrame(
        {
            "station_id": ["c31", "c31"],
            "subloc": ["default", "lower"],
        }
    )

    actual = attach_subloc(request, subloc_lookup=lookup)

    assert actual.to_dict("records") == [
        {"station_id": "c31", "param": "ec", "subloc": "default"},
        {"station_id": "c31", "param": "ec", "subloc": "lower"},
    ]