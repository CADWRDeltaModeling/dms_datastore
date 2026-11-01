import pandas as pd
import pytest
from vtools import dst_st, ts_coarsen
from vtools.functions.unit_conversions import convert_units

from dms_datastore.dropbox_data import (
    _TRANSFORMS, _apply_transforms, _get_transforms, reader_for, register_transform,
)


def test_add_column_transform_adds_missing_string_column() -> None:
    idx = pd.date_range("2024-01-01", periods=2, freq="D")
    ts = pd.DataFrame({"value": [1.0, 2.0]}, index=idx)

    out = _apply_transforms(
        ts,
        [
            {
                "name": "add_column",
                "args": {
                    "name": "user_remarks",
                    "dtype": "string",
                    "default": None,
                },
            }
        ],
    )

    assert list(out.columns) == ["value", "user_remarks"]
    assert out["user_remarks"].dtype.name == "string"
    assert out["user_remarks"].isna().all()


def test_unit_conversion_transform_converts_flow_units() -> None:
    idx = pd.date_range("2024-01-01", periods=2, freq="D")
    ts = pd.DataFrame({"value": [1.0, 2.0]}, index=idx)

    out = _apply_transforms(
        ts,
        [{"name": "unit_conversion", "args": {"in_unit": "ft^3/s", "out_unit": "m^3/s"}}],
    )

    assert out["value"].tolist() == [0.028316846592000004, 0.05663369318400001]


def test_vtools_transforms_are_registered_directly():
    transforms = _get_transforms()
    assert transforms["convert_units"] is convert_units
    assert transforms["unit_conversion"] is convert_units
    assert transforms["coarsen"] is ts_coarsen
    assert transforms["dst_tz"] is dst_st
    assert transforms["dst_st"] is dst_st


def test_custom_transform_accepts_a_vtools_callable_and_multiple_columns(monkeypatch):
    monkeypatch.setattr(
        "dms_datastore.dropbox_data._TRANSFORMS", dict(_TRANSFORMS)
    )
    register_transform("custom_conversion", convert_units)
    data = pd.DataFrame(
        {"flow": [1.0, 2.0], "other_flow": [3.0, 4.0]},
        index=pd.date_range("2025-01-01", periods=2, freq="h"),
    )
    result = _apply_transforms(
        data,
        [{"name": "custom_conversion", "args": {"in_unit": "ft^3/s", "out_unit": "m^3/s"}}],
    )
    pd.testing.assert_frame_equal(result, data * 0.028316846592000004)


@pytest.mark.parametrize("name", ["unit_conversion", "coarsen", "dst_tz"])
def test_direct_transforms_reject_unknown_arguments(name):
    with pytest.raises(TypeError, match="unexpected"):
        _apply_transforms(pd.DataFrame(), [{"name": name, "args": {"typo": True}}])


def test_registration_requires_explicit_override_for_discovered_name(monkeypatch):
    monkeypatch.setattr("dms_datastore.dropbox_data._TRANSFORMS", dict(_TRANSFORMS))
    with pytest.raises(ValueError, match="already registered"):
        register_transform("convert_units", abs)
    register_transform("convert_units", abs, override=True)
    assert _get_transforms()["convert_units"] is abs
    assert _get_transforms()["unit_conversion"] is abs


def test_discovers_reader_without_a_local_registration(monkeypatch):
    from importlib.metadata import EntryPoint

    monkeypatch.setattr("vtools.plugins.entry_points", lambda: {
        "timeseries.readers": [
            EntryPoint("external_reader", "pandas:read_csv", "timeseries.readers")
        ]
    })
    assert reader_for("external_reader") is pd.read_csv


def test_discovers_transform_without_a_local_registration(monkeypatch):
    from importlib.metadata import EntryPoint

    import vtools.plugins

    original = vtools.plugins.entry_points

    def advertised():
        entries = original()
        transforms = list(entries.select(group="timeseries.transforms"))
        transforms.append(EntryPoint("external_abs", "builtins:abs", "timeseries.transforms"))
        return {"timeseries.transforms": transforms}

    monkeypatch.setattr(vtools.plugins, "entry_points", advertised)
    data = pd.Series([-1.0, 2.0], name="flow")
    pd.testing.assert_series_equal(_apply_transforms(data, ["external_abs"]), data.abs())
