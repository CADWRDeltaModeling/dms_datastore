from pathlib import Path

import pandas as pd

from dms_datastore.dropbox_data import _apply_transforms


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
        [{"name": "unit_conversion", "args": {"from_unit": "ft^3/s", "to_unit": "m^3/s"}}],
    )

    assert out["value"].tolist() == [0.028316846592000004, 0.05663369318400001]
