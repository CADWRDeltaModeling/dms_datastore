"""Frequency resolution must change sampling interval only, never gap-fill."""

import numpy as np
import pandas as pd

from dms_datastore.read_multi import _apply_freq_resolution, _transform_interp


def _ts(start, periods, freq, values):
    idx = pd.date_range(start, periods=periods, freq=freq)
    return pd.DataFrame({"value": values}, index=idx)


def test_interp_fills_between_consecutive_samples():
    ts = _ts("2024-01-01", 4, "10min", [1.25, 2.25, 3.25, 4.25])
    out = _transform_interp(ts, "15min")
    assert out.loc["2024-01-01 00:15", "value"] == 2.75  # between 00:10 and 00:20
    assert out.loc["2024-01-01 00:30", "value"] == 4.25


def test_interp_does_not_bridge_missing_or_flagged_sample():
    # 00:20 rejected by provider (NaN): neither 00:15 nor 00:30 may be invented
    ts = _ts("2024-01-01", 5, "10min", [1.25, 2.25, np.nan, 4.25, 5.25])
    out = _transform_interp(ts, "15min")
    assert out.loc["2024-01-01 00:00", "value"] == 1.25
    assert np.isnan(out.loc["2024-01-01 00:15", "value"])
    assert out.loc["2024-01-01 00:30", "value"] == 4.25  # native sample, not interpolated


def test_interp_hourly_single_missing_hour_stays_nan():
    ts = _ts("2024-01-01", 4, "h", [1.5, np.nan, 3.5, 4.5])
    out = _transform_interp(ts, "15min")
    assert out.loc["2024-01-01 00:00":"2024-01-01 01:45", "value"].iloc[1:].isna().all()
    assert abs(out.loc["2024-01-01 02:15", "value"] - 3.75) <= 0.05  # rounded to source precision


def test_resolution_leaves_native_target_freq_untouched():
    hourly = _ts("2024-01-01", 4, "h", [1.5, 2.5, 3.5, 4.5])
    fine = _ts("2024-01-02", 4, "15min", [1.5, np.nan, np.nan, 4.5])
    items = [
        {"ts": hourly, "freq_label": "h"},
        {"ts": fine, "freq_label": "15min"},
    ]
    resolved, target = _apply_freq_resolution(items, "interp_to_finer")
    assert target == pd.tseries.frequencies.to_offset("15min")
    assert resolved[1]["ts"] is fine
    assert resolved[1]["ts"]["value"].isna().sum() == 2
    assert resolved[0]["freq_label"] == "15min"
