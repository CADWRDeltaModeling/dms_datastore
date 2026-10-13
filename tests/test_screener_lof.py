import pandas as pd
import numpy as np
import dms_datastore.auto_screen as auto_screen
from dms_datastore.screeners import detect_lof


def create_test_series():
    np.random.seed(0)
    dates = pd.date_range("20210101", periods=100)
    data = np.random.randn(100)
    # Introduce spikes
    data[30] = 9  # Spike
    data[70] = -8  # Spike
    return pd.DataFrame(data, index=dates, columns=["value"])


def test_detect_spikes():
    test_series = create_test_series()
    result = detect_lof(test_series, n_neighbors=5, contamination=0.02)

    assert result.loc[test_series.index[30], "Anomaly"] == True
    assert result.loc[test_series.index[70], "Anomaly"] == True
    assert result["Anomaly"].sum() == 2  # Assuming only 2 spikes


def test_max_change_test_converts_hourly_rate_to_sample_threshold(monkeypatch):
    captured = {}

    def fake_max_change_test(ts, max_change, max_plateau_len):
        captured["ts"] = ts
        captured["max_change"] = max_change
        captured["max_plateau_len"] = max_plateau_len
        return pd.Series(False, index=ts.index, name=ts.columns[0])

    monkeypatch.setattr(auto_screen, "_max_change_test", fake_max_change_test)
    ts = pd.DataFrame(
        {"value": [20.0, 18.0, 16.0]},
        index=pd.date_range("2026-01-01", periods=3, freq="15min"),
    )
    result = auto_screen.max_change_test(ts, max_rate=8.0, max_gap=2)

    assert captured["ts"] is ts
    assert captured["max_change"] == 2.0
    assert captured["max_plateau_len"] == 2
    assert not result.any()


# Run the test with pytest in the command line
# test_series = create_test_series()
# result = detect_lof(test_series, n_neighbors=5, contamination=0.02)
# ax1 = test_series.plot()
# test_series[result["Anomaly"]].plot(style="ro", ax=ax1)
# ax1.show()
