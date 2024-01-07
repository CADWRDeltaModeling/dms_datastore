import pandas as pd
import numpy as np
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


# Run the test with pytest in the command line
# test_series = create_test_series()
# result = detect_lof(test_series, n_neighbors=5, contamination=0.02)
# ax1 = test_series.plot()
# test_series[result["Anomaly"]].plot(style="ro", ax=ax1)
# ax1.show()
