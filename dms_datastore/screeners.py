import pandas as pd
import numpy as np
from sklearn.neighbors import LocalOutlierFactor


def detect_lof(ts, n_neighbors=5, contamination=0.01):
    # Extract values
    values = ts.values if isinstance(ts, pd.DataFrame) else ts

    # Apply Local Outlier Factor
    lof = LocalOutlierFactor(n_neighbors=n_neighbors, contamination=contamination)
    outlier_flags = lof.fit_predict(values)

    # Identify anomalies (outlier flags are -1 for outliers, 1 for inliers)
    anomalies = outlier_flags == -1

    # Return DataFrame with anomaly flags
    return pd.DataFrame({"Anomaly": anomalies}, index=ts.index)


# Example usage
# df = pd.DataFrame({'value': your_time_series_data}, index=your_datetime_index)
# anomalies_df = detect_spikes_lof(df)
