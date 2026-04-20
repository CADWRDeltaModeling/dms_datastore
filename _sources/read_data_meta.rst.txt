
********************************
Reading Time Series and Metadata
********************************

Data Quality and Flags
======================

Data quality is tracked through two complementary concepts:

**Status**
   Data can be *Accepted* (flagged by provider, or with a QAQC flag indicating
   attention) or *Provisional* (from a real-time source). The system prioritizes
   data from the "provider of record" (e.g., Water Data Library – WDL) over
   real-time sources (e.g., CDEC) for accepted data, while provisional data may
   come from real-time backups.

**Quality**
   Includes *Provider quality* and *User quality*. Provider flags indicating bad
   data are honored and lead to values being set to ``NaN``. *User quality* allows
   the project's QA/QC process to signal bad data while preserving original values.
   The ``user_flag`` column in screened data marks anomalous records: ``1`` means
   anomalous, ``0`` (or ``NA``) means the anomaly was overridden by a user.


Data Quality Flow
-----------------

.. mermaid::

   graph LR
       A[Raw Data] --> B{Provider Flags}
       B --(Set to NaN)--> C[Formatted Data]
       C --> D{Automated Screening}
       D --> E[User QA/QC & Manual Review]
       E --(Overrides auto flags, sets user_flag)--> F[Screened Data]
       F --> G[Processed Data]


Data Screening Methods
======================

The :mod:`~dms_datastore.auto_screen` module performs YAML-specified screening
protocols on time series data.

Built-in screening functions
----------------------------

``dip_test(ts, low, dip)``
   Checks for anomalies based on dips below a threshold.

``repeat_test(ts, max_repeat, lower_limit=None, upper_limit=None)``
   Identifies anomalies due to values repeating more than a specified number of times.

``short_run_test(ts, small_gap_len, min_run_len)``
   Flags small clusters of valid data points surrounded by larger gaps.

Additional methods from ``vtools3``
-----------------------------------

``nrepeat(ts)``
   Returns the length of consecutive runs of repeated values.

``threshold(ts, bounds, copy=True)``
   Masks values outside specified bounds.

``bounds_test(ts, bounds)``
   Detects anomalies based on specified bounds.

``median_test(ts, ...) / med_outliers(ts, ...)``
   Detects outliers using a median filter.

``median_test_oneside(ts, ...)``
   Uses a one-sided median filter for outlier detection.

``median_test_twoside(ts, ...)``
   Similar to ``med_outliers`` but uses a two-sided median filter.

``gapdist_test_series(ts, smallgaplen=0)``
   Fills small gaps to facilitate gap analysis.

``steep_then_nan(ts, ...)``
   Identifies outliers near large data gaps.

``despike(arr, n1=2, n2=20, block=10)``
   Implements an algorithm to remove spikes from data.


Reading Data with ``read_ts_repo``
==================================

:func:`~dms_datastore.read_multi.read_ts_repo` is the primary way to access data
from the datastore. It handles file path construction, source prioritization,
and data consolidation automatically.

Basic usage
-----------

.. code-block:: python

   from dms_datastore.read_multi import read_ts_repo

   # Basic usage — retrieve data for a station and variable
   data = read_ts_repo(station_id="sjj", variable="flow")

   # With sublocation — for stations where position matters
   data = read_ts_repo(station_id="msd", variable="elev", subloc="bottom")

   # Filter to a date range after loading
   import pandas as pd
   data = read_ts_repo(station_id="mrz", variable="elev", subloc="upper").loc[
       pd.Timestamp(2018, 1, 1):pd.Timestamp(2023, 1, 1)
   ]

   # Return metadata alongside data
   data_with_meta = read_ts_repo(station_id="sjj", variable="flow", meta=True)

   # Override default source priority from config
   data = read_ts_repo(station_id="sjj", variable="flow",
                       provider_priority=["usgs", "cdec"])

   # Use a custom repository location
   data = read_ts_repo(station_id="msd", variable="elev",
                       repo="/path/to/custom/repo")

Function parameters
-------------------

``station_id``
   Station identifier as defined in the station database.

``variable``
   Standardized variable name (e.g., ``"flow"``, ``"elev"``, ``"temp"``).

``subloc``
   Optional sublocation identifier (e.g., ``"bottom"``, ``"upper"``, ``"bgc"``).

``repo``
   Optional repository path. If ``None``, uses the default from configuration.

``provider_priority``
   Source priority list. If ``"infer"``, derives from configuration based on
   station type.

``meta``
   If ``True``, returns metadata alongside the data.

``force_regular``
   Force the returned time series to have regular time intervals.


Example: retrieval and visualization
------------------------------------

.. code-block:: python

   import pandas as pd
   import matplotlib.pyplot as plt
   from dms_datastore.read_multi import read_ts_repo

   # Get flow data for San Joaquin at Jersey Point
   flow_data = read_ts_repo("sjj", "flow")

   # Filter to 2020
   start = pd.Timestamp("2020-01-01")
   end   = pd.Timestamp("2020-12-31")
   flow_period = flow_data.loc[start:end]

   plt.figure(figsize=(12, 6))
   plt.plot(flow_period.index, flow_period.values)
   plt.title("San Joaquin River Flow at Jersey Point (2020)")
   plt.xlabel("Date")
   plt.ylabel("Flow (cfs)")
   plt.tight_layout()
   plt.show()


Caching repeated reads
----------------------

For repeated access to the same data the datastore provides the
``@cache_dataframe`` decorator:

.. code-block:: python

   from dms_datastore.read_multi import read_ts_repo
   from dms_datastore.caching import cache_dataframe

   @cache_dataframe()
   def get_filtered_flow(station, variable):
       """Retrieve and process flow data with caching."""
       data = read_ts_repo(station, variable)
       data = data.interpolate(method='linear', limit=4)
       return data.resample('D').mean()

   # First call reads from repository
   daily_flow = get_filtered_flow(station="sjj", variable="flow")

   # Subsequent calls use cached data
   daily_flow = get_filtered_flow(station="sjj", variable="flow")

See also the :doc:`Local Caching notebook <notebooks/cache>` for a hands-on walkthrough.