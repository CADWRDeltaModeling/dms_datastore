
#######################
Automatic Ingestion
#######################

The automatic route refreshes the continuous family from providers with web services
(USGS, NOAA, CDEC, DWR, USBR, and others) for the stations in the registry.
``populate_repo`` drives the source downloaders. Tiers are built in a staging tree
(``repo_staging_auto``) that alternates with the shared repository: each tier is
validated in staging and promoted before the next tier is built from it. Sources that
are not covered by web services enter through the :doc:`dropbox` instead.


Pipeline
========

.. mermaid::

   sequenceDiagram
       participant AWS as Agency Web Services
       participant SR as Staging Raw
       participant SF as Staging Formatted
       participant RF as Repo Formatted
       participant SS as Staging Screened
       participant RS as Repo Screened

       AWS->>SR: populate_repo / download_*
       SR->>SF: reformat
       SF->>SF: usgs_multi
       Note over SF,RF: spot_check --group continuous_formatted
       SF->>RF: update_repo
       RF->>SS: auto_screen
       Note over SS,RS: spot_check --group continuous_screened
       SS->>RS: update_flagged_data

1. **Download.** ``populate_repo`` or an individual downloader writes provider files to
   staging ``raw``.
2. **Format.** ``reformat`` converts staging ``raw`` into staging ``formatted``;
   ``usgs_multi`` then splits or consolidates USGS multi-sensor files in place.
3. **Gate and promote formatted.** ``spot_check`` compares the staged formatted set
   against the repository baseline (expected streams, file counts, and filename/registry
   integrity) and fails if the build collapsed or regressed. ``update_repo`` then
   reconciles the accepted staged files into repository ``formatted``.
4. **Screen from the repository.** ``auto_screen`` reads repository ``formatted`` (its
   default ``--repo``), not staging, so screening sees the full promoted history rather
   than only the recent download. Output goes to staging ``screened`` with screened
   naming (``--output-naming screened``) and results recorded in ``user_flag``.
5. **Gate and promote screened.** ``spot_check`` checks staging ``screened``, and
   ``update_flagged_data`` merges it into repository ``screened``. Because the repository
   copy carries user flag edits, explicit flag conflicts default to the repository side
   (``--flag-conflict prefer_repo``), so a rebuild does not erase manual QA/QC.

Both promotion commands support ``--plan`` (with ``--out-actions``) for a dry run before
``--apply``. ``compare_directories`` and ``inventory`` can be used to review remaining
differences between staging and repository. When a gate fails, the staged data or its
upstream cause is corrected and the step rerun; drift is not accepted into the
repository silently. Gate definitions live in ``spot_check_spec.yaml``. Command
examples are in :doc:`commands`.


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
