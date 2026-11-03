
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
   staging ``raw``. For DES, ``populate_repo`` then runs ``rationalize_time_partitions``
   so that overlapping instrument files do not overlap in time (see
   `Multi-instrument records`_).
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


Multi-instrument records
========================

Some providers, notably DES, publish a station's record as several raw files, one per
instrument or deployment, named with the years each covers (for example
``des_mal@upper_60_ph_2000_2008.csv`` and ``des_mal@upper_60_ph_2004_2019.csv``). These
files can overlap in time. The formatted series must use one instrument at any given
time: values from different instruments are never mixed, and one instrument's gaps
(including values the provider flagged as bad) are never filled from another.

``rationalize_time_partitions`` enforces this on the raw tier, series by series:

1. **Engulfed files are dropped.** A file whose filename year span lies within another
   file's span is deleted, without being read. An engulfed instrument is treated as a
   side deployment that the provider did not intend as the record.
2. **Overlapping files are cut, not merged.** The remaining files are ordered by their
   first valid value. Each newer file takes over at its first valid value and keeps the
   record from then on; the older file is trimmed to end just before that time. There
   is no hand-back to an older file.
3. **Explicit rules override the default.** A rule in ``des_rationalize_cfg.yaml`` can
   choose a different cut-over date, keep a short-lived instrument for a period, or hand
   back to an earlier file. If the default would need a hand-back (a later file whose
   data lie inside an earlier file's), the series is left unchanged and a warning asks
   for an explicit rule.

Rationalization rewrites and renames raw files in place, and explicit rules refer to
the file names as downloaded. Run it on a fresh download (as ``populate_repo`` does),
use ``--dry-run`` to review the planned cut-overs, and do not rerun explicit rules on
files they have already cut.


Frequency resolution in reformat
================================

DES raw files for one series are reformatted together into a single formatted series
at a **single frequency**. When the files differ (for example an hourly early record
and a 15-minute later one), ``reformat_freq_resolver_from_labels`` chooses the target
(typically the finer one) and converts the other files:

- Files already at the target frequency are copied unchanged; native values are never
  interpolated.
- A converted file is interpolated only between **consecutive valid readings**. A
  target slot next to a missing or provider-rejected reading stays empty, so
  interpolation never bridges a gap or replaces a flagged value. For example, 10-minute
  readings at 00:10 and 00:20 fill the 15-minute slot at 00:15; if 00:20 is missing,
  00:15 stays empty.
- Converted values are rounded to the source file's typical precision.

Because the whole series is resolved to one frequency, every year of a series that
contains a finer instrument is written at the finer frequency, including years that
only ever had the coarser instrument. Readers that span repository files of differing
frequency (``read_ts_repo``) raise an error unless a ``freq_resolver`` is supplied;
the same interpolation rules apply there.


How promotion reconciles staged and repository files
====================================================

``update_repo`` compares staged files with repository files **year shard by year
shard**. It is designed for nightly use: it rewrites only what changed and samples older
history rather than re-reading all of it.

**Which shards are compared.** Shards from the last ``--recent-years`` (default 3) are
always compared. Older shards are sampled: ``--p3`` (default 0.15) of shards 3 to 10
years old and ``--p10`` (default 0.05) of older ones. The sample is deterministic, so
the same old shards are chosen on every run with the same settings. A shard whose data
section is byte-identical to the repository's is skipped.

**What happens to a compared shard.**

========================================  =========================================
Comparison                                Action
========================================  =========================================
Missing in repository                     write the staged shard
Same frequency, data differ               splice: staged values win where present
Different frequency                       replace (default) or quarantine
Repository shard irregular                replace
Staged shard irregular                    quarantine the staged file, skip
========================================  =========================================

- *Splice* merges with staged priority. Where the staged shard has values, they win;
  where it has interior gaps, the gaps are kept. Repository values **before the first
  or after the last** staged value in the shard are kept.
- *Replace* writes the staged shard over the repository shard as a whole, keeping the
  repository file's header. ``--freq-mismatch replace`` is the default because a series
  has a single resolved frequency, so a staged shard at a new frequency supersedes the
  old one. ``--freq-mismatch quarantine`` leaves the repository shard unchanged instead.
- *Quarantine* copies the staged file to a ``quarantine`` directory under the current
  working directory and logs it.

**Escalation.** Two findings mean the sample is not enough and the whole series is
reconciled: a data change in a shard older than the recent window (the provider revised
history), or a frequency mismatch in any compared shard (the series' frequency
changed). Every other shard of the series is then processed, each with the same
frequency check and rules as above. A frequency change normally appears in the recent
years first, so it propagates through the whole series in one run.

**Screened tier.** ``update_flagged_data`` follows the same shard logic but merges
``user_flag`` values so that manual QA/QC decisions survive. Its ``--freq-mismatch``
default remains ``quarantine``, because replacing a screened shard discards the
repository's flags for that shard.

When to rebuild instead
-----------------------

Nightly reconciliation converges slowly for changes that affect old history broadly,
such as a change in reformat or rationalization rules. Most old shards are not
sampled, and a splice keeps repository values at the edges of a shard. After such a
change, rebuild the affected provider instead:

1. Rebuild staging (download, rationalize, reformat) with the new code.
2. Check that staging covers every series and year the repository holds for that
   provider, so nothing the provider no longer serves is lost.
3. Remove that provider's files from repository ``formatted`` (and ``screened``, if it
   carries no manual flags you need), then promote and screen again. Every shard is
   then written fresh.

Alternatively, ``update_repo --p3 1 --p10 1`` compares every shard once, but edge values
kept by splicing may remain.


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
