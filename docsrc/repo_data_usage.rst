****************************
Reading and Writing Data
****************************

This tutorial covers the common ways to retrieve and save time-series data with
``dms_datastore``. Repository-aware readers are preferred when data is in a
configured repository; use ``read_ts`` for explicit files.

Imports
=======

The main functions are available from the package top level::

   from dms_datastore import read_ts, read_ts_repo, read_ts_block, write_ts_csv

Read one series from a repository
=================================

Use :func:`~dms_datastore.read_multi.read_ts_repo` to select a series by station
and variable. The default repository is configured by ``dstore_config.yaml``
(typically ``screened``)::

   ec = read_ts_repo("mrz", "ec")

The result is a pandas ``DataFrame`` indexed by datetime. You can select an
inclusive time window in the call, or slice the returned data::

   ec_2023 = read_ts_repo("mrz", "ec", start="2023-01-01", end="2023-12-31")

Stations with multiple sensors can include the sublocation in the station id.
Alternatively, pass it as ``subloc=``. Variable modifiers can be included in
the variable name::

   upper_ec = read_ts_repo("mrz@upper", "ec")
   lower_ec = read_ts_repo("mrz", "ec", subloc="lower")
   daily_ec = read_ts_repo("mrz", "ec@daily")

Do not specify both ``station@subloc`` and ``subloc=``. The ``force_regular``
option defaults to ``True``; keep it enabled for regular continuous data and
fix irregular source data rather than suppressing the check.

Read an explicit file
=====================

Use :func:`~dms_datastore.read_ts.read_ts` when you already know the file path.
It detects supported time-series formats and preserves their metadata and flag
handling, unlike reading the CSV directly with pandas::

   one_file = read_ts("data/usgs_mrz_11337190_ec_2023.csv")
   shards = read_ts("data/usgs_mrz_*_ec_*.csv")

Read several series together
============================

Use :func:`~dms_datastore.read_block.read_ts_block` to read combinations of
stations and variables into one table. ``tidy`` uses a datetime/station index
and one column per variable::

   block = read_ts_block(
       station=["mrz@upper", "mrz@lower"],
       param=["ec", "temp"],
       dim="tidy",
       start="2023-01-01",
       end="2023-01-02",
   )

Use ``dim="flat"`` for a datetime index with columns such as
``mrz@upper_ec`` and ``mrz@lower_temp``. For an explicit list of station and
variable pairs, pass a CSV path using ``request=`` instead of ``station=`` and
``param=``. The command-line equivalent is ``data_block``::

   data_block -o block.csv -s 2023-01-01 -e 2023-01-02 \
     --station mrz@upper --station mrz@lower --param ec --param temp --dim tidy

Write a time series
===================

Use :func:`~dms_datastore.write_ts.write_ts_csv` to write a CSV with the
standard metadata header. Pass metadata explicitly when appropriate::

   write_ts_csv(
       ec,
       "output/mrz_ec.csv",
       metadata={"agency": "usgs", "station_id": "mrz"},
   )

Avoid ``DataFrame.to_csv`` for repository-format data because it does not
preserve the standard metadata header or dtype conventions. See
:doc:`repository` for repository file and metadata details.

Look up a station
=================

Before choosing a station id, use ``station_info`` to search the configured
registries::

   station_info Martinez
   station_info --list-registries

The Python equivalent is ``dms_datastore.station_info.station_info(search)``.