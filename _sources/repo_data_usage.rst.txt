
###############################
Reading and Writing Time Series
###############################

Repository-aware readers are preferred when data is in a configured repository; use
``read_ts`` for explicit files. The main functions are available from the package top
level::

   from dms_datastore import read_ts, read_ts_repo, read_ts_block, write_ts_csv

Before choosing a station id, use ``station_info`` to search the configured registries
(see :doc:`station_info`)::

   station_info Martinez
   station_info --list-registries


Reading Common File Formats and Metadata
========================================

Use :func:`~dms_datastore.read_ts.read_ts` when you already know the file path. It
detects supported time-series formats (repository files and many provider formats) and
preserves their metadata and flag handling, unlike reading the CSV directly with pandas.
A wildcard reads time shards (``_2020.csv`` or ``_2015_2019.csv``) that share a format::

   one_file = read_ts("data/usgs_mrz_11337190_ec_2023.csv")
   shards = read_ts("data/usgs_mrz_*_ec_*.csv")

Repository-format files carry a ``#``-commented YAML header with metadata such as
station, agency, variable, and units. The data columns are ``value`` and, in screened
files, ``user_flag`` (see :doc:`concepts` for flag semantics).


Writing Compliant Files
=======================

Use :func:`~dms_datastore.write_ts.write_ts_csv` to write a CSV with the standard
metadata header::

   write_ts_csv(
       ec,
       "output/mrz_ec.csv",
       metadata={"agency": "usgs", "station_id": "mrz"},
   )

The ``metadata`` argument may be:

``None``
   Creates a minimal header (format and timestamp only).

``dict`` (preferred)
   Structured metadata. It must include or receive ``format``. The dict is not mutated.

``str``
   A legacy or manual header, for migration purposes.

Output formatting is stable, round trips are idempotent, and the YAML header is
canonical. Avoid ``DataFrame.to_csv`` for repository-format data because it does not
preserve the header or dtype conventions. To place a file in a repository, use the
:doc:`dropbox` rather than writing into repository directories.


Fetching From Repo
==================

Read one series
---------------

:func:`~dms_datastore.read_multi.read_ts_repo` selects a series by station and variable,
resolving file paths, provider priority, and time shards. The default repository is set
in ``dstore_config.yaml`` (``screened``)::

   ec = read_ts_repo("mrz", "ec")
   ec_2023 = read_ts_repo("mrz", "ec", start="2023-01-01", end="2023-12-31")

The result is a pandas ``DataFrame`` indexed by datetime. Sublocations can be given in
the station id or with ``subloc=`` (not both), and variable modifiers are part of the
variable name::

   upper_ec = read_ts_repo("mrz@upper", "ec")
   lower_ec = read_ts_repo("mrz", "ec", subloc="lower")
   daily_ec = read_ts_repo("mrz", "ec@daily")

Other repositories are chosen with ``repo=`` (a configured name or a directory), and
``meta=True`` returns metadata alongside the data. ``provider_priority`` overrides the
configured provider order. ``force_regular`` defaults to ``True``; keep it enabled for
regular continuous data and fix irregular source data rather than suppressing the check.

Lookup is deterministic and fails on ambiguity rather than guessing a provider.

Read several series together
----------------------------

:func:`~dms_datastore.read_block.read_ts_block` reads combinations of stations and
variables into one table. ``dim="tidy"`` gives a datetime/station index with one column
per variable::

   block = read_ts_block(
       station=["mrz@upper", "mrz@lower"],
       param=["ec", "temp"],
       dim="tidy",
       start="2023-01-01",
       end="2023-01-02",
   )

``dim="flat"`` gives a datetime index with columns such as ``mrz@upper_ec`` and
``mrz@lower_temp``. For an explicit list of station and variable pairs, pass a CSV path
with ``request=`` instead of ``station=`` and ``param=``. Block reads search
``repo_search_order`` (``processed`` then ``screened``). The command-line equivalent is
``data_block``::

   data_block -o block.csv -s 2023-01-01 -e 2023-01-02 \
     --station mrz@upper --station mrz@lower --param ec --param temp --dim tidy

Caching repeated reads
----------------------

For repeated access to the same data the ``@cache_dataframe`` decorator stores results::

   from dms_datastore.caching import cache_dataframe

   @cache_dataframe()
   def daily_flow(station, variable):
       return read_ts_repo(station, variable).resample("D").mean()

See the :doc:`Local Caching notebook <notebooks/cache>` for a walkthrough.
