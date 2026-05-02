

########################
Time Series Repositories
########################

This page describes the design of the repository system for developers and advanced
users who need to build inventory tools, implement downloaders, or integrate with
registry metadata.


Core Concepts
=============

The repository system is built around four elements:

1. **Repository Configuration** — describes how a directory of files becomes structured data
2. **Filename Interpretation** — maps filenames ↔ structured metadata
3. **Registry** (``station_dbase.csv``) — authoritative station metadata
4. **Inventory & I/O** — reading, writing, and summarising datasets

These form a strict, deterministic pipeline::

   files → interpret_fname → metadata → registry join → inventory / read_ts_repo


Repository Configuration
========================

A repo configuration defines how a directory of files is interpreted as structured data.

Example ``dstore_config.yaml`` repo block:

.. code-block:: yaml

   root: //.../repo/continuous/formatted
   registry: continuous

   provider_key: source
   provider_resolution_mode: by_registry_column

   filename_templates:
     - "{source}_{station_id@subloc}_{agency_id}_{param}_{year}.csv"

   search:
     use_source_slot: true
     shard_style: auto

   parse:
     style: legacy

Required fields: ``root``, ``registry``, ``provider_key``,
``provider_resolution_mode``, ``filename_templates``. Missing any of these is an error.

The site identity column is always ``station_id`` and is not configurable.

Key terminology
---------------

``station_id``
   Universal identity column for all sites (stations, structures, synthetic locations).
   Used for registry joins, inventory grouping, and read lookups.

``provider_key``
   Data provenance (e.g., ``source``, formerly ``agency``). Used for distinguishing
   file families and applying priority ordering.

``provider_resolution_mode``
   Defines how conflicts are resolved when multiple providers supply data for the
   same station. Typical mode: ``by_registry_column``.


Source Priority Configuration
-----------------------------

The ``source_priority`` block in ``dstore_config.yaml`` specifies preferred data
sources per agency-managed station group:

.. code-block:: yaml

   source_priority:
     ncro:    ['ncro','cdec']
     dwr_ncro: ['ncro']
     des:     ['des']
     dwr_des: ['des']
     usgs:    ['usgs']
     noaa:    ['noaa']
     usbr:    ['cdec']
     dwr_om:  ['cdec']
     dwr:     ['cdec']
     ebmud:   ['usgs','ebmud','cdec']

For example, EBMUD station data is resolved by preferring USGS, then EBMUD, then CDEC.


Filename Templates and Interpretation
=====================================

Filename templates define the bidirectional mapping between metadata and filenames.

Example template::

   {source}_{station_id@subloc}_{agency_id}_{param}_{year}.csv

Given ``usgs_dsj_11313433_ec_2020.csv``, interpretation recovers::

   source:     usgs
   station_id: dsj
   subloc:     default
   agency_id:  11313433
   param:      ec
   year:       2020

Design rules:

* Parsing is structural, not heuristic.
* Templates must support both rendering and interpretation.
* A filename that matches no template is an error.
* The ``@`` in ``station_id@subloc`` is structural — ``subloc`` defaults to
  ``"default"`` when absent.


Registry (station_dbase.csv)
============================

The registry provides authoritative metadata that enriches filename-derived fields.

Flow::

   filename → parsed metadata → registry join → enriched metadata

* The registry is authoritative; filenames are operational identifiers only.
* Registry data provides spatial, descriptive, and relationship metadata.


Inventory System
================

Inventory converts repository files into structured summaries.

File inventory (``repo_file_inventory``)
----------------------------------------

Groups by ``file_pattern``. Represents:

* Physical file families
* Shard coverage (years)
* Provider-specific datasets

Data inventory (``repo_data_inventory``)
----------------------------------------

Groups by ``series_id``. Represents:

* Unique logical time series, independent of provider.

A ``series_id`` is constructed from metadata::

   [provider?] | site | subloc | param | modifier?

+-------------------+---------------+-----------------------------------+
| Inventory type    | Groups by     | Purpose                           |
+===================+===============+===================================+
| File inventory    | file_pattern  | Filesystem view                   |
+-------------------+---------------+-----------------------------------+
| Data inventory    | series_id     | Logical dataset view              |
+-------------------+---------------+-----------------------------------+


Populating the Repository
=========================

The :doc:`commands` page documents the full CLI workflow. A summary::

   populate_repo --dest <raw_dir>
   reformat --inpath <raw_dir> --outpath <formatted_dir>
   usgs_multi --fpath <formatted_dir>
   auto_screen --fpath <formatted_dir> --dest <screened_dir>


``read_ts_repo``
================

The canonical function for reading a dataset from the repository.

.. code-block:: python

   from dms_datastore.read_multi import read_ts_repo
   data = read_ts_repo("dsj", "ec", repo="formatted")

Responsibilities:

1. Resolve request → metadata search
2. Match filename patterns
3. Apply provider priority
4. Read files and merge time shards
5. Return time series

Design principles:

* **Deterministic** — same inputs always produce same outputs
* **No guessing** — no implicit provider defaults
* **No silent fallback** — fail on ambiguity

See :doc:`read_data_meta` for full parameter reference and usage examples.


``write_ts_csv``
================

Canonical writer for repository CSV files.

Metadata modes
--------------

``None``
   Creates a minimal header (format + timestamp only).

``dict`` (preferred)
   Structured metadata dict. Must include or receive ``format``. The dict is not mutated.

``string``
   Legacy/manual header, for migration purposes.

Guarantees: stable formatting, idempotent round-trip, canonical YAML header.


End-to-End Flow
===============

Read path::

   request → read_ts_repo → pattern search → interpret_fname → read_ts → merged time series

Inventory path::

   files → interpret_fname → metadata dataframe → groupby → registry join → inventory output

Write path::

   time series + metadata → write_ts_csv → prep_header → CSV file


Design Principles
=================

Fail fast
   Bad filenames → error. Bad config → error. No implicit recovery.

No hidden behavior
   No implicit defaults, no guessing providers.

Separation of concerns

+-------------------------+------------------+
| Concern                 | Component        |
+=========================+==================+
| Filename parsing        | interpret_fname  |
+-------------------------+------------------+
| Metadata enrichment     | registry         |
+-------------------------+------------------+
| Dataset lookup          | read_ts_repo     |
+-------------------------+------------------+
| File writing            | write_ts_csv     |
+-------------------------+------------------+


Architectural Evolution Notes
=============================

The repository system was refactored from an implicit agency-based model to a fully
config-driven provider model. Key terminology changes:

+------------------+------------------------+----------------------------------------+
| Old term         | New term               | Notes                                  |
+==================+========================+========================================+
| agency           | provider               | Generalizes provenance                 |
+------------------+------------------------+----------------------------------------+
| key_column       | station_id (hardcoded) | Universal identity column              |
+------------------+------------------------+----------------------------------------+
| source_priority  | provider_resolution_mode | Config-driven                        |
+------------------+------------------------+----------------------------------------+

Configs **must** define ``provider_key`` and
``provider_resolution_mode``. The site identity column (``station_id``) is
hardcoded and no longer configurable. Misconfigured repos fail immediately —
there is no fallback behavior.

The legacy ``parse.style = legacy`` option remains for backward compatibility.