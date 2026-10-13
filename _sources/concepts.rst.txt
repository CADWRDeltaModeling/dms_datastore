
########################
Concepts and Conventions
########################

Overview
========

``dms_datastore`` retrieves time series from data providers and stores them in a common
format with standardized naming, metadata, and units. Quality control is recorded as
flags rather than deletions, and derived products are produced for applications such as
boundary conditions. Data are accessed programmatically through repository-aware readers
rather than by handling files directly.


Repositories and Tiers
======================

A *repository* is a named collection of files that share a directory root, a filename
convention, and a station registry. Repositories are declared in ``dstore_config.yaml``
(see `Repository Configuration`_) and are addressed by name in the API and command-line
tools. They are organized by data family (continuous, structures, daily) and, within a
family, by tier of refinement.


Tiers
-----

A family moves through some or all of four tiers:

**Raw**
   Data is stored exactly "as downloaded" without transformation or unit changes.
   Raw files are unique per datastream per time block.

**Formatted**
   Data adheres to file naming conventions and includes prescribed metadata.
   Units are generally not changed at this stage.

**Screened**
   Data has undergone QA/QC processes including flagging data rejected by providers
   or users. Units are standardized and consistent here.

**Processed**
   Final stage data may have been filled by algorithms and is ready for specific
   applications like boundary conditions. These files are not necessarily unique
   per datastream.


Configured Repositories
-----------------------

Each stage of each data family is a separately configured repository, addressed by name
in the API and command-line tools (e.g., ``read_ts_repo(..., repo="formatted")``).

+--------------------------+------------------------------------------------------------+
| Repository               | Contents                                                   |
+==========================+============================================================+
| ``raw``                  | Downloader output before reformatting. There is only one   |
|                          | raw repository and it lives under ``repo_staging_auto``.   |
+--------------------------+------------------------------------------------------------+
| ``formatted``            | Continuous data in the common format. The same series may  |
|                          | be present from several providers (e.g., ``ncro`` and      |
|                          | ``cdec``); the preferred one is chosen from the observing  |
|                          | agency in the registry.                                    |
+--------------------------+------------------------------------------------------------+
| ``proprietary_formatted``| Formatted continuous data from proprietary sources, held   |
|                          | apart from ``formatted``.                                  |
+--------------------------+------------------------------------------------------------+
| ``screened``             | The default repository. One version of each series, named  |
|                          | by observing agency, with user flags and standard units.   |
|                          | Always regular.                                            |
+--------------------------+------------------------------------------------------------+
| ``processed``            | Filled, transformed, or derived series, named by           |
|                          | processor. Uses the continuous registry joined with a      |
|                          | registry of synthetic locations.                           |
+--------------------------+------------------------------------------------------------+
| ``structures_formatted``,| Gate and structure operations. These are irregular         |
| ``structures_processed`` | (``regular: false``).                                      |
+--------------------------+------------------------------------------------------------+
| ``daily_formatted``,     | Data reported at a daily time step.                        |
| ``daily_screened``       |                                                            |
+--------------------------+------------------------------------------------------------+

When a request does not name a repository, ``read_ts_repo`` uses ``default_repo``
(``screened``). Block reads (``read_ts_block``) search ``repo_search_order``
(``processed`` then ``screened``) and take the first repository that has data.


Directory Layout
----------------

The shared repositories live under the configured ``repo_base``
(``//cnrastore-bdo/Modeling_Data/repo``). Bulk automated builds are staged in a parallel
``repo_staging_auto`` tree and promoted after validation (see :doc:`automatic_ingestion`)::

   //cnrastore-bdo/Modeling_Data/
     repo/
       continuous/
         formatted/
         screened/
         processed/
         proprietary/formatted/
       structures/
         formatted/
         processed/
       daily/
         formatted/
     repo_staging_auto/
       continuous/
         raw/
         formatted/
         screened/

The roots above are the configured defaults. ``dstore_config.yaml`` is authoritative.


Repository Configuration
------------------------

Repositories, registries, and supporting tables are declared in ``dstore_config.yaml``,
which ships in ``dms_datastore/config_data`` and is loaded at import. The main sections are:

``repo_base``, ``default_repo``, ``repo_search_order``
   Location of the shared repository and the repositories used when a request does
   not name one.

File resources (``station_dbase``, ``sublocations``, ``variable_mappings``, ``screen_config``, ...)
   Top-level labels naming CSV/YAML resources. ``dstore_config.config_file(label)``
   resolves the name as given (absolute, or relative to the current working directory)
   and then in the packaged ``config_data`` directory, so a same-named file in the
   working directory takes precedence over the packaged copy.

``registries``
   Named station registries (e.g., ``continuous`` → ``station_dbase.csv``,
   ``structures`` → ``structures_registry.csv``) with an optional ``column_map`` to
   canonical metadata names and ``crs`` for geographic and projected coordinates.
   A repository may join more than one registry.

``repos``
   One entry per repository giving its ``root``, ``registry``, ``filename_templates``,
   the provider slot (``provider_key``: ``source``, ``agency``, or ``processor``),
   how multiple providers are resolved (``provider_resolution_mode``:
   ``assume_unique`` or ``registry_column`` with a ``provider_resolution_order``),
   ``file_key``/``data_key`` identities, and optional ``regular``, ``float_format``,
   ``dtypes``, and ``alt`` (alternate root used when ``root`` is unavailable).

Most tools accept either a configured repository name or an explicit directory, so the
same repository semantics can be applied to a staging copy or a test directory.
Configuration can be inspected programmatically:

.. code-block:: python

   from dms_datastore import dstore_config

   dstore_config.repo_names()
   dstore_config.repo_root("screened")
   dstore_config.config_file("station_dbase")

A repository configuration missing ``root``, ``registry``, ``provider_key``,
``provider_resolution_mode``, or ``filename_templates`` is an error. The legacy
``parse.style: legacy`` option remains for backward compatibility. Filename templates are
described under `File Naming Conventions`_.


Registry
--------

The registry (``station_dbase.csv`` for continuous data) is authoritative for station
metadata: spatial, descriptive, and relationship information is joined onto the metadata
parsed from filenames. Filenames are operational identifiers only::

   filename -> parsed metadata -> registry join -> enriched metadata


Inventory
---------

Inventory converts repository files into structured summaries. The file inventory
(``repo_file_inventory``) groups by ``file_pattern`` and gives the filesystem view:
physical file families, shard coverage, and provider-specific datasets. The data
inventory (``repo_data_inventory``) groups by ``series_id`` and gives the logical view of
one unique time series independent of provider. A ``series_id`` is constructed from
``[provider?] | site | subloc | param | modifier?``.


Design Principles
-----------------

Programmers are asked to respect the naming conventions and structural rules of the repository system:

* The repository structure and naming conventions must be followed strictly. This is enforced.
* Metadata must be complete and accurate, with no missing required fields. Take the time
  to figure out whether the repo calles water temperature temp or temperature (temp).
  Take the time to figure out every file has an index datetime or date, not Date/Time with 
  special characters and capitalization. Even if you don't like the standard, having one is nice.
* Use SKILLS to develop downloaders and recipes. There is a suggested prompt in the docs for 
  a recipe.
* Minimize hidden behavior: no implicit defaults and no guessing of providers.
* Separation of concerns: ``interpret_fname`` parses filenames, the registry enriches
  metadata, ``read_ts_repo`` looks up datasets, and ``write_ts_csv`` writes files. See
  :doc:`repo_data_usage` for reading and writing.


Modes of Ingestion
==================

Time series reach the repository by two routes. Both produce files with the same naming,
metadata, and units and are validated against the station registry; they differ in scale
and in how much each source is individually controlled.


Automatic Ingestion
-------------------

The automatic route refreshes the continuous family from providers with web services
(USGS, NOAA, CDEC, DWR, USBR, and others) for the stations in the registry.
``populate_repo`` drives the source downloaders, and the tiers are built in a staging
tree and promoted one at a time after validation (download, reformat, gate, screen, gate).
See :doc:`automatic_ingestion`.


Dropbox
-------

The dropbox route handles sources that are not part of the bulk run: vendor and SCADA
exports, one-off backfills, locally processed products, and feeds that need custom
handling. A YAML recipe states how each source file is read, transformed (for example
time-zone correction, coarsening, or unit conversion), and labeled with metadata. The
dropbox writes a standardized file to a staging directory and, when the recipe requests
it, reconciles that file into a named repository (commonly ``formatted``, ``processed``,
or ``structures_formatted``) under an explicit overlap policy. Metadata and coordinates
are validated against the station registry before anything reaches the repository.
Recipes serve as the record of how each series was produced and can be rerun to top off
ongoing feeds. See :doc:`dropbox`.


Other Downloader Scripts and Data
---------------------------------

Some downloaders retrieve data that are not station time series, such as gridded
atmospheric fields from the NOAA High-Resolution Rapid Refresh model (``download_hrrr``)
and ocean model fields from HYCOM (``download_hycom``). These are written to a
destination given on the command line and are stored separately from the time-series
repository; they are not described by the repositories in ``dstore_config.yaml`` or by
the station registry.


Station, Sublocation, and Datastream Concepts
=============================================

**Station**
   A well-defined concept tied to a (location, institution) pair. Physical locations may
   vary slightly and different agencies at the same approximate location may have subtly
   different platforms. The ``station_dbase.csv`` contains station information including
   ID, agency ID, name, latitude, and longitude. These locations are corrected to fall
   near piers or piles and to be sufficiently covered by water under our elevation models.

**Sublocation**
   Used when a station ID alone does not uniquely identify a datastream — for example,
   top/bottom sensors or different programs within the same agency measuring the same
   variable. The ``station_subloc.csv`` table defines sublocations. The ``subloc``
   concept generalizes depths and other ambiguities.

**Datastream**
   Describes a single sensor and is uniquely identified by the combination of
   (station, sublocation, variable).


Agency vs Source vs Provider
============================

Three related terms describe who is behind a series. They are often the same value but
are not interchangeable.

**Agency**
   The organization that *observes* the data, recorded in the ``agency`` column of the
   station registry (e.g., ``ncro``, ``usgs``, ``dwr_om``). It can include a program
   name (``dwr_des``). It is a property of the station. ``agency_id`` is that agency's own
   identifier for the station (e.g., ``11337190`` for USGS).

**Source**
   The system that *distributes* the data and from which a file was obtained (e.g.,
   ``ncro`` for the Water Data Library, ``cdec`` for the real-time CDEC service). The
   same observed series can be available from several sources, with different
   latency and QA/QC. In ``raw`` and ``formatted`` filenames the source occupies the
   first slot (``{source}_{station_id}_...``).

**Provider**
   The generic, configuration-level term for whatever fills the first filename slot of a
   repository. Each repository sets ``provider_key`` to say which field that is:

   * ``source`` for ``formatted``, ``proprietary_formatted``, and ``daily_formatted``,
     and for the files in ``raw``;
   * ``agency`` for ``screened`` and ``atmospheric``, which hold one version of each
     series named by the observing agency;
   * ``processor`` for ``processed``, ``structures_formatted``, and
     ``structures_processed``, where the slot names the entity that produced or
     delivered the series.

How they interact: when a ``formatted`` repository holds a series from more than one
source, the registry ``agency`` selects the preferred source through
``provider_resolution_order``. For example, for a station whose agency is ``ncro`` the
order is ``ncro`` then ``cdec``, so the quality-controlled WDL version wins and CDEC fills
in where it is missing. Screening then writes a single version of the series into
``screened``, renamed with the agency in the first slot, which is why that repository
uses ``provider_resolution_mode: assume_unique``. The mapping is defined in
``dstore_config.yaml``; see `Repository Configuration`_.

Note that the agency and the source can differ, and the resolution order lists the
sources to try for each agency. For example, the order for ``usbr`` is ``usgs``,
``usbr``, then ``cdec``, and for ``ebmud`` it is ``usgs``, ``ebmud``, then ``cdec``.


File Naming Conventions
=======================

The configuration files for each repo give the expected filename pattern(s) for that repo. 
For instance the formatted continuous repositories follow this pattern::

  {source}_{station_id@subloc}_{agency_id}_{param}_{year}.csv

Each repository declares one or more ``filename_templates`` in ``dstore_config.yaml``
that define a bidirectional mapping between metadata and filenames, for example::

   {agency}_{station_id@subloc}_{agency_id}_{param}_{year}.csv

Applied to ``usgs_dsj_11313433_ec_2020.csv`` it recovers ``agency=usgs``,
``station_id=dsj``, ``subloc=default``, ``agency_id=11313433``, ``param=ec``, and
``year=2020``. Parsing is structural, not heuristic, and a filename that matches no
template is an error. Other metadata is likely stored in the file, but this is the 
part for tying the filename to identity.

Components:

**provider**
   The first slot describes the provenance of the data: ``source``, ``agency`` 
   (possibly including a program name such as
   ``dwr_des``), or ``processor``, depending on the repository. See
   `Agency vs Source vs Provider`_.

**station_id and sublocation**
   The DWR station ID, followed by ``@subloc`` if a sublocation exists (e.g.,
   ``anh@north``). The ``@`` symbol is structural, and ``subloc`` is ``default`` when
   absent. ``station_id`` is the universal identity column for all sites (stations,
   structures, synthetic locations) and is not configurable.

**agency_id**
   The identifier used by the observing agency (e.g., ``11337190`` for USGS).

**variable**
   Standardized variable name (e.g., ``turbidity``, ``temp``, ``ec@daily``).

**_YYYY_9999**
   Time shard. ``9999`` means the file is open-ended (actively updated).

Files use ``#`` for comments, ``,`` as separator, and ISO/CF compliant timestamps
(e.g., ``2009-02-10T00:00``). Metadata is included as ``key: value`` pairs in the
``#``-commented header.


Units and Standardization
=========================

* **CF Compliance**: Variable names and units are intended to be CF (Climate and
  Forecast) compliant wherever possible.
* **Stage and flow**: feet (ft) and cubic feet per second (ft^3/s) respectively. 
* **All other variables**: SI units (e.g., °C for temperature).
* **PSU exception**: Practical Salinity Unit is technically a ratio and not a true unit.
* **Specific Conductivity (EC)**: Always normalized to 25°C (µS/cm at 25°C).


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

.. mermaid::

   graph LR
       A[Raw Data] --> B{Provider Flags}
       B --(Set to NaN)--> C[Formatted Data]
       C --> D{Automated Screening}
       D --> E[User QA/QC & Manual Review]
       E --(Overrides auto flags, sets user_flag)--> F[Screened Data]
       F --> G[Processed Data]

Screening methods are listed in :doc:`automatic_ingestion`.


