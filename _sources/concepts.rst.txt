
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
``repo_staging_auto`` tree and promoted after validation (see `Bulk Automated Download`_)::

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

See :doc:`repository` for the design of templates, registries, and provider resolution,
and `Modes of Ingestion`_ for how data enter the repositories.


Data Flow Through the Tiers
---------------------------

.. mermaid::

   flowchart LR
    subgraph providers ["Data Providers"]
      direction LR
       d1["USGS"]
       d2["DWR"]
       d3["NOAA"]
       d4["CDEC"]
       d5["USBR"]
    end
    subgraph other ["Other Sources"]
       o1["Vendor and SCADA exports, backfills, local products"]
    end
    subgraph repograph ["Repository Tiers"]
      C2["raw/"]
      D1["formatted/"]
      E1["screened/"]
      F1["processed/"]
    end
    subgraph userqaqc ["User QA/QC"]
           H["Flag Editing"]
           H-- updates user flag -->E1
    end
       providers --> B("Bulk download")
       B --> C2
       C2 --> D("Reformat")
       D --> D1
       other --> X("Dropbox recipe")
       X --> D1
       X --> F1
       D1 --> E("Automated Screening")
       E --> E1
       E1 --> F("Process")
       F --> F1
       F1 --> G["Applications & Boundary Conditions"]


User Access
-----------

The shared repository directories are read-only for most users. New data are staged in a
working or ``incoming`` area and enter the repository through one of the
`Modes of Ingestion`_ rather than by direct edits to repository files. Manual QA/QC is
recorded as ``user_flag`` values in ``screened``. Users are generally not expected to
access raw data directly; ``formatted`` data allows review of original downloaded values,
and ``screened`` data includes user flags and consistent units.


Modes of Ingestion
==================

Time series reach the repository by two routes. Both produce files with the same naming,
metadata, and units and are validated against the station registry; they differ in scale
and in how much each source is individually controlled.


Bulk Automated Download
-----------------------

The bulk route refreshes the continuous family from providers with web services (USGS,
NOAA, CDEC, DWR, USBR, and others) for the stations in the registry. ``populate_repo``
drives the source downloaders. Tiers are built in a staging tree (``repo_staging_auto``)
that alternates with the shared repository: each tier is validated in staging and
promoted before the next tier is built from it.

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


File Naming Conventions
=======================

Files follow the pattern::

   agency_dwrID[@subloc]_agencyID_variable[_YYYY[_9999]].csv

Example: ``usgs_sjj@bgc_11337190_turbidity_2016_2020.csv``

Components:

**agency**
   The agency that collects the data, potentially including a high-level program name
   (e.g., ``dwr_des``).

**dwr ID and sublocation**
   The DWR station ID, followed by ``@subloc`` if a sublocation exists (e.g.,
   ``anh@north``). The ``@`` symbol is structural.

**agency_id**
   The identifier used by the collecting agency (e.g., ``11337190`` for USGS).

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


Known Challenges and Exceptions
===============================

**WDL Station IDs**
   WDL station IDs may not match the canonical ``station_id`` due to appended ``"00"``
   or ``"Q"`` suffixes. An internal alias is used as the ``station_id`` to ensure
   uniqueness.

**SWP/CVP Exports**
   Exports are calculated differently for hourly vs. instantaneous values, producing
   distinct datasets. These different calculations are treated as "sublocations".

**USGS Multiple Instruments**
   USGS may have multiple instruments measuring the same variable for one station ID
   due to different programs or sublocations. The ``raw/`` directory can store dual
   versions for QA/QC, though the processed set should ideally be unified.
