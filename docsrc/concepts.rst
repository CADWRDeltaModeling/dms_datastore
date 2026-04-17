
########################
Concepts and Conventions
########################

Overview
========

The overarching goal of this data organization effort is to retrieve data from data
providers and store it in a common format, validate data (screened), and produce data
suitable for applications such as boundary conditions (filled/aggregated or derived data),
referred to as "processed" data. The system moves away from manually manipulated data
in favor of standardized formats for programmatic access.


Data Repository Structure
=========================

The centralized data repository is housed in a file system-based share at
``<internal shared directory server>\\Modeling_Data\\continuous_station_repo``.
A mirrored copy is available at http://tinyurl.com/dmsdatastore.

Data flows through four distinct stages:

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


Data Repository Workflow
------------------------

.. mermaid::

   flowchart LR
    subgraph datasources ["Data Sources"]
      direction LR
       d1["USGS"]
       d2["DWR"]
       d3["NOAA"]
       d4["CDEC"]
       d5["USBr"]
    end
    subgraph dropbox ["Drop Box"]
    end
    subgraph repograph ["Repository"]
      C2["raw/"]
      D1["formatted/"]
      E1["screened/"]
      F1["processed/"]
    end
    subgraph userqaqc ["User QA/QC"]
           H["Flag Editing"]
           H-- updates user flag -->E1
    end
       datasources --> B("Download")
       B --> C2
       C2 --> D("Format")
       D --> D1
       D1 --> E("Automated Screening")
       E --> E1
       E1 --> F("Process")
       F --> F1
       F1 --> G["Modeling Applications & Boundary Conditions"]

       dropbox --> repograph


User Access
-----------

Users typically have write access only to "incoming" subdirectories within ``raw``,
``screened``, and ``processed`` directories. Correctly formatted submissions are then
ingested into the main, read-only directories. Users are generally not expected to access
raw data directly; ``formatted`` data allows review of original downloaded values, and
``screened`` data includes user flags and consistent units.


Station, Sublocation, and Datastream Concepts
=============================================

**Station**
   A well-defined concept tied to a (location, institution) pair. Physical locations may
   vary slightly and different agencies at the same approximate location may have subtly
   different platforms. The ``station_dbase.csv`` contains station information including
   ID, agency ID, name, latitude, and longitude. These locations are corrected to fit
   the SCHISM mesh.

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
* **Stage and flow**: feet (ft) and cubic feet per second (cfs) respectively.
* **All other variables**: SI units (e.g., °C for temperature).
* **PSU exception**: Practical Salinity Unit is technically a ratio and not a true unit.
* **Specific Conductivity (EC)**: Always normalized to 25°C (µS/cm at 25°C).


Known Challenges and Exceptions
================================

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

