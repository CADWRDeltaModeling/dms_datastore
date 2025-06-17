# dms_datastore

Delta Modeling Section Datastore provides tools for downloading and managing continuous data. This repository is a work in progress. 

## Table of Contents
- [Overview](#overview)
- [Data Repository Structure](#data-repository-structure)
- [Data Quality and Flags](#data-quality-and-flags)
- [Data Screening and Error Detection Methods](#data-screening-and-error-detection-methods)
- [Metadata and Station Concepts](#metadata-and-station-concepts)
- [File Naming Conventions](#file-naming-conventions)
- [Units and Standardization](#units-and-standardization)
- [Data Fetching and Priority](#data-fetching-and-priority)
- [Challenges and Exceptions](#challenges-and-exceptions)
- [Installation](#installation)

## Overview

The overarching goal of this data organization effort is to retrieve data from data providers and store in a common format, validate data (screened) and for use in applications such as boundary conditions (filled/aggregated or derived data), which is referred to as "processed" data. The system aims to move away from manually manipulated data in favor of standardized formats for programmatic access.

## Data Repository Structure

The centralized data repository is housed in a file system-based share at 

`<internal shared directory server>\Modeling_Data\continuous_station_repo`. 

A mirrored copy is available at http://tinyurl.com/dmsdatastore.

The system processes data through distinct stages:

1. **Raw**: Data is stored exactly "as downloaded" without transformation or unit changes. This includes data fetched from various sources in their original formats, even if proprietary or unusual. Raw files are unique per datastream per time block.

2. **Formatted**: Data in this stage adheres to file naming conventions and includes prescribed metadata. While the original intention for unit conversion at this stage was questioned, it's noted that units were not typically changed here.

3. **Screened**: This stage incorporates data that has undergone quality assurance and quality control (QA/QC) processes, including flagging data rejected by providers or users. At this stage, units are standardized and consistent across data.

4. **Processed**: This represents the final stage where data may have been filled by algorithms and is ready for specific applications like boundary conditions, where smoothing or no missing values are required. These files are not necessarily unique in the same way as raw or screened data.

### Data Repository Workflow

```mermaid
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

```

### User Access

Users typically have write access only to "incoming" subdirectories within `raw`, `screened`, and `processed` directories, with submissions formatted correctly then being ingested into the main, read-only directories. Users are generally not expected to access raw data directly; `formatted` data allows review of original downloaded values, and `screened` data includes user flags and consistent units.

## Data Quality and Flags

Data quality is tracked through several concepts:

- **Status**: Data can be "Accepted" (flagged by provider or with a QAQC flag indicating attention) or "Provisional" (from a real-time source). The system prioritizes data from the "provider of record" (e.g., Water Data Library - WDL) over real-time sources (e.g., CDEC) for "accepted" data, while provisional data may come from real-time backups.

- **Quality**: This includes "Provider quality" and "User quality". Provider flags indicating bad data are honored and lead to values being set to NaN (Not a Number). "User quality" allows the project's QA/QC process to signal bad data while respecting original values. A `user_flag` column in screened data indicates anomalous data, where `1` means anomalous and `0` (or `NA`) means the anomaly was overridden by a user.

### Data Quality Flow

```mermaid
graph LR
    A[Raw Data] --> B{Provider Flags}
    B --(Set to NaN)--> C[Formatted Data]
    C --> D{Automated Screening}
    D --> E[User QA/QC & Manual Review]
    E --(Overrides auto flags, sets user_flag)--> F[Screened Data]
    F --> G[Processed Data]
```

## Data Screening and Error Detection Methods

The `auto_screen.py` module in `dms_datastore` performs YAML-specified screening protocols on time series data. Key screening methods include:

- **dip_test(ts, low, dip)**: Checks for anomalies based on dips below a threshold.
- **repeat_test(ts, max_repeat, lower_limit=None, upper_limit=None)**: Identifies anomalies due to values repeating more than a specified number of times.
- **short_run_test(ts, small_gap_len, min_run_len)**: Flags small clusters of valid data points surrounded by larger gaps as anomalies.

Additional error detection methods from `vtools3` include:

- **nrepeat(ts)**: Returns the length of consecutive runs of repeated values.
- **threshold(ts, bounds, copy=True)**: Masks values outside specified bounds.
- **bounds_test(ts, bounds)**: Detects anomalies based on specified bounds.
- **median_test(ts, ...) / med_outliers(ts, ...)**: Detects outliers using a median filter.
- **median_test_oneside(ts, ...)**: Uses a one-sided median filter for outlier detection.
- **median_test_twoside(ts, ...)**: Similar to `med_outliers` but uses a two-sided median filter.
- **gapdist_test_series(ts, smallgaplen=0)**: Fills small gaps to facilitate gap analysis.
- **steep_then_nan(ts, ...)**: Identifies outliers near large data gaps.
- **despike(arr, n1=2, n2=20, block=10)**: Implements an algorithm to remove spikes from data.

## Metadata and Station Concepts

The system defines clear concepts for organizing time series data:

- **Station**: A well-defined concept tied to a (location, institution) pair, acknowledging that physical locations may vary slightly and different agencies at the same approximate location may have subtly different platforms. The `station_dbase.csv` contains station information like ID, agency ID, name, latitude, and longitude. These locations are corrected to fit the SCHISM mesh.

- **Sublocation**: Used when a "station" doesn't uniquely describe a datastream, such as top/bottom sensors or different programs within the same agency measuring the same variable. The `station_subloc_new.csv` table is used to define sublocations. The `subloc` concept generalizes depths and other ambiguities.

- **Datastream**: This term describes a single sensor and is uniquely identified by the combination of (station, sublocation, variable).

## File Naming Conventions

A simplified file naming convention is used for data files:
`agency_dwrID@subloc_agencyID_variable_YYYY_9999.csv`

For example: `usgs_sjj@bgc_11337190_turbidity_2016_2020.csv`

Components include:

- **agency**: The agency that collects the data, potentially including a high-level program name (e.g., `dwr_des`).
- **dwr id and sublocation**: The DWR ID from `stations_utm.csv` and the `subloc` from `station_subloc_new.csv`, separated by an `@` sign if a sublocation exists.
- **agency_id**: The identifier used by the agency (e.g., `11337190` for USGS).
- **variable**: The variable name using the project's standardized naming convention (e.g., `turbidity`, `temp`).
- **_YYYY_9999**: Indicates the time shard, with `9999` representing "until now".

File formats use `#` for comments, `,` as a separator, and ISO/CF compliant timestamps (e.g., `2009-02-10T00:00`). Metadata is included as key-value pairs in the header.

## Units and Standardization

The system aims for standardization of variables and units:

- **CF Compliance**: Variable names and units are intended to be CF (Climate and Forecast) compliant wherever possible.
- **Unit Handling**: The "screened" data should have consistent units. The standard practice for stage and flow is feet and cubic feet per second, respectively, while SI units are used for everything else like temperature.
- **PSU Exception**: Practical Salinity Unit (PSU) is noted as an exception, as it's technically a ratio and not a true unit.
- **Specific Conductivity (EC)**: This is the standard way salinity data is collected and is always normalized to 25°C.

## Data Fetching and Priority

Data is fetched through download scripts (`download_noaa`, `download_cdec`, `download_nwis`, `download_des.py`). The `auto_screen` process uses `custom_fetcher` functions to retrieve data.

The system handles cases where data for the same station comes from different sources. The `src_priority` mechanism in `read_ts_repo` ensures that data from higher-priority sources is preferred.

## Challenges and Exceptions

Several challenges and workarounds are identified:

- **WDL Station IDs**: WDL station IDs may not have the same `station_id` due to appended "00" or "Q". The solution is to use an internal alias as the `station_id` to ensure uniqueness.
- **SWP/CVP Exports**: These exports are calculated differently for hourly and instantaneous values, leading to them being distinct data sets. The solution is to treat these different calculations as "sublocations".
- **USGS Multiple Instruments**: USGS may have multiple instruments measuring the same variable for one `station_id` due to different programs or sublocations. The `/raw` directory can store these dual versions for QA/QC, though the "processed" set should ideally be unified.

## Installation

```bash
git clone https://github.com/CADWRDeltaModeling/dms_datastore
conda env create -f environment.yml # should create a dms_datastore and pip install the package
# alternatively, pip install -e . after running the above command if you want to develop the package
conda activate dms_datastore
```
