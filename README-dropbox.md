# Dropbox Data Processing System

## Overview

The Dropbox Data Processing System is a component of the DMS Datastore package designed to facilitate the collection, transformation, and storage of time-series data. It provides a flexible configuration-based mechanism to process data files from various sources and integrate them into a standardized repository format.

## Key Components

### 1. `dropbox_data.py`

This is the main processing script that handles data collection, metadata enrichment, and storage. It reads configuration from a YAML specification file and processes data according to the defined rules.

### 2. `dropbox_spec.yaml`

This YAML configuration file defines data sources, collection parameters, and metadata specifications. It serves as the blueprint for how data should be processed.

## How It Works

The system follows these steps:

1. Reads a YAML specification file
2. For each data entry in the specification:
   - Locates source files based on patterns and locations
   - Reads time-series data
   - Augments with metadata (either directly specified or inferred)
   - Produces standardized output files in a designated location

## Usage

### Basic Usage

To process data according to the specification:

```python
from dms_datastore.dropbox_data import dropbox_data

# Process data using the specification file
dropbox_data("path/to/dropbox_spec.yaml")
```

Alternatively, you can run the script directly:

```bash
python -m dms_datastore.dropbox_data
```

### Configuration Specification

The `dropbox_spec.yaml` file has the following structure:

- `dropbox_home`: Base directory for data processing
- `dest`: Destination folder for processed files
- `data`: List of data sources to process, each with:
  - `name`: Descriptive name for the data source
  - `skip`: Optional flag to skip processing (True/False)
  - `collect`: Collection parameters including:
    - `name`: Collection method name
    - `file_pattern`: Pattern for matching files
    - `location`: Source directory path
    - `recursive_search`: Whether to search subdirectories
    - `reader`: Reading method (e.g., "read_ts")
    - `selector`: Column selector (optional)
  - `metadata`: Static metadata fields including:
    - `station_id`: Station identifier (or "infer_from_agency_id" for dynamic inference)
    - `source`: Data source name
    - `agency`: Agency name
    - `param`: Parameter type (flow, temp, etc.)
    - `sublocation`: Sub-location identifier
    - `unit`: Measurement unit
  - `metadata_infer`: Optional rules for inferring metadata from filenames:
    - `regex`: Regular expression pattern
    - `groups`: Mapping of regex groups to metadata fields

## Example Configuration

Below is an example entry from the configuration file:

```yaml
- name: USGS Aquarius flows
  skip: False
  collect: 
    name: file_search
    recursive_search: True
    file_pattern: "Discharge.ft^3_s.velq@*.EntireRecord.csv"
    location: "//cnrastore-bdo/Modeling_Data/repo_staging/dropbox/usgs_aquarius_request_2020/**"
    reader: read_ts
  metadata:
    station_id: infer_from_agency_id
    source: aquarius
    agency: usgs
    param: flow
    sublocation: default
    unit: ft^3/s
  metadata_infer:
    regex: .*@(.*)\.EntireRecord.csv
    groups:
      1: agency_id
```

## Key Classes and Functions

### DataCollector

A class that handles file discovery based on specified patterns:

```python
collector = DataCollector(name, location, file_pattern, recursive)
files = collector.data_file_list()
```

### get_spec

Loads and caches the YAML specification:

```python
spec = get_spec("dropbox_spec.yaml")
```

### populate_meta

Enriches metadata using the station database:

```python
meta_out = populate_meta(file_path, listing, metadata)
```

### infer_meta

Extracts metadata from file names based on regex patterns:

```python
metadata = infer_meta(file_path, listing)
```

## Output

Processed files are saved in the destination directory (`dest`) specified in the configuration. Each file is named according to the pattern:

```
{source}_{station_id}_{agency_id}_{param}.csv
```

Files may be chunked by year depending on the specified options.

## Additional Notes

- The system relies on a station database for lookup of station details
- Time-series data is standardized with a "value" column
- Metadata includes geospatial coordinates and projection information
- Files can be chunked by year for easier management of large datasets
