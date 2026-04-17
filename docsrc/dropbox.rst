Dropbox Data Processing System
==============================

Overview
--------

The Dropbox Data Processing System is a component of the DMS Datastore package that
facilitates collection, transformation, and storage of time-series data using a
configuration-driven workflow.

Key Components
--------------

1. ``dropbox_data.py``

   Main processing script that reads a YAML specification file and processes
   data according to the defined rules.

2. ``dropbox_spec.yaml``

   YAML configuration that defines data sources, collection parameters, and
   metadata inference rules.

How it works
-----------

The processing follows these steps:

1. Read a YAML specification.
2. For each entry: locate files, read time-series, augment with metadata,
   and write standardized output files.

Usage
-----

Basic usage from Python::

   from dms_datastore.dropbox_data import dropbox_data
   dropbox_data("path/to/dropbox_spec.yaml")

Or run as a module::

   python -m dms_datastore.dropbox_data

Configuration Specification
---------------------------

Typical keys in the spec include ``dropbox_home``, ``dest``, and a ``data`` list
with entries containing ``collect`` and ``metadata``. The spec also supports
``metadata_infer`` rules using regular expressions.

Example configuration snippet::

   - name: USGS Aquarius flows
     skip: False
     collect:
       name: file_search
       recursive_search: True
       file_pattern: "Discharge.ft^3_s.velq@*.EntireRecord.csv"
       location: "//cnrastore-bdo/.../dropbox/usgs_aquarius_request_2020/**"
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

Key Classes and Functions
-------------------------

- ``DataCollector`` — handles file discovery based on patterns.
- ``get_spec`` — loads and caches the YAML spec.
- ``populate_meta`` — enriches metadata using the station database.
- ``infer_meta`` — extracts metadata from filenames via regex.

Output
------

Processed files are saved in the destination directory. Filenames follow the
pattern ``{source}_{station_id}_{agency_id}_{param}.csv`` and may be chunked by year.

Additional Notes
----------------

- Relies on a station database for lookups.
- Standardizes time-series to include a `value` column and metadata headers.
- Files may be year-sharded for easier management.
