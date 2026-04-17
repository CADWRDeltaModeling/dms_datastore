
############################
Station Database and Queries
############################

Station Lookup
==============

The ``station_info`` command lets you search for stations by name fragment or ID::

   $ station_info francisco

See :doc:`commands` for the full CLI reference.


Configuration System
=====================

The datastore uses YAML files and Python modules to manage station metadata,
variable mappings, and screening configurations.

Main configuration files
------------------------

``dstore_config.yaml``
   The central configuration file. Defines paths to station databases, repository
   locations, source priorities, and screening configurations.

``dstore_config.py``
   Python module that reads the YAML configuration and exposes helper functions.


Key data files
--------------

``station_dbase.csv``
   Master database of all stations. Key columns:

   * ``id`` — internal unique identifier
   * ``agency_id`` — ID used by the collecting agency
   * ``name`` — descriptive station name
   * ``lat``, ``lon`` — geographic coordinates
   * ``x``, ``y`` — projected coordinates (SCHISM mesh-corrected)

``variable_mappings.csv``
   Maps agency-specific variable codes/names to standardized variable names used
   within the datastore.

``variable_definitions.csv``
   Defines standard variables with their units and descriptive information.

``station_subloc.csv``
   Defines sublocations (e.g., depths, sensor positions) for stations where the
   station ID alone is insufficient to identify a unique datastream.


Configuration API
-----------------

The :mod:`~dms_datastore.dstore_config` module exposes these functions:

``station_dbase()``
   Returns the station database as a :class:`pandas.DataFrame`.

``sublocation_df()``
   Returns the sublocations table.

``configuration()``
   Returns the full configuration dictionary.

``config_file(label)``
   Returns the path to a named configuration file. Checks the current working
   directory first, then the built-in ``config_data/`` package directory.

All functions cache their results to avoid repeated filesystem reads.


Screen Configuration
---------------------

The screening configuration YAML (referenced by ``screen_config`` in
``dstore_config.yaml``) drives :mod:`~dms_datastore.auto_screen` and contains
rule sets for:

* **Bounds checking** — acceptable min/max values per variable
* **Spike detection** — parameters for flagging data spikes
* **Repetition checking** — rules for flagging suspicious repeated values
* **Custom screening functions** — advanced algorithms for specific data types
