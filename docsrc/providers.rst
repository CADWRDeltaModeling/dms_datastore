Provider Quirks
===============

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
