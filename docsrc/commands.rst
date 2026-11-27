dms_datastore Command Reference
===============================

This page collects CLI command help pointers and workflow examples for the commands
exposed by the package entry points.

Main Entrypoint
---------------

Use the `dms` grouped CLI (or call commands directly by script name).

.. code-block:: bash

   # grouped help
   dms --help

   # subcommand help (example)
   dms download_ncro --help

Command Help Shortcuts
----------------------

.. code-block:: bash

   dms --help
   download_noaa --help
   download_hycom --help
   download_hrrr --help
   download_etss --help
   download_gfs --help
   download_rrfs --help
   download_rtma --help
   download_wcofs --help
   download_rtofs --help
   download_stofs2d --help
   download_cdec --help
   download_wdl --help
   download_nwis --help
   download_des --help
   download_ncro --help
   download_mokelumne --help
   download_ucdipm --help
   download_cimis --help
   download_dcc --help
   download_montezuma_gates --help
   download_smscg --help
   compare_directories --help
   populate_repo --help
   station_info --help
   reformat --help
   auto_screen --help
   inventory --help
   usgs_multi --help
   delete_from_filelist --help
   data_cache --help
   merge_files --help
   dropbox --help
   coarsen --help
   update_repo --help
   update_flagged_data --help
   rationalize_time_partitions --help

Workflow A: Repository Build Pipeline (Download → Reformat → Auto Screen)
-------------------------------------------------------------------------

Stage 1: Download into raw/staging

.. code-block:: bash

   # helper pattern used by all downloaders
   download_noaa --help
   download_nwis --help
   download_des --help
   download_ncro --help

Examples
~~~~~~~~

.. code-block:: bash

   # NOAA
   download_noaa --start 2024-01-01 --end 2024-01-31 --param water_level --stations ccc --dest <raw_dir>

   # NWIS
   download_nwis --start 2024-01-01 --end 2024-01-31 --stations sjj --param 00060 --dest <raw_dir>

   # DES
   download_des --start 2024-01-01 --end 2024-01-31 --stations cll --param flow --dest <raw_dir>

   # NCRO timeseries
   download_ncro --start 2024-01-01 --end 2024-12-31 --stations orm --param elev --dest <raw_dir>

   # NCRO inventory only
   download_ncro --inventory-only

   # CDEC
   download_cdec --start 2024-01-01 --end 2024-01-31 --stations cse --param elev --dest <raw_dir>

   # HYCOM
   download_hycom --sdate 2024-01-01 --edate 2024-01-31 --raw_dest <hycom_raw_dir> --processed_dest <hycom_processed_dir>

   # HRRR
   download_hrrr --sdate 2024-01-01 --edate 2024-01-03 --dest <hrrr_raw_dir>

   # ETSS forecast products (not a station repository download)
   download_etss --date 2026-11-18 --dest <etss_dir> --dry-run

   # GFS atmospheric forcing (not a station repository download)
   download_gfs --date 2026-10-19 --dest <gfs_dir> --dry-run

   # RRFS atmospheric forcing (not a station repository download)
   download_rrfs --date 2026-10-16 --sflux --dest <rrfs_dir> --dry-run
   download_rtma --date 2024-11-25 --dest <rtma_dir> --dry-run

   # WCOFS forecast products (not a station repository download)
   download_wcofs wcofs --date 2026-10-13 --format netcdf --dest <wcofs_dir> --dry-run

   # RTOFS forecast products (not a station repository download)
   download_rtofs --date 2026-10-14 --source nomads --forecast-steps 96:192:6 --dest <rtofs_dir> --dry-run

   # ESTOFS/STOFS-2D CONUS-west water levels
   download_stofs2d --date 2020-12-30 --dest <estofs_dir> --dry-run
   download_stofs2d --date 2020-12-30 --dest <estofs_dir> --vdatum-grid <wcofs_vdatums.nc>
   download_stofs2d --date 2023-01-08 --dest <estofs_dir> --dry-run

   # UCD IPM (positional dates)
   download_ucdipm 2024-01-01 2024-01-31 --stnkey 281

``download_stofs2d`` uses ``estofs.YYYYMMDD/estofs.*`` before 2023-01-08
and ``stofs_2d_glo.YYYYMMDD/stofs_2d_glo.*`` on and after that date. Local
subset and processed filenames retain the ``estofs`` prefix for compatibility.
The t00z cycle is preferred. If no t00z CONUS-west forecast exists, the tool
logs a warning and selects the earliest available cycle, such as t06z.
Subset filenames and forecast valid times retain the chosen cycle. If multiple
cycles already exist locally, processing uses the earliest cycle for each date
and warns about the ignored cycles. Missing explicitly requested leads remain
an error; forecasts from different cycles are not combined to fill them.
It selects combined tide and surge,
crops to the RTOFS box, and writes surface-elevation-only hourly fixed-PST
daily NetCDF grids and per-run point CSVs. Processing requires a WCOFS
``navd88tomsl`` datum grid: the source is treated as MSL and converted to
NAVD88. Cells more than 10 km from valid datum coverage are masked with a
warning. A selected forecast must have hourly coverage; missing hours are not
interpolated across. Use ``--skip-processing`` for subsets only or
``--process-only`` to reuse them.

Stage 1b: Rationalize overlapping instrument files (DES; run by ``populate_repo``)

.. code-block:: bash

   rationalize_time_partitions "des*_*.csv" --spec des_rationalize_time_spec --root-dir <raw_dir> --dry-run
   rationalize_time_partitions "des*_*.csv" --spec des_rationalize_time_spec --root-dir <raw_dir>

Run on a fresh download; it rewrites raw files in place. See
:doc:`automatic_ingestion` (Multi-instrument records).

Stage 2: Reformat raw → formatted

.. code-block:: bash

   reformat --inpath <raw_dir> --outpath <formatted_dir>

Stage 2b: USGS multivariate cleanup

.. code-block:: bash

   usgs_multi --fpath <formatted_dir>

Stage 3: Auto screen formatted → screened

.. code-block:: bash

   auto_screen --fpath <formatted_dir> --dest <screened_dir>

Workflow B: Dropbox Ingest (separate workflow)
----------------------------------------------

.. code-block:: bash

   dropbox --input dms_datastore/config_data/dropbox_spec.yaml

Workflow C: Staging → Repo update and utilities
-----------------------------------------------

.. code-block:: bash

   update_repo <staging_formatted_dir> <repo_formatted_dir> --plan --out-actions update_plan.csv
   update_repo <staging_formatted_dir> <repo_formatted_dir> --apply

Shards whose frequency differs from the repository are replaced by default
(``--freq-mismatch replace``). Which shards are compared, and when a whole series is
reconciled, is described in :doc:`automatic_ingestion`.

Additional Utilities
--------------------

See the documentation for details and examples for `station_info`, `merge_files`, `coarsen`, and other utilities.