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

   # UCD IPM (positional dates)
   download_ucdipm 2024-01-01 2024-01-31 --stnkey 281

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

Additional Utilities
--------------------

See the documentation for details and examples for `station_info`, `merge_files`, `coarsen`, and other utilities.