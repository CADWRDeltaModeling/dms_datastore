# dms_datastore Command Reference

This document contains full CLI command help pointers and workflow-based usage examples for all commands defined in `pyproject.toml` under `[project.scripts]`.

Path arguments are intentionally generic and OS-agnostic. Replace placeholders like `<raw_dir>`, `<staging_dir>`, and `<repo_dir>` with paths for your environment.

## Main Entrypoint

Use `dms` as a grouped CLI (or call commands directly by script name).

```bash
# grouped help
dms --help

# subcommand help (example)
dms download_ncro --help
```

## Help for Every Command

```bash
dms --help
download_noaa --help
download_hycom --help
download_hrrr --help
download_etss --help
download_gfs --help
download_rrfs --help
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
```

## Workflow A: Repository Build Pipeline (Download -> Reformat -> Auto Screen)

This order matches the operational flow used in `populate_tasks.bat` and core scripts.

### Stage 1: Download into raw/staging

```bash
# help (all downloaders follow this pattern)
download_noaa --help
download_nwis --help
download_des --help
download_ncro --help
```

```bash
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

# WDL (water years)
download_wdl --syear 2020 --eyear 2024 --param flow --stations orm --dest <raw_dir>

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

# WCOFS forecast products (not a station repository download)
download_wcofs wcofs --date 2026-10-13 --format netcdf --dest <wcofs_dir> --dry-run
download_wcofs wcofs --date 2021-03-23 --format netcdf --native-grid --dest <wcofs_dir> --dry-run

# RTOFS forecast products (not a station repository download)
download_rtofs --date 2026-10-14 --source nomads --forecast-steps 96:192:6 --dest <rtofs_dir> --dry-run

# ESTOFS/STOFS-2D CONUS-west water levels
download_stofs2d --date 2020-12-30 --dest <estofs_dir> --dry-run
download_stofs2d --date 2020-12-30 --dest <estofs_dir> --vdatum-grid <wcofs_vdatums.nc>
download_stofs2d --date 2023-01-08 --dest <estofs_dir> --dry-run

# UCD IPM (positional dates)
download_ucdipm 2024-01-01 2024-01-31 --stnkey 281

# CIMIS (daily, web API; requires CIMIS_API_KEY)
download_cimis --start 2024-01-01 --end 2024-01-31 --stations cms140 --dest <raw_dir>

# DCC gates
download_dcc --base-dir <dcc_raw_dir>

# Montezuma gates
download_montezuma_gates --base-dir <montezuma_raw_dir>

# SMSCG gates
download_smscg --base-dir smscg --outfile dms_smscg_gate.csv

# Mokelumne report conversion
download_mokelumne --fname mokelumne_flow.csv --raw-dir <mokelumne_raw_dir> --converted-dir <formatted_dir>
```

For historical WCOFS forecasts, `--native-grid` downloads complete ROMS
`fields` NetCDF files from `noaa-ofs-pds` into `wcofs/native`. These are
native curvilinear/sigma-grid data, not the regular-grid subsets; this mode
does not crop, interpolate, or produce hourly PST files or point CSVs. The
archive contains forecasts from 2021-03-23. Names use
`nos.wcofs.fields.fNNN.YYYYMMDD.t03z.nc` through 2024-09-16 and
`wcofs.t03z.YYYYMMDD.fields.fNNN.nc` starting 2024-09-17. Use
`--forecast-steps 3` to download just one lead; the default is 24 files
per requested cycle, approximately 336–354 MB each. Unlike regular-grid
processing, native-grid mode does not request the preceding day's cycle.
Existing files of the expected size are skipped unless `--overwrite` is set.

`download_stofs2d` uses `estofs.YYYYMMDD/estofs.*` before 2023-01-08
and `stofs_2d_glo.YYYYMMDD/stofs_2d_glo.*` on and after that date. Local
subset and processed filenames retain the `estofs` prefix for compatibility.
The t00z cycle is preferred. If no t00z CONUS-west forecast exists, the tool
logs a warning and selects the earliest available cycle, such as t06z.
Subset filenames and forecast valid times retain the chosen cycle. If multiple
cycles already exist locally, processing uses the earliest cycle for each date
and warns about the ignored cycles. Missing explicitly requested leads remain
an error; forecasts from different cycles are not combined to fill them.
It selects combined tide and surge, crops
to the RTOFS box, and writes surface-elevation-only hourly fixed-PST daily
NetCDF grids and per-run point CSVs. Processing requires a WCOFS `navd88tomsl`
datum grid: the source is treated as MSL and converted to NAVD88. Cells more
than 10 km from valid datum coverage are masked with a warning. A selected
forecast must have hourly coverage; missing hours are not interpolated across.
Use `--skip-processing` for subsets only or `--process-only` to reuse them.
For the newly registered standalone command, reinstall the package after
updating; `dms download_stofs2d` is also available.

### Stage 2: Reformat raw -> formatted

```bash
# help
reformat --help

# from populate_tasks.bat-style flow
reformat --inpath <raw_dir> --outpath <formatted_dir>

# agency-limited run
reformat --inpath <raw_dir> --outpath <formatted_dir> --agencies usgs --agencies noaa
```

### Stage 2b: USGS multivariate cleanup on formatted

```bash
# help
usgs_multi --help

# from populate_tasks.bat-style flow
usgs_multi --fpath <formatted_dir>
```

### Stage 3: Auto screen formatted -> screened

```bash
# help
auto_screen --help

# full repo-style run
auto_screen --fpath <formatted_dir> --dest <screened_dir>

# targeted run
auto_screen --fpath <formatted_dir> --dest <screened_dir> --stations sjj --params flow --plot-dest interactive
```

## Workflow B: Dropbox Ingest (separate workflow)

```bash
# help
dropbox --help

# run from YAML spec
dropbox --input dms_datastore/config_data/dropbox_spec.yaml
```

## Workflow C: Staging -> Repo update and utilities

These commands handle comparisons, planned updates, and maintenance between staging and repository directories.

```bash
# help
populate_repo --help
compare_directories --help
update_repo --help
update_flagged_data --help
```

```bash
# populate staged raw data (as used in populate_tasks.bat)
populate_repo --dest <raw_dir>

# inventory for formatted set (as used in populate_tasks.bat)
inventory --repo <formatted_dir>

# compare staging vs repo (as used in populate_tasks.bat)
compare_directories --base <repo_raw_dir> --compare <staging_raw_dir> --outfile compare_raw.txt

# plan repo reconciliation
update_repo <staging_formatted_dir> <repo_formatted_dir> --plan --out-actions update_plan.csv

# apply repo reconciliation
update_repo <staging_formatted_dir> <repo_formatted_dir> --apply

# plan screened flag-aware update
update_flagged_data <staging_screened_dir> <repo_screened_dir> --plan --out-actions flagged_plan.csv

# apply screened flag-aware update
update_flagged_data <staging_screened_dir> <repo_screened_dir> --apply
```

## Additional Utilities (with help + concrete usage)

```bash
# station lookup
station_info --help
station_info jersey
station_info --config

# delete files from list
delete_from_filelist --help
delete_from_filelist --dpath <raw_dir> --filelist files_to_delete.txt

# cache management
data_cache --help
data_cache --to-csv
data_cache --clear

# merge/splice timeseries
merge_files --help
merge_files --merge-type merge --order last --pattern "<formatted_dir>/usgs_*.csv" --pattern "<formatted_dir>/cdec_*.csv" --output merged.csv

# coarsen a CSV time series
coarsen --help
coarsen input.csv output.csv --grid 15min --qwidth 0.05 --heartbeat-freq 120min

# rationalize overlapping instrument files on a fresh raw download (DES; populate_repo runs this)
rationalize_time_partitions --help
rationalize_time_partitions "des*_*.csv" --spec des_rationalize_time_spec --root-dir <raw_dir> --dry-run
rationalize_time_partitions "des*_*.csv" --spec des_rationalize_time_spec --root-dir <raw_dir>
```
