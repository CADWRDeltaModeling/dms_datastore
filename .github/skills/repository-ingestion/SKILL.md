---
name: repository-ingestion
description: "Use when populating, updating, or rebuilding a dms_datastore repository: the download to reformat to usgs_multi to auto_screen to update_repo sequence, which CLI command belongs at each stage, and how the raw, formatted, screened, and processed tiers relate."
---

# Repository ingestion sequence

The standard pipeline moves data through four tiers: **raw → formatted → screened → processed**.

## Standard sequence

1. `populate_repo` — orchestrates downloads into the raw tier.
2. `reformat` — raw → formatted.
3. `usgs_multi` — USGS multivariate cleanup applied to the formatted tier.
4. `auto_screen` — formatted → screened, writing QA/QC results into `user_flag`.
5. `update_repo` — incremental refresh of an existing repository.

A second, more one-off ingestion path exists for arbitrary source files. See the `dropbox-ingestion` skill.

## Stage detail

### Download

Individual downloaders follow a common option pattern:

```bash
download_noaa --start 2024-01-01 --end 2024-01-31 --param water_level --stations ccc --dest <raw_dir>
download_nwis --start 2024-01-01 --end 2024-01-31 --stations sjj --param 00060 --dest <raw_dir>
download_des  --start 2024-01-01 --end 2024-01-31 --stations cll --param flow --dest <raw_dir>
download_ncro --start 2024-01-01 --end 2024-12-31 --stations orm --param elev --dest <raw_dir>
download_cdec --start 2024-01-01 --end 2024-01-31 --stations cse --param elev --dest <raw_dir>
```

Some sources differ: `download_wdl` takes `--syear/--eyear`, `download_hycom` and `download_hrrr` take `--sdate/--edate`, gate downloaders take `--base-dir`.

Full command inventory and examples: [README-commands.md](../../../README-commands.md).

### Reformat

```bash
reformat --inpath <raw_dir> --outpath <formatted_dir>
reformat --inpath <raw_dir> --outpath <formatted_dir> --agencies usgs --agencies noaa
```

### USGS multivariate cleanup

```bash
usgs_multi --fpath <formatted_dir>
```

### Auto screen

```bash
auto_screen --fpath <formatted_dir> --dest <screened_dir>
auto_screen --fpath <formatted_dir> --dest <screened_dir> --stations sjj --params flow --plot-dest interactive
```

Screening is YAML-driven via `auto_screen.py` and `screeners.py`. Results are recorded in `user_flag`, never by deleting rows.

## Rules

- Long ingestion runs use the log-and-quarantine pattern: a bad file is logged and quarantined rather than aborting the run or being silently skipped.
- Everything landing in `screened` must be regular. Fix the upstream cause of irregularity rather than disabling `force_regular` downstream.
- Structures data is legitimately irregular and lives in the `structures` repository.
- Maintenance utilities available after ingestion: `inventory`, `merge_files`, `coarsen`, `rationalize_time_partitions`, `reconcile_data`, `delete_from_filelist`, `compare_directories`, `update_flagged_data`.

On-disk naming and format rules: see the `repository-format` skill.
