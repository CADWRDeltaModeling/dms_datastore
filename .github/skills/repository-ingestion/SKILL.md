---
name: repository-ingestion
description: "Use when populating, updating, or rebuilding a dms_datastore repository: the staging-to-repo zigzag, the download -> reformat -> screen flow, the spot_check validation gate, and how the raw, formatted, screened, and processed tiers relate."
---

# Repository ingestion sequence

The standard pipeline moves data through four tiers: **raw → formatted → screened → processed**. The operational pattern is a staging-to-repo zigzag: data lands in `repo_staging_auto`, is validated there, and then is promoted into the canonical repo with `update_repo` or `update_flagged_data`.

## Standard sequence

1. `populate_repo` or a source-specific downloader writes to the raw staging tier.
2. `reformat` promotes raw staging into formatted staging.
3. `spot_check --config spot_check_spec --group continuous_formatted` validates the staged formatted set against the repo baseline before promotion.
4. `update_repo formatted <repo>/formatted --apply` moves the accepted staged files into the repo.
5. `usgs_multi` may be run on the formatted tier before repo promotion.
6. `auto_screen` writes screened outputs in the staged area and records QA/QC results via `user_flag`.
7. `update_flagged_data screened <repo>/screened --apply` promotes the accepted screened files into the repo.
8. `inventory` and the comparison utilities confirm the staged and repo state remain aligned.

A second, more one-off ingestion path exists for arbitrary source files. See the `dropbox-ingestion` skill.

## Stage detail

### Download

Individual downloaders follow a common option pattern:

```bash
download_noaa --start 2024-01-01 --end 2024-01-31 --param water_level --stations ccc --dest <raw_dir>
download_nwis --start 2024-01-01 --end 2024-01-31 --stations sjj --param 00060 --dest <raw_dir>
download_ncro --start 2024-01-01 --end 2024-12-31 --stations orm --param elev --dest <raw_dir>
download_cdec --start 2024-01-01 --end 2024-01-31 --stations cse --param elev --dest <raw_dir>
download_cimis --start 2024-01-01 --end 2024-01-31 --stations ccc --dest <raw_dir>
```

Some sources differ: `download_wdl` takes `--syear/--eyear`, `download_hycom` and `download_hrrr` take `--sdate/--edate`, gate downloaders take `--base-dir`.

Full command inventory and examples: [README-commands.md](../../../README-commands.md).

### Reformat and spot-check

```bash
reformat --inpath <raw_dir> --outpath <formatted_dir>
spot_check --config spot_check_spec --group continuous_formatted
```

This is the staging validation gate. It compares the repo baseline to the just-built staged set and fails early when the expected logical streams do not appear in staging.

### USGS multivariate cleanup

```bash
usgs_multi --fpath <formatted_dir>
```

### Auto screen and spot-check

```bash
auto_screen --fpath <formatted_dir> --dest <screened_dir>
auto_screen --fpath <formatted_dir> --dest <screened_dir> --stations sjj --params flow --plot-dest interactive
spot_check --config spot_check_spec --group continuous_screened
```

Screening is YAML-driven via `auto_screen.py` and `screeners.py`. Results are recorded in `user_flag`, never by deleting rows.

## Zigzag from staging to repo

The key pattern is not a one-way write. It is a staged validation loop:

1. Build data under `repo_staging_auto/...`.
2. Validate the staged set with `spot_check`.
3. Promote with `update_repo` or `update_flagged_data`.
4. Compare the staged result to the repo with `compare_directories` and `inventory`.
5. If the repo check fails, correct the staged data rather than silently accepting drift.

This keeps the canonical repo clean while still allowing a full staging review before promotion.

## Rules

- Long ingestion runs use the log-and-quarantine pattern: a bad file is logged and quarantined rather than aborting the run or being silently skipped.
- Everything landing in `screened` must be regular. Fix the upstream cause of irregularity rather than disabling `force_regular` downstream.
- Structures data is legitimately irregular and lives in the `structures` repository.
- Maintenance utilities available after ingestion: `inventory`, `merge_files`, `coarsen`, `rationalize_time_partitions`, `reconcile_data`, `delete_from_filelist`, `compare_directories`, `update_flagged_data`.

On-disk naming and format rules: see the `repository-format` skill.
