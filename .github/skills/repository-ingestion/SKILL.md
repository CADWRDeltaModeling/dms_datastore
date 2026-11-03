---
name: repository-ingestion
description: "Use when populating, updating, or rebuilding a dms_datastore repository: the staging-to-repo zigzag, the download -> reformat -> screen flow, the spot_check validation gate, how the raw, formatted, screened, and processed tiers relate, DES multi-instrument rationalization, single-frequency resolution in reformat, and how update_repo compares, splices, replaces, quarantines, and escalates shards (including when to rebuild a provider)."
---

# Repository ingestion sequence

The standard pipeline moves data through four tiers: **raw → formatted → screened → processed**. The operational pattern is a staging-to-repo zigzag: data lands in `repo_staging_auto`, is validated there, and then is promoted into the canonical repo with `update_repo` or `update_flagged_data`.

## Standard sequence

1. `populate_repo` or a source-specific downloader writes to the raw staging tier. For DES, `populate_repo` then runs `rationalize_time_partitions` on the fresh download so overlapping instrument files no longer overlap (see below).
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
download_cimis --start 2024-01-01 --end 2024-01-31 --stations cms140 --dest <raw_dir>
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

## Multi-instrument records (DES)

DES publishes a station's record as several raw files, one per instrument, named by years covered (`..._2000_2008.csv`, `..._2004_2019.csv`), which can overlap. A formatted series uses one instrument at a time: never mix instruments, and never fill one instrument's gaps or provider-rejected values from another.

`rationalize_time_partitions "des*_*.csv" --spec des_rationalize_time_spec --root-dir <raw_dir>` enforces this on raw, per series:

1. A file whose **filename** year span lies inside another file's span is deleted unread (engulfed = side deployment, not the record). Identical spans keep the later name.
2. Remaining overlapping files are cut, not merged: ordered by first valid value, each newer file takes over at its first valid value; no hand-back.
3. Explicit rules in `des_rationalize_cfg.yaml` override (other cut-over dates, keeping a short-lived instrument, hand-back). If the default would need a hand-back, the series is left as-is with a warning asking for a rule.

It rewrites raw in place and explicit rules name the as-downloaded files, so run it once on a fresh download (`--dry-run` first); rerunning an explicit rule on files it already cut fails.

## Frequency resolution in reformat

DES files for one series are reformatted together into **one frequency** (`reformat_freq_resolver_from_labels`, usually the finer). Files already at the target are copied unchanged. Converted files are interpolated only between consecutive valid readings, so gaps and flagged values are never bridged (00:15 is filled from 00:10 and 00:20; empty if either is missing). Every year of a series with a finer instrument is written at the finer frequency.

## How update_repo reconciles

Comparison is per year shard. Recent shards (`--recent-years`, default 3) are always compared; older ones are sampled deterministically (`--p3` 0.15, `--p10` 0.05); hash-identical shards are skipped.

- Missing in repo: write. Same frequency, data differ: splice with staged priority (staged gaps inside its range stay gaps; repo values before the first or after the last staged value are kept).
- Different frequency: **replace** the repo shard whole (`--freq-mismatch replace`, the default; a series has one resolved frequency), or `quarantine`. Repo shard irregular: replace. Staged shard irregular: quarantine (copied to `./quarantine`).
- **Escalation:** a data change in an old shard, or a frequency mismatch in any compared shard, reconciles every shard of the series with the same per-shard rules.
- `update_flagged_data` (screened) uses the same shard logic but preserves `user_flag`; its `--freq-mismatch` default stays `quarantine` because replace discards repo flags.

## When to rebuild a provider

Sampling plus splicing converges slowly after changes that alter old values broadly (e.g. new reformat or rationalize rules). Instead: rebuild staging with the new code, confirm staging covers every repo series and year for the provider, delete the provider's files from repo `formatted` (and `screened` if it holds no manual flags you need), then promote and screen again. `update_repo --p3 1 --p10 1` is the slower alternative and can leave spliced edge values.

## Rules

- Long ingestion runs use the log-and-quarantine pattern: a bad file is logged and quarantined rather than aborting the run or being silently skipped.
- Everything landing in `screened` must be regular. Fix the upstream cause of irregularity rather than disabling `force_regular` downstream.
- Structures data is legitimately irregular and lives in the `structures` repository.
- Maintenance utilities available after ingestion: `inventory`, `merge_files`, `coarsen`, `reconcile_data`, `delete_from_filelist`, `compare_directories`, `update_flagged_data`.

On-disk naming and format rules: see the `repository-format` skill.
