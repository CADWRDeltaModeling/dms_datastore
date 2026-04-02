# dms_datastore — Workspace Instructions

## Project Overview

`dms_datastore` is a Python library and CLI toolkit for the Delta Modeling Section (DMS) that downloads, formats, screens, and manages continuous time-series data from water-quality and hydrological agencies (USGS, CDEC, NOAA, NCRO, DES, etc.). Data flows through four stages: **raw → formatted → screened → processed**.

## Build and Test

The `dms_datastore` conda environment is assumed to exist. Always activate it before running any tests or install commands.

```bash
# Install (development mode)
conda activate dms_datastore
pip install --no-deps -e .

# Unit/integration tests (no real repo required)
conda activate dms_datastore && pytest

# Integration tests against a real repository
conda activate dms_datastore && pytest test_repo/ --repo=<path_to_repo>

# Single file
conda activate dms_datastore && pytest tests/test_filename.py
```

pytest is configured in `pyproject.toml` (`[tool.pytest.ini_options]`): strict markers, JUnit XML output, ignores `setup.py` and `build/`.

## Architecture

| Layer | Modules | Purpose |
|---|---|---|
| Public API | `__init__.py` | Re-exports `read_ts_repo`, `read_ts`, `write_ts_csv` |
| CLI | `__main__.py` | Click group `dms` aggregating all subcommands |
| Config | `dstore_config.py`, `config_data/dstore_config.yaml` | Repo roots, station DBs, variable/source mappings |
| File naming | `filename.py` | Parse/render filenames via `interpret_fname` / `meta_to_filename` |
| I/O | `read_ts.py`, `write_ts.py` | Low-level CSV read/write with YAML front-matter |
| Multi-file read | `read_multi.py` | `read_ts_repo` — resolves source priority, merges year-sharded files |
| Download | `download_*.py` | One module per agency (CDEC, NWIS, NOAA, NCRO, DES, HRRR, HYCOM, …) |
| Pipeline | `populate_repo.py`, `update_repo.py` | Orchestrate download → format → screen |
| QA/QC | `auto_screen.py`, `screeners.py` | YAML-driven screening; flags stored as `user_flag` column |
| Utilities | `inventory.py`, `merge_files.py`, `coarsen_file.py`, `rationalize_time_partitions.py`, `reconcile_data.py` | Repo maintenance |

## File Naming Convention

Pattern: `{agency}_{station_id@subloc}_{agency_id}_{variable}_{syear}_{eyear}.csv`

- `@subloc` is omitted when subloc is `default`/`None`
- End year `9999` means open-ended (actively updated)
- `variable@modifier` encodes e.g. `ec@daily`

Examples:
- `usgs_anh@north_11303500_flow_2024.csv`
- `cdec_sac_11447650_flow_2020_9999.csv`

See [dms_datastore/filename.py](../dms_datastore/filename.py) for `meta_to_filename` / `interpret_fname`.

## Data File Format

CSV files with `#`-commented YAML front-matter:

```csv
# format: dwr-dms-1.0
# date_formatted: 2024-01-15T12:00:00
# source_info:
#   siteName: MOKELUMNE R A ANDRUS ISLAND
datetime,value,user_flag
2020-01-01 00:00:00,1.5,0
```

- Index column: `datetime`
- Always two data columns: `value` (float) and `user_flag` (`Int64`, nullable)
- `user_flag != 0` → anomalous; masked by `read_ts` by default (`read_flagged=True`)
- Files are year-sharded; wildcards handled automatically by `read_ts`

## Key Conventions

- **Station IDs with sublocation**: `station_id@subloc` (e.g. `anh@north`, `msd@bottom`)
- **Variables with modifier**: `param@modifier` (e.g. `ec@daily`)
- **Units**: SI for most variables; stage/flow in ft / cfs; salinity as specific conductivity at 25°C (µS/cm)
- **Source priority** is declared per agency in `dstore_config.yaml` and resolved by `read_ts_repo` — do not hard-code provider preferences in code
- **Config paths** are resolved by `dstore_config.config_file(label)` — checks cwd first, then `config_data/`
- New download modules must register as a Click command in `__main__.py` and add an entry point in `pyproject.toml`

## Tests

- `tests/` — unit and integration tests with monkeypatched config; no real repo needed
- `test_repo/` — integration tests; pass `--repo=<path>` to pytest
- Use `tmp_path` and `monkeypatch` for config isolation
- Do not couple unit tests to the shared repo path

## Key Reference Files

- [README.md](../README.md) — full data model, flags, units, configuration system
- [README-dropbox.md](../README-dropbox.md) — Dropbox data ingestion via `dropbox_spec.yaml`
- [README-commands.md](../README-commands.md) — CLI command reference
- [dms_datastore/config_data/dstore_config.yaml](../dms_datastore/config_data/dstore_config.yaml) — central config
