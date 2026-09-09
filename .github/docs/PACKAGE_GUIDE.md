# dms_datastore package guide

Detailed architecture reference. See [AGENTS.md](../../AGENTS.md) for the rules that govern changes.

## Project overview

`dms_datastore` is a Python library and CLI toolkit for the Delta Modeling Section (DMS) that downloads, formats, screens, and manages continuous time-series data from water-quality and hydrological agencies (USGS, CDEC, NOAA, NCRO, DES, and others).

Data flows through four stages:

**raw → formatted → screened → processed**

Each stage is a directory tier of the repository. `screened` is the default read tier.

## Module map

| Layer | Modules | Purpose |
|---|---|---|
| Public API | `__init__.py` | Re-exports `read_ts_repo`, `read_ts`, `write_ts_csv` |
| CLI | `__main__.py` | Click group `dms` aggregating all subcommands |
| Config | `dstore_config.py`, `config_data/dstore_config.yaml` | Repo roots, station DBs, variable/source mappings |
| File naming | `filename.py` | Parse/render filenames via `interpret_fname` / `meta_to_filename` |
| I/O | `read_ts.py`, `write_ts.py` | Low-level CSV read/write with YAML front-matter |
| Multi-file read | `read_multi.py` | `read_ts_repo` — resolves source priority, merges year-sharded files |
| Download | `download_*.py` | One module per data source (CDEC, NWIS, NOAA, NCRO, DES, HRRR, HYCOM, …) |
| Pipeline | `populate_repo.py`, `update_repo.py` | Orchestrate download → format → screen |
| QA/QC | `auto_screen.py`, `screeners.py` | YAML-driven screening; flags stored in the `user_flag` column |
| Dropbox ingest | `dropbox_data.py`, `dropbox_recipes/` | One-off/recipe-driven ingestion of arbitrary source files |
| Utilities | `inventory.py`, `merge_files.py`, `coarsen_file.py`, `rationalize_time_partitions.py`, `reconcile_data.py` | Repo maintenance |

## Reading data

`read_ts_repo` is the preferred reader for most applications. It looks up a repository config in `dstore_config.yaml`, locates the data, resolves source priority, and merges year-sharded files.

Source priority is declared per agency in `dstore_config.yaml` and resolved by `read_ts_repo`. Do not hard-code provider preferences in code.

Ad hoc reading with `pd.read_csv` is discouraged. See the data-access rules in [AGENTS.md](../../AGENTS.md).

## Configuration resolution

`dstore_config.config_file(label)` resolves a config label by checking the current working directory first, then the packaged `config_data/` directory. Several utilities accept either a real path or such a label.
