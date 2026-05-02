
# Retrieval
- For continuous, regular data prefer read_ts_repo.  `from dms_datstore import read_ts_repo`
- In most cases the default repo "screened" (which is defined in dstore_config.yaml). Other examples are "processed" for filled/transformed/derived data and "structures" for irregular gated data. 
- Note that force_regular is usually True. Report and solve problems rather than revert. Using force_regular=False is a typical AI antipattern. Everything in "screened" tier is regular. Structures are not regular.
- alternately use read_ts(file_or_pattern)
- avoid pd.read_csv unless for special cases. It omits wildcards, regression issues, flag handling, NA codes, # comments, lacks metadata.
- scripts that use read_ts_repo in applied settings may assume "back door" acquisition using known station ids but should provide cli or config choices to allow acquisition using files. [TODO: provide tools for this]
- post-read of repo data, avoid regularity and duplicate index checks.  


# Architecture 

## Project Overview

`dms_datastore` is a Python library and CLI toolkit for the Delta Modeling Section (DMS) that downloads, formats, screens, and manages continuous time-series data from water-quality and hydrological agencies (USGS, CDEC, NOAA, NCRO, DES, etc.). Data flows through four stages: **raw → formatted → screened → processed**.


| Layer | Modules | Purpose |
|---|---|---|
| Public API | `__init__.py` | Re-exports `read_ts_repo`, `read_ts`, `write_ts_csv` |
| CLI | `__main__.py` | Click group `dms` aggregating all subcommands |
| Config | `dstore_config.py`, `config_data/dstore_config.yaml` | Repo roots, station DBs, variable/source mappings |
| File naming | `filename.py` | Parse/render filenames via `interpret_fname` / `meta_to_filename` |
| I/O | `read_ts.py`, `write_ts.py` | Low-level CSV read/write with YAML front-matter. raw use of pd.csv() should be avoided. |
| Multi-file read | `read_multi.py` | `read_ts_repo` — resolves source priority, merges year-sharded files |
| Download | `download_*.py` | One module per data source (CDEC, NWIS, NOAA, NCRO, DES, HRRR, HYCOM, …) |
| Pipeline | `populate_repo.py`, `update_repo.py` | Orchestrate download → format → screen |
| QA/QC | `auto_screen.py`, `screeners.py` | YAML-driven screening; flags stored as `user_flag` column |
| Utilities | `inventory.py`, `merge_files.py`, `coarsen_file.py`, `rationalize_time_partitions.py`, `reconcile_data.py` | Repo maintenance |

## Data ingestion
Usually `populate_repo` followed by `reformat` and `usgs_multi` for USGS data. Then `autoscreen` and `update_repo`

A second more one-off method for ingesting data is through `dropbox_data.py`. It's design is in [README-dropbox.md]

## File Naming Convention

File names are parsed and searched in [dms_datastore/filename.py](../dms_datastore/filename.py) based on patterns in `config_data/dstore_config.yaml` 

An example pattern is this:
`{agency}_{station_id@subloc}_{agency_id}_{variable}_{syear}_{eyear}.csv`

- `@subloc` is omitted when subloc is `default`/`None`. 
- End year `9999` means open-ended (actively updated)
- `variable@modifier` encodes e.g. `ec@daily` and again things after the `@` are optional.

Examples:
- `usgs_anh@north_11303500_flow_2024.csv`
- `cdec_sac_11447650_flow_2020_9999.csv`

See  for `meta_to_filename` / `interpret_fname`.

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
- `user_flag != 0` → anomalous; masked by `dms_datastore/read_ts` by default (`read_flagged=True`)
- Files are year-sharded; wildcards handled automatically by `read_ts`

The preferred reader for most applications is `dms_datastore/read_ts_repo`. It looks up a repo config in dbase_config.yaml, identifies the location of the data. 

Ad hoc reading with pd.read_csv discouraged.

## Elements of style

- Prefer failure to robustification and passes.
- For long processes, there is a log-and-quarantine pattern. 
- CLI should be in click
- A workhorse function should provide similar functionality programmatically.
- Designs should layer opening and validating data from programmatic work. This isn't always possible for things like downloaders.

## Metadata

- **Station IDs with sublocation**: `station_id@subloc` (e.g. `anh@north`, `msd@bottom`)
- **Variables with modifier**: `param@modifier` (e.g. `ec@daily`)
- **Units**: SI for most variables; stage/flow in ft / cfs; salinity as specific conductivity at 25°C (µS/cm)
- **Source priority** is declared per agency in `dstore_config.yaml` and resolved by `read_ts_repo` — do not hard-code provider preferences in code
- **Config paths** are resolved by `dstore_config.config_file(label)` — checks cwd first, then `config_data/`
- Some utilities like dropbox_data.py and reformat follow the following convention:
  * they can take a path as an argument
  * or they can take a string that evalues to a path using `config_file()` in `dbase_config.py`
  * for this reason the use of Path rather than str is often not preferred.

### Coordinates

Coordinates are the **single responsibility of the station registry** (`station_dbase.csv`).

- Registry columns: `agency_lat`, `agency_lon` (WGS84, agency-reported), `x`, `y` (EPSG:26910, adjusted)
- Output file headers use: `latitude`, `longitude`, `projection_x_coordinate`, `projection_y_coordinate`
- Dropbox recipes **must not** contain literal coordinate values — they are auto-populated from the registry during processing. Any of `lat`, `lon`, `latitude`, `longitude`, `agency_lat`, `agency_lon`, `x`, `y`, `projection_x_coordinate`, `projection_y_coordinate` in a recipe metadata section will raise an error.
- To fix missing or wrong coordinates, update the registry CSV — not the recipe.

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
