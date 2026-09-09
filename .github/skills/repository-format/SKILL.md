---
name: repository-format
description: "Use when reading, writing, naming, or validating dms_datastore repository files: filename grammar (agency/station/subloc/variable/year sharding), the CSV plus YAML front-matter data format, user_flag semantics, units, and station_id@subloc or variable@modifier metadata conventions."
---

# dms_datastore repository format

Authoritative rules for the on-disk contract of a `dms_datastore` repository.

## File naming grammar

Filenames are parsed and searched in `dms_datastore/filename.py` using patterns declared in `dms_datastore/config_data/dstore_config.yaml`.

A representative pattern:

```
{agency}_{station_id@subloc}_{agency_id}_{variable}_{syear}_{eyear}.csv
```

- `@subloc` is omitted when the sublocation is `default` or `None`.
- End year `9999` means open-ended, i.e. actively updated.
- `variable@modifier` encodes things like `ec@daily`. Anything after `@` is optional.

Examples:

- `usgs_anh@north_11303500_flow_2024.csv`
- `cdec_sac_11447650_flow_2020_9999.csv`

Use `meta_to_filename` to render and `interpret_fname` to parse. Do not build or split these names with ad hoc string manipulation.

## Data file format

CSV with `#`-commented YAML front matter:

```csv
# format: dwr-dms-1.0
# date_formatted: 2024-01-15T12:00:00
# source_info:
#   siteName: MOKELUMNE R A ANDRUS ISLAND
datetime,value,user_flag
2020-01-01 00:00:00,1.5,0
```

- Index column is `datetime`.
- Always two data columns: `value` (float) and `user_flag` (nullable `Int64`).
- `user_flag != 0` means anomalous. `read_ts` masks flagged values by default (`read_flagged=True`).
- Files are year-sharded. `read_ts` handles wildcards across shards automatically.

Read with `read_ts_repo` for repository data and `read_ts` for explicit files or patterns. Write with `write_ts_csv` so front matter and dtypes are preserved. `pd.read_csv` loses front matter, flag handling, NA codes, and comment handling.

## Metadata semantics

- **Station id with sublocation**: `station_id@subloc`, e.g. `anh@north`, `msd@bottom`.
- **Variable with modifier**: `param@modifier`, e.g. `ec@daily`.
- **Units**: SI for most variables; stage and flow in ft and cfs; salinity as specific conductance at 25 °C in µS/cm.
- **Source priority** is declared per agency in `dstore_config.yaml` and resolved by `read_ts_repo`. Never hard-code provider preferences in code.
- **Config paths** resolve through `dstore_config.config_file(label)`, which checks the current working directory before the packaged `config_data/`. Because several utilities accept either a path or a config label, `str` is often preferred over `Path` in those signatures.

Coordinates are not part of this contract; they are owned by the station registry. See the `station-registry` skill.
