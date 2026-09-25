---
name: repo-data-usage
description: "Use when reading or writing dms_datastore repository time series, programmatically or via CLI: required imports for read_ts, read_ts_repo, read_ts_block/data_block, and write_ts_csv, station@subloc and param@modifier syntax, dim layouts, and the station_info lookup utility."
---

# Repository data usage

## Required imports

```python
from dms_datastore import read_ts, read_ts_repo, read_ts_block, write_ts_csv
```

All four are re-exported from the package top level (`dms_datastore/__init__.py`); `read_ts`/`write_ts_csv` also live in `dms_datastore.read_ts`/`dms_datastore.write_ts`, and `read_ts_repo` in `dms_datastore.read_multi`.

## `read_ts` — a single file or glob pattern

Use for explicit files, not repository lookups by station/variable.

```python
ts = read_ts("W:/repo/continuous/screened/usgs_mrz_11337190_ec_2023.csv")
ts = read_ts("W:/repo/continuous/screened/usgs_mrz_*_ec_*.csv")  # glob, merged
```

`read_ts` auto-detects the file format (dms1, USGS, NOAA, CDEC, NCRO, WDL, ...). Prefer it over `pd.read_csv`: plain pandas loses flag/NA/unit handling and header metadata.

## `read_ts_repo` — one station/param from a configured repository

```python
ec_upper = read_ts_repo("mrz@upper", "ec", repo="screened", force_regular=True)
ec_lower = read_ts_repo("mrz@lower", "ec", repo="screened", force_regular=True)
temp_upper = read_ts_repo("mrz@upper", "temp")
elev_upper = read_ts_repo("mrz@upper", "elev")
```

- `station_id` may embed a sublocation as `station@subloc` (e.g. `mrz@upper`, `mrz@lower`), or pass `subloc=` separately — not both.
- `variable` may embed a modifier as `variable@modifier` (e.g. `ec@daily`).
- `repo` defaults to `"screened"`. `force_regular=True` is the default and should not be turned off to silence an error — fix the upstream irregularity instead.
- Result is always a `DataFrame`; squeeze if you need a `Series`.
- Data from `read_ts_repo` is already regular with a correct frequency and a unique index — do not re-validate it.

## `read_ts_block` / `data_block` — many stations and params at once

Built on `read_ts_repo`. Reads a cross product of stations × params (or an explicit request list) and reshapes into one table.

```python
from dms_datastore import read_ts_block

block = read_ts_block(
    station=["mrz@upper", "mrz@lower"],
    param=["ec", "temp", "elev"],
    dim="tidy",              # or "wide", "flat", "long", "sd_tidy", "[[t],[s_p]]"
    start="2023-01-01",
    end="2024-01-02",
)
```

CLI equivalent (console script `data_block`, entry point `dms_datastore.read_block:read_block_cli`):

```bash
data_block -o block.csv -s 2023-01-01 -e 2024-01-02 \
  --station mrz@upper --station mrz@lower \
  --param ec --param temp --param elev \
  --dim tidy
```

- `--station`/`station=` accepts the `station@subloc` suffix directly (e.g. `mrz@upper`); there is no separate `--subloc` option.
- `dim` layout tokens are `t` (datetime), `s` (station), `p` (param); aliases: `tidy`, `long`, `sd_tidy`, `flat`, `wide`. See the `read_block.py` module docstring for the full table.
- `mrz@upper` and `mrz@lower` are distinct `station` labels throughout — they never collide, in any layout, because the full `station@subloc` string is used as the label.
- Use `request=<csv path>` (or `--request`) instead of `station`/`param` for an explicit list of pairs, optionally with a `subloc` column.

## Writing — `write_ts_csv`

```python
write_ts_csv(ts, "path/to/output.csv", metadata={"agency": "usgs", "station_id": "mrz"})
```

- Use `write_ts_csv`, not `ts.to_csv`, so the `#`-commented YAML front matter and dtypes (e.g. nullable `Int64` `user_flag`) are preserved.
- Front-matter contract, `user_flag` semantics, column-naming conventions, and the filename grammar are owned by the `repository-format` skill — read that before hand-writing a repository file.

## `station_info` — look up station identity/metadata

Use before guessing a station id or coordinate, to confirm it exists and see which registries/repos serve it.

```bash
station_info mrz
station_info Martinez
station_info --list-registries
station_info --list-repos
station_info --config
```

Programmatic equivalent: `dms_datastore.station_info.station_info(search, registries=None)`.

## Rules

- Don't re-check regularity or index uniqueness on data returned by `read_ts_repo`/`read_ts_block` — the repository guarantees both if force_regular=True
- Prefer `read_ts_repo`/`read_ts_block` over raw file globbing when the data lives in a configured repository; use `read_ts` directly only for files outside that scheme.
- Station/sublocation and file-naming conventions: see the `repository-format` and `station-registry` skills.
