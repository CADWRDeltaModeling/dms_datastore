# Dropbox Data Processing System

## Overview

The dropbox system reads unformatted time-series data files from arbitrary sources,
applies transforms, attaches standardized metadata, and writes formatted CSV files
into a staging area. Optionally it reconciles staged files into a repository.

The entry point is a YAML specification file (a "recipe") that describes one or more
data ingestion tasks. Recipes use [OmegaConf](https://omegaconf.readthedocs.io/)
for variable interpolation.

## CLI

```bash
dms dropbox --input dropbox_spec.yaml                # run all entries
dms dropbox --input dropbox_spec.yaml --name ccfb    # run one entry by name
dms dropbox --input dropbox_spec.yaml --debug        # verbose logging
dms dropbox --input dropbox_spec.yaml --logdir ./logs --quiet
```

Options:
- `--input` (required): Path to the YAML recipe file.
- `--name` (repeatable): Run only the named recipe entry/entries.
- `--logdir`: Directory for log files.
- `--debug`: Enable debug-level logging.
- `--quiet`: Suppress console output.

## Programmatic Use

```python
from dms_datastore.dropbox_data import dropbox_data
dropbox_data("dropbox_spec.yaml")
dropbox_data("dropbox_spec.yaml", selected_names=["ccfb"])
```

## Recipe Structure

```yaml
# Top-level variables available via ${...} interpolation
dropbox_home: //cnrastore-bdo/Modeling_Data/repo_staging/dropbox
target_tz: "Etc/GMT+8"

data:
  - name: <unique recipe entry name>
    skip: false                     # optional, set true to skip

    collect:
      file_pattern: "*.csv"         # glob or filename template (see below)
      location: "${dropbox_home}/subdir"
      recursive_search: false
      reader: read_ts               # currently the only supported reader
      reader_args: {}               # optional kwargs passed to reader
      selector: null                # column name to select, or null
      wildcard: null                # null | time_shard | time_overlap
      merge_method: ts_splice       # ts_splice | ts_merge (for time_overlap)
      merge_args: {}                # kwargs to merge function
      splice_args: {}               # optional: {rename: value} or {rename: {old: new}}

    transforms:                     # optional, applied in order
      - dst_tz                      # string form (no args)
      - name: coarsen               # dict form (with args)
        args:
          grid: 2min
          preserve_vals: [0.0]

    metadata:
      station_id: <id>              # required (literal, infer_from_filename, or infer_from_agency_id)
      subloc: default               # required
      source: <source>              # required
      agency: <agency>              # required (literal or registry_lookup)
      param: <param>                # required
      unit: <unit>                  # required
      time_zone: Etc/GMT+8          # required
      freq: infer                   # required (literal freq string, "infer", or None for irregular)
      # Other fields as needed (station_name: registry_lookup, etc.)
      # Coordinates are NOT allowed here — they are auto-populated from the registry.

    output:
      repo_name: formatted          # must match a repo in dstore_config.yaml
      staging:
        dir: ./drop_staging         # must exist; staged files written here
        write_args:                 # optional kwargs to write_ts_csv
          float_format: "%.4f"
          chunk_years: false
      reconcile:                    # optional; if present, staged files are reconciled into repo
        #repo_data_dir: ./fake_repo # override target dir (omit to use repo root from config)
        prefer: staged              # staged | repo
        allow_new_series: true
        inspection:
          recent_years: 3
          p3: 0.15
          p10: 0.05
```

## Metadata Sentinels

Recipe metadata values can be:
- **Literal**: `station_id: anh` — used as-is.
- **`infer_from_filename`**: Parsed from the filename using the `file_pattern` template.
- **`registry_lookup`**: Looked up from the station registry CSV by station_id or agency_id.
  Supported fields: `station_name`, `agency`, `agency_id`.
- **`infer_from_agency_id`**: Special value for `station_id` — resolves station_id from the
  registry by matching `agency_id`.

## Coordinate Policy

Geospatial coordinates are **always auto-populated from the station registry** (e.g.
`station_dbase.csv`). Recipe authors must not include coordinate fields in `metadata:`.

The following keys are banned in recipe metadata sections:

> `lat`, `lon`, `latitude`, `longitude`, `agency_lat`, `agency_lon`,
> `x`, `y`, `projection_x_coordinate`, `projection_y_coordinate`

If any of these appear, the recipe will fail with an error directing the user to
add the station to the registry instead.

The registry provides:
- `agency_lat` / `agency_lon` — agency-reported WGS84 coordinates (written to file
  headers as `latitude` / `longitude`)
- `x` / `y` — projected coordinates in EPSG:26910 (UTM Zone 10N), potentially
  adjusted for accuracy (written as `projection_x_coordinate` / `projection_y_coordinate`)

## Wildcard Modes

The `collect.wildcard` field controls how multiple files matching `file_pattern` are handled:

- **omitted / null**: Pattern must match exactly one file.
- **`time_shard`**: Pass the glob pattern directly to the reader (year-sharded/blocked files). Lexicographical sorting is assumed to match chronological.
- **`time_overlap`**: Glob, read each file individually, then merge via `merge_method`.

## Filename Templates (Inference Mode)

When `file_pattern` contains `{field}` placeholders (e.g.
`{source}_{station_id}_{agency_id}_{param}_{syear}_{eyear}.csv`), the system enters
"inference mode": each matched file's name is parsed to extract metadata fields marked
`infer_from_filename`. In this mode, `wildcard` must be omitted — each file produces
a separate output.

## Transforms

Transforms are applied to the time series after reading (and after merging if applicable).
Built-in transforms:

- **`dst_st` / `dst_tz`**: Convert from local (DST-aware) time to a fixed timezone.
  Args: `src_tz`, `target_tz`.
- **`coarsen`**: Reduce irregular high-frequency data to a regular grid.
  Args: `grid`, `preserve_vals`, `qwidth`, `hyst`, `heartbeat_freq`.

Custom transforms can be registered via `register_transform(name, func)`.

## Failure Handling

Each recipe entry is processed independently. If one fails, the error is logged and
processing continues with the next entry. At the end, if any entries failed, a
`RuntimeError` is raised listing all failed entry names. Use `--name <entry>` to
rerun individual failures.

## Examples

See `examples/dropbox/` for working recipes:
- `dropbox_spec.yaml` — single-file and wildcard patterns
- `dropbox_spec_ccf.yaml` — structure gate data with transforms (coarsen, DST)
- `dropbox_daily.yaml` — template-based inference mode for daily NWIS data

