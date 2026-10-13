---
name: station-registry
description: "Use when dealing with dms_datastore station identity or geospatial metadata: station_dbase.csv registry columns, latitude and longitude versus projected x and y, EPSG:26910 adjusted coordinates, output header names, sublocations, and fixing missing or wrong coordinates."
---

# Station registry and coordinate ownership

Coordinates are the **single responsibility of the station registry**, located through `dstore_config` (`registries` section of `dstore_config.yaml`). The main registry is `station_dbase.csv` (`continuous`); meteorological stations are in `atmospheric_station_registry.csv` (`atmospheric`, CIMIS ids `cms<number>`).

## Registry columns

| Column | Meaning |
| --- | --- |
| `agency_lat`, `agency_lon` | Official location reported by the agency (EPSG:4269). Often wrong, e.g. a telemetry shed 200 m from the water. |
| `lat`, `lon` | Our preferred location (EPSG:4269) |
| `x`, `y` | Our preferred location, EPSG:26910 |

`lat`/`lon` and `x`/`y` start as copies of the agency location. Corrections are usually made in `x`/`y` first and then carried into `lat`/`lon`. Both pairs are our preference; `agency_lat`/`agency_lon` are kept as reported.

The CRS of each registry is declared under its `crs` key (`geographic`, `projected`) and read with `registry_crs()`.

## Output header names

Written data files use different names than the registry:

| Registry | File header |
| --- | --- |
| `lat` | `latitude` |
| `lon` | `longitude` |
| `x` | `projection_x_coordinate` |
| `y` | `projection_y_coordinate` |

## Rules

- Coordinates are auto-populated from the registry during processing.
- Dropbox recipes must not contain literal coordinate values. Any of `lat`, `lon`, `latitude`, `longitude`, `agency_lat`, `agency_lon`, `x`, `y`, `projection_x_coordinate`, `projection_y_coordinate` in a recipe metadata section raises an error.
- To fix a missing or wrong coordinate, update the registry CSV — not the recipe, and not the output file.
- Do not add spatial dependencies to perform reprojection that the registry already carries.

## Station identity

- Stations are identified as `station_id`, or `station_id@subloc` when a sublocation applies, e.g. `anh@north`, `msd@bottom`.
- `@subloc` is omitted from filenames when the sublocation is `default` or `None`.

Filename and metadata encoding rules: see the `repository-format` skill.
