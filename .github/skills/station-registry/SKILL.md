---
name: station-registry
description: "Use when dealing with dms_datastore station identity or geospatial metadata: station_dbase.csv registry columns, latitude and longitude versus projected x and y, EPSG:26910 adjusted coordinates, output header names, sublocations, and fixing missing or wrong coordinates."
---

# Station registry and coordinate ownership

Coordinates are the **single responsibility of the station registry** (`station_dbase.csv`, located through `dstore_config`).

## Registry columns

| Column | Meaning |
| --- | --- |
| `agency_lat`, `agency_lon` | WGS84, as reported by the source agency |
| `x`, `y` | EPSG:26910, adjusted |

## Output header names

Written data files use different names than the registry:

| Registry | File header |
| --- | --- |
| `agency_lat` | `latitude` |
| `agency_lon` | `longitude` |
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
