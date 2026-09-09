---
name: dropbox-ingestion
description: "Use when ingesting arbitrary or one-off time-series files into dms_datastore through the dropbox system: authoring or debugging a dropbox recipe YAML, recipe name resolution, dms dropbox CLI options, transforms, attached metadata, and reconciliation of staged files into a repository."
---

# Dropbox ingestion

The dropbox system reads unformatted time-series files from arbitrary sources, applies transforms, attaches standardized metadata, and writes formatted CSV into a staging area. It can optionally reconcile staged files into a repository.

This is the one-off complement to the standard pipeline; for the routine `populate_repo` → `reformat` → `auto_screen` flow use the `repository-ingestion` skill.

Full specification, including the complete recipe schema and transform list: [README-dropbox.md](../../../README-dropbox.md).

## Entry points

```bash
dms dropbox --input dropbox_spec.yaml                # run all entries
dms dropbox --input dropbox_spec.yaml --name ccfb    # run one entry by name
dms dropbox --input dropbox_spec.yaml --debug        # verbose logging
dms dropbox --input dropbox_spec.yaml --logdir ./logs --quiet
```

```python
from dms_datastore.dropbox_data import dropbox_data

dropbox_data("dropbox_spec.yaml")
dropbox_data("dropbox_spec.yaml", selected_names=["ccfb"])
```

`dropbox_data` is the workhorse; the CLI is a thin wrapper over it.

## Recipe resolution

`--input` accepts either a path or the bare name of a bundled recipe. Resolution order, first match wins:

1. The value as given, if it is an existing file path (absolute, or relative to the current working directory).
2. `./dropbox_recipes/<name>` — a project-local override directory in the cwd.
3. `<package>/dropbox_recipes/<name>` — the bundled recipes.

A `.yaml` extension is appended when the name has none. Shared recipes live in `dms_datastore/dropbox_recipes/`, deliberately separate from `config_data/`, so bare names resolve identically for an editable install and a deployed wheel.

Because an argument may be a path *or* a label, these interfaces take `str` rather than `Path`.

## Recipes

Recipes are YAML and use OmegaConf interpolation. Each recipe holds one or more named ingestion entries.

Recipes must **not** contain literal coordinates. Any of `lat`, `lon`, `latitude`, `longitude`, `agency_lat`, `agency_lon`, `x`, `y`, `projection_x_coordinate`, `projection_y_coordinate` in a recipe metadata section raises an error. Coordinates are populated from the station registry during processing — see the `station-registry` skill.

## Debugging

- Use `--name` to isolate a single failing entry.
- Use `--debug` for verbose logging and `--logdir` to capture it.
- A failing entry should surface an explicit error rather than being robustified into a silent pass.
