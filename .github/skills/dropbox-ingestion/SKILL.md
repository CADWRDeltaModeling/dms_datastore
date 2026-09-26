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

There is a dedicated directory `<package/dropbox_recipes` that contains sample recipes. 

## Reconciling staged files into a repository

A recipe's `reconcile:` block controls how a staged file is merged into an existing repository series at overlap. Key setting: `prefer` (default `staged`), read in `dropbox_data.py` from `reconcile.prefer`. There is no CLI override — it's recipe-only.

- `prefer: staged` (default) — the freshly staged file wins at any overlap (`ts_merge([staged, repo], strict_priority=True)`, or `ts_splice(..., transition="prefer_last")` for irregular data). Use this when the producer of the staged file recomputes its full covered window consistently every run, so each run's staged file is internally self-consistent end to end and should simply supersede what's there.
- `prefer: repo` — the existing repo data wins at overlap; the staged file can only fill gaps or append beyond the repo's current end (`ts_merge([repo, staged], strict_priority=True)`, or `transition="prefer_first"`). Use this only for genuinely gap-filling/backfilling ingestion, or as a one-off safety net (e.g. a first validation pass after a producer's reliability fix) — not as the routine setting for a producer that appends a small tail on every run. Under `prefer: staged`, a producer that recomputes and republishes only a small recent tail each run stays consistent, because everything before its own cutoff is untouched between runs. Setting `prefer: repo` for that same routine, incrementally-appending workflow is *worse*, not safer: it permanently freezes each run's newly-appended tail right next to the previous run's differently-fit tail, baking in a small seam at every day's join forever. Reserve `prefer: repo` for deliberate one-off sweeps, then revert the recipe to `prefer: staged` afterward.
- See `dms_datastore/reconcile_data.py` (`apply_actions`/`splice_write`) for the exact merge semantics.

A "coherence sweep" — reconciling a full, unsliced recompute after a producer changes its own drift/edge-effect behavior, or after moving a producer's routine publish cutoff further forward in time — is a manual, occasional operation: temporarily flip that recipe's `prefer` to `repo`, run the dropbox ingest once, confirm the result, then revert to `staged` for routine runs.

## Debugging

- Use `--name` to isolate a single failing entry.
- Use `--debug` for verbose logging and `--logdir` to capture it.
- A failing entry should surface an explicit error rather than being robustified into a silent pass.

