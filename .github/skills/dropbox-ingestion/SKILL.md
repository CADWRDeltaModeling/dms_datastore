---
name: dropbox-ingestion
description: "Use when guiding users through dms_datastore dropbox ingestion: planning and authoring a recipe, choosing a repository and station identity, deciding inference or wildcard mode, regularity and transforms, staging and validation, reconciliation, dry runs, and debugging."
---

# Dropbox ingestion

The dropbox system reads unformatted time-series files from arbitrary sources, applies transforms, attaches standardized metadata, and writes formatted CSV into a staging area. It can optionally reconcile staged files into a repository.

The dropbox is one of the two modes of ingestion described in `docsrc/concepts.rst` ("Modes of Ingestion"): bulk automated download covers the continuous family from provider web services; the dropbox covers controlled formatting and updating of non-bulk sources (vendor/SCADA exports, backfills, locally processed products, feeds needing custom handling). For the bulk flow use the `repository-ingestion` skill. Do not describe the bulk flow as *the* repo workflow.

User-facing concepts, decision diagram, schema, transforms, and worked examples: [Dropbox Data Ingestion](../../../docsrc/dropbox.rst). Keep this skill consistent with that page.

## End-to-end workflow

Guide the user through these decisions in order. Do not invent missing station,
repository, unit, time-zone, or overlap semantics; inspect the source files and
configured registries/repositories, or ask for the missing information.

1. **Identify the series.** Establish what each file represents, its station,
   parameter, sublocation, units, time zone, and provenance. Confirm the station
   exists in the applicable registry (continuous stations commonly use
   `station_dbase.csv`). Coordinates belong to the registry, never recipe-authored
   metadata. `agency` identifies the instrument-owning agency; `agency_id` is its
   native identifier; `source` describes how this product was collected or
   processed.
2. **Choose the receiving repository.** Check `dstore_config.yaml` and select an
   existing `repo_name` whose purpose and regularity match the data. The usual
   continuous-data progression is `raw` (incoming/initial staging, not a
   persistent tier), `formatted` (standardized source observations), `screened`
   (QA/QC and standardized units), then `processed` (derived, filled, or
   application-oriented data). Irregular structure or gate records use an
   appropriate irregular repository such as `structures_formatted`; daily data
   may have its own configured family. Treat names as configuration, not a
   universal fixed list. Creating a repository is an exceptional decision, not
   the first response to a mismatch.
3. **Choose how files produce series.** Use filename-template inference when
   separate files encode separate series. Otherwise, one entry produces one
   series: use no wildcard for one file, `time_shard` for non-overlapping time
   blocks, and `time_overlap` only for actual overlaps. See `Recipe decision path`
   below.
4. **Set regularity and transformations.** Regular series use
   `force_regular: true` and `metadata.freq: infer` or a frequency, with a
   regular repository. Irregular event/gate series use `force_regular: false`,
   `metadata.freq: none`, and an irregular repository. Apply `dst_tz` to
   daylight-saving-local timestamps before `coarsen`; omit transforms when the
   source already has the required time basis and values.
5. **Stage and inspect first.** Omit `output.reconcile` to write standardized
   files to `output.staging.dir` without changing a repository. Inspect the
   staged filenames, series identity, metadata/units, time coverage, values,
   and regularity before proceeding.
6. **Dry-run reconciliation, then production.** When ready to merge, configure
   `output.reconcile` deliberately and use `output.repo_data_dir` to target an
   existing mock directory seeded with the current files for those series.
   Compare the mock result and overlap joins with the originals. Only after
   validating the dry run should the user remove `repo_data_dir` to target the
   configured repository. Keep staging separate from the reconcile target.

For a historical backfill plus a recurring feed, the documented pattern is to
keep both entries in one recipe: run the backfill once, then set its `skip: true`
to preserve provenance without repeating it, while the ongoing entry tops off
the series.

## Worked recipe patterns

Use the bundled examples in `dms_datastore/dropbox_recipes/` to compare patterns;
verify that their identity, repository, and update semantics fit before adapting:

- `dropbox_smscg_scada.yaml` — irregular, overlapping SCADA gate exports; applies
   `dst_tz` before `coarsen`, targets `structures_formatted`, and keeps a one-time
   radial backfill as a `skip: true` entry after it has run.
- `dropbox_daily.yaml` — filename-template inference, producing separate regular
   daily series from files whose names encode their metadata.
- `dropbox_mrz_stage_processed.yaml` — a recomputed processed product; uses
   `prefer: staged` because the producer recomputes the covered history.

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

## Recipe decision path

Mirrors the "Recipe Decision Path" diagram in `dropbox.rst`. Each step settles one part of the recipe.

1. **Inference mode vs. `collect.wildcard`.** Identify the source files as a pattern, then:
   - Files are *different series* (e.g. different variables or stations) whose names encode identity → **inference mode**: `file_pattern` is a template with `{field}` placeholders; omit `wildcard` (setting it raises). Each file is its own output.
   - Otherwise the matches share one output-series identity and one entry produces that series. Multi-file modes combine the data along time; they do not combine distinct series. `collect.wildcard` is one of:
     - `none` (or omitted) — pattern matches exactly one file; multiple matches raise.
     - `time_shard` — files cover different, non-overlapping periods (e.g. years).
     - `time_overlap` + `merge_method` (`ts_splice` | `ts_merge`, with `merge_args`) — only when files genuinely overlap in time. Rare; don't copy it in reflexively.
2. **Regularity (`collect.reader_args`, `metadata.freq`, `output.repo_name`).**
   - Regular: `force_regular: true` (default), `metadata.freq: infer` or an explicit lower-case freq (`15min`), regular repo.
   - Irregular (gates, event logs): `force_regular: false`, `metadata.freq: none`, irregular repo such as `structures_formatted`. This is the main legitimate use of `force_regular: false`.
3. **`transforms`.** DST-local clock time → `dst_tz` (alias `dst_st`), applied *before* any `coarsen`. Otherwise no time-zone transform.
4. **`output`.**
   - Review only → omit `reconcile`; stop at staging.
   - Update repo → `reconcile` with deliberate `prefer` (`staged` | `repo`) and `allow_new_series` (`true` | `false`).
   - Then **dry run** (`output.repo_data_dir` → seeded mock repo) before **production** (omit `repo_data_dir`; target inferred from `repo_name`). See below.

## Recipes

Recipes are YAML loaded with `OmegaConf.load` and resolved with `OmegaConf.to_container(resolve=True)`, so `${...}` interpolation is available. Each recipe has top-level variables and a `data` list of named entries. Shared recipes live in `<package>/dropbox_recipes/` and serve as worked examples.

Choosing the receiving repository: it must exist in `dstore_config.yaml` and match the data's regularity and family. Ask: regular? structure operations? an abstract/derived product (e.g. Net Delta Outflow, a regulation objective, a harmonic forecast) → `processed`? daily time step → `daily_*`? Creating a new repository is infrequent and cautious — never the first move; use a dry run with `repo_data_dir` instead.

Metadata rules:

- **Series identity** is station, sublocation, parameter (and sometimes agency). `station_id` must be in the station registry. Make `subloc: default` explicit in recipes.
- **Parameter names and underscores:** the current filename-template parser rejects underscores in `{param}` (for example, `wind_speed` and `air_temp`). Tell the user about this limitation before ingestion; do not silently abbreviate names. Consult the configured variable definitions (`variables.csv`) and `variable_mappings.csv`, and encourage use of an existing, filename-compatible standard variable name where possible. If the standard name itself contains an underscore, flag the naming/parser inconsistency and prompt the user to agree on a coordinated fix rather than inventing a recipe-only alias. Validate the proposed output name with `meta_to_filename` and `interpret_fname` for the receiving repo before processing the full input.
- **Provenance:** `agency` is the instrument-owning agency, `agency_id` its native station id, `source` the collection/processing source (e.g. agency `ncro`, source `cdec`).
- **Coordinates come from the registry.** The keys `lat`, `lon`, `latitude`, `longitude`, `agency_lat`, `agency_lon`, `x`, `y`, `projection_x_coordinate`, `projection_y_coordinate` raise if given literal values. The only allowed value for them is `registry_lookup`, which many recipes use explicitly. Fix wrong or missing coordinates in the registry, not in a recipe (see the `station-registry` skill).
- **Time zone** is derived from the registry. Don't add it. The key `tz` raises if given a literal value. The only allowed value is `registry_lookup`. Fix wrong or missing time zones in the registry, not in a recipe. Typically PST is used for Bay-Delta data. Daylight savings time is never applied.
- **Averaged values** must be clearly indicated in the metadata. Use a dedicated key (e.g., `averaging_timestamp: pre|post|center`) to specify the time window over which the values are averaged. Do not embed this information in the parameter name or filename.
- Keep string fields lower case.

When in doubt ask. Users are encourage to consult the station registry, variable definitions, and experienced colleagues rather than making ad hoc decisions in the recipe and AI should not be relied upon to resolve these ambiguities automatically.

YAML value rules:

- Booleans (`allow_new_series`, `force_regular`, `skip`, `chunk_years`) should be lower-case, unquoted `true`/`false`. Unquoted `True`/`False` parse as booleans too, but a *quoted* `"false"` is a non-empty string and is truthy — `allow_new_series: "false"` silently permits new series.
- Unknown keys under `output`, `output.staging`, `output.reconcile`, and `output.reconcile.inspection` raise with a hint (e.g. `staging_dir`, `repo_dir`, `merge_priority` are stale names).
- `output.staging.dir` must be a scratch directory; its leaf is created if the parent exists. It must not coincide with or nest inside the reconcile target.

## Reconciling staged files into a repository

A recipe's `reconcile:` block controls how a staged file is merged into an existing repository series at overlap. Keys: `prefer`, `allow_new_series`, `inspection` (`recent_years`, `p3`, `p10`; meaningful only for time-sharded files). `prefer` defaults to `staged` and is recipe-only (no CLI override). Only the series the entry just produced are reconciled, and rewrites happen only when the data section actually differs.

- `prefer: staged` (default) — the freshly staged file wins at any overlap (`ts_merge([staged, repo], strict_priority=True)`, or `ts_splice(..., transition="prefer_last")` for irregular data). Use this when the producer of the staged file recomputes its full covered window consistently every run, so each run's staged file is internally self-consistent end to end and should simply supersede what's there.
- `prefer: repo` — the existing repo data wins at overlap; the staged file can only fill gaps or append beyond the repo's current end (`ts_merge([repo, staged], strict_priority=True)`, or `transition="prefer_first"`). Use this only for genuinely gap-filling/backfilling ingestion, or as a one-off safety net (e.g. a first validation pass after a producer's reliability fix) — not as the routine setting for a producer that appends a small tail on every run. Under `prefer: staged`, a producer that recomputes and republishes only a small recent tail each run stays consistent, because everything before its own cutoff is untouched between runs. Setting `prefer: repo` for that same routine, incrementally-appending workflow is *worse*, not safer: it permanently freezes each run's newly-appended tail right next to the previous run's differently-fit tail, baking in a small seam at every day's join forever. Reserve `prefer: repo` for deliberate one-off sweeps, then revert the recipe to `prefer: staged` afterward.
- See `dms_datastore/reconcile_data.py` (`apply_actions`/`splice_write`) for the exact merge semantics.

A "coherence sweep" — reconciling a full, unsliced recompute after a producer changes its own drift/edge-effect behavior, or after moving a producer's routine publish cutoff further forward in time — is a manual, occasional operation: temporarily flip that recipe's `prefer` to `repo`, run the dropbox ingest once, confirm the result, then revert to `staged` for routine runs.

## Dry run vs. production (`output.repo_data_dir`)

Reconcile writes the repo in place; there is no plan-only mode for the dropbox. The dry run is `output.repo_data_dir`:

- Placement: directly under `output`. Under `output.reconcile` it raises (`dropbox_data.py` checks this explicitly). Do not invent `repo_dir`; it is rejected.
- It must be an existing directory. `resolve_repo_data_dir` treats a non-existent path as a repo name and fails.
- `repo_name` still supplies naming, `regular`, `dtypes`, and `float_format`; only the physical target changes.
- Seed the mock repo with copies of the current repo files for the produced series. Empty mock + `allow_new_series: false` raises (no namesake); empty mock + `allow_new_series: true` only tests creation, not the overlap merge.
- Production = remove/comment out `repo_data_dir`; the target becomes the configured root of `repo_name`.
- Staging must never coincide with or nest inside the target in either mode.

Always propose a dry run against a seeded mock repo before a production reconcile.

## Debugging

- Use `--name` to isolate a single failing entry.
- Use `--debug` for verbose logging and `--logdir` to capture it.
- Use `--omit-unregistered` in inference mode to skip files whose station is not in the registry instead of failing the entry.
- Entries run independently; failures are logged and a `RuntimeError` listing failed entries is raised at the end.
- A failing entry should surface an explicit error rather than being robustified into a silent pass.

