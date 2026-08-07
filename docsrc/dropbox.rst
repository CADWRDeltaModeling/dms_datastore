Dropbox Data Ingestion
======================

Overview
--------

The *dropbox* is the standard-conformant on-ramp for getting time series into a
``dms_datastore`` repository. It lets collection and pre-processing happen in
whatever scratch area, web scrape, or incoming pipeline is convenient, and then
moves the result into a repository (``formatted``, ``processed``,
``structures_formatted``, ...) with consistent naming, metadata, units, and
headers.

A single YAML *recipe* describes one or more ingestion tasks. Each task moves
data through four steps:

1. **Read** the data from its scratch/incoming location. Input is CSV and a
   single column can be selected.
2. **Transform** the data. Typical transforms convert SCADA-style local
   (daylight-saving) clock time to a fixed offset, coarsen high-rate irregular
   data, or trim empty edges. Unit conversions and similar reshaping happen
   here.
3. **Stage** a standardized CSV (canonical filename, YAML front-matter header,
   ``value``/``user_flag`` columns) into a staging directory.
4. **Reconcile** the staged file into the repository under an explicit policy
   (top-off vs. replace, whether new series may be created, etc.).

The entry point is the ``dms dropbox`` CLI or the ``dropbox_data()`` function.

Motivation
----------

The dropbox exists so that the *messy* part of data handling — vendor-specific
CSV layouts, wrong time zones, legacy units, one-off backfills — stays out of
the repository. The repository only ever receives files that already conform to
the datastore conventions. Benefits:

- **Separation of concerns.** Cleaning/scraping can live anywhere. The recipe is
  the single, reviewable description of how a raw feed becomes a repo series.
- **A self-documenting trail.** A recipe (and its ``skip:`` markers) records
  exactly how each series was produced. One-time backfills stay in the file as
  documentation rather than living only in someone's shell history.
- **Repeatability.** Ongoing feeds re-run the same recipe to *top off* existing
  series with new data.
- **Safety.** Metadata, coordinates, and naming are validated against the
  station registry before anything touches the repo.

CLI
---

.. code-block:: bash

   dms dropbox --input dropbox_smscg_scada.yaml                     # run all entries
   dms dropbox --input dropbox_smscg_scada.yaml --name smscg_scada  # one entry
   dms dropbox --input dropbox_smscg_scada.yaml --debug            # verbose logging
   dms dropbox --input dropbox_smscg_scada.yaml --logdir ./logs --quiet
   dms dropbox --input dropbox_daily.yaml --omit-unregistered      # skip unknown stations

Options:

- ``--input`` (required): the recipe to run. May be a path to a YAML file
  (absolute or relative to the current directory) or the bare name of a recipe
  bundled in ``dms_datastore/dropbox_recipes/`` (e.g. ``dropbox_daily`` or
  ``dropbox_daily.yaml``).
- ``--name`` (repeatable): run only the named recipe entry/entries.
- ``--logdir``: directory for log files.
- ``--debug``: enable debug-level logging and per-file output.
- ``--quiet``: suppress console output.
- ``--omit-unregistered``: in inference mode, skip files that cannot be matched
  to a station registry entry and stage the rest. Without this flag, any
  unregistered file causes the recipe to fail.

Recipe Resolution
-----------------

Recipes are centralized in the bundled ``dms_datastore/dropbox_recipes/``
directory so bare names resolve identically in a development install
(``pip install -e .``) or a deployed wheel. ``--input`` (and the
``dropbox_data()`` argument) is resolved in this order, first match wins:

1. The value as given, if it is an existing file path.
2. ``./dropbox_recipes/<name>`` — a project-local override directory in the cwd.
3. ``<package>/dropbox_recipes/<name>`` — the bundled recipes.

A ``.yaml`` extension is appended automatically when omitted.

Programmatic Use
----------------

.. code-block:: python

   from dms_datastore.dropbox_data import dropbox_data

   dropbox_data("dropbox_smscg_scada.yaml")
   dropbox_data("dropbox_smscg_scada.yaml", selected_names=["smscg_scada"])

How to Write a Recipe
---------------------

A recipe is a YAML file with top-level interpolation variables and a ``data``
list of entries. Values are resolved with `OmegaConf
<https://omegaconf.readthedocs.io/>`_, so ``${...}`` interpolation is available
(use it for path composition instead of Python ``.format``).

.. code-block:: yaml

   # Top-level variables available via ${...} interpolation
   repo_home: //cnrastore-bdo/Modeling_Data/
   target_tz: "Etc/GMT+8"          # POSIX sign reversed => PST
   target_tz_label: PST

   data:
     - name: my_series             # unique entry name (used by --name)
       skip: false                 # optional; true leaves the entry as a documented no-op

       collect:
         file_pattern: "*.csv"     # glob or filename template
         location: "${repo_home}/incoming/my_source"
         recursive_search: false
         reader: read_ts           # currently the only supported reader
         reader_args: {}           # kwargs passed to read_ts
         selector: none            # column name (or list) to keep, or none
         wildcard: none            # none | time_shard | time_overlap
         merge_method: ts_splice   # ts_splice | ts_merge (for time_overlap)
         merge_args: {}            # e.g. {transition: prefer_last}
         splice_args: {}           # optional {rename: value} or {rename: {old: new}}

       transforms:                 # optional, applied in order
         - name: dst_tz
           args:
             src_tz: US/Pacific
             target_tz: ${target_tz}

       metadata:
         station_id: my_id         # required
         source: my_source         # required
         agency: usgs              # required
         param: flow               # required
         unit: ft^3/s              # required
         time_zone: ${target_tz}   # required
         freq: infer               # required: literal freq | "infer" | none (irregular)
         subloc: default           # optional

       output:
         repo_name: formatted      # must match a repo in dstore_config.yaml
         staging:
           dir: ${repo_home}/incoming/drop_staging
           write_args:
             float_format: "%.2f"
             chunk_years: false
         reconcile:                # optional; omit to stage only
           prefer: staged          # staged | repo
           allow_new_series: false

Step by step:

1. **Point ``collect`` at the raw files.** Use ``selector`` to keep one column
   (or a list of columns for multi-gate frames). Use ``reader_args`` for
   source-specific quirks (``names``, ``usecols``, ``na_values``, ``hint``).
2. **Add ``transforms`` only when needed.** Regular scientific feeds usually
   need none. SCADA feeds typically need ``dst_tz`` and often ``coarsen``.
3. **Fill in ``metadata``.** Keep string fields lower case. Coordinates come from
   the registry — never hard-code them (see below).
4. **Choose an ``output`` policy.** Stage into a scratch directory, then decide
   whether/how to reconcile into the repo.

Naming convention: keep recipe file names consistent and descriptive
(``dropbox_smscg_scada.yaml``, ``dropbox_ccf.yaml``). Avoid the legacy ``spec``
infix in new recipes.

Regular vs. Irregular Data
--------------------------

Whether a series is regular (evenly sampled, with a fixed frequency) or
irregular (event/gate data) drives several settings. Getting this wrong is the
most common recipe mistake.

**Regular data** (nearly everything: stage, flow, EC, temperature, ...)

- ``metadata.freq: infer`` (or a literal like ``15min``). The pipeline infers
  and records the frequency.
- ``reader_args.force_regular`` should be left at its default (``True``). Do not
  set ``force_regular: False`` for regular data — that is an anti-pattern that
  silently drops the frequency contract. Report and fix gaps instead of
  reverting to irregular reads.
- The target repo is a regular repo (``regular: true`` in ``dstore_config.yaml``);
  reconciliation applies regular-frequency checks.

**Irregular data** (gates, structures, event logs)

- ``metadata.freq: none``.
- ``reader_args.force_regular: False`` is correct here — gate positions are not
  evenly sampled. This is one of the few places ``force_regular: False`` belongs.
- The target repo is an irregular repo (e.g. ``structures_formatted``);
  reconciliation skips the frequency-mismatch gate and stitches steps with
  ``ts_splice`` by time rather than forcing a union index.

Rule of thumb: ``force_regular: False`` is often right for gates and almost never
right for anything else.

Wildcard Modes
--------------

``collect.wildcard`` controls how multiple matching files are handled:

- **omitted / ``none``**: the pattern must match exactly one file. This is the
  normal case for a single consolidated source file. YAML's bare ``null``
  keyword (or ``None``) is also accepted as a synonym for ``none``, but
  ``none`` is the preferred, self-explanatory spelling in new recipes.
- **``time_shard``**: pass the glob straight to the reader for year-sharded /
  non-overlapping blocked files (``..._2023.csv``, ``..._2024.csv``).
  Lexicographic order is assumed to match chronological order.
- **``time_overlap``**: glob, read each file individually, then merge via
  ``merge_method``. Use this only when incoming files genuinely overlap in time.

.. note::

   ``time_overlap`` is frequently over-used by copy-and-paste from SCADA
   recipes. Overlap in time is uncommon, *especially for regular data*. If your
   files do not actually overlap, prefer ``time_shard`` (sharded blocks) or a
   single-file pattern. Reach for ``time_overlap`` only when it is truly needed.

For ``time_overlap``, ``merge_method`` selects the combiner (``ts_splice`` or
``ts_merge``) and ``merge_args`` passes options such as
``transition: prefer_last`` / ``prefer_first``.

Filename Templates (Inference Mode)
-----------------------------------

When ``file_pattern`` contains ``{field}`` placeholders (e.g.
``{source}_{station_id}_{agency_id}_{param}_{syear}_{eyear}.csv``), the recipe
enters *inference mode*: each matched file name is parsed to extract metadata
fields marked ``infer_from_filename``, and each file produces a separate output.
In this mode ``wildcard`` must be omitted. Files that cannot be matched to a
registry entry cause a failure unless ``--omit-unregistered`` is passed.

Transforms
----------

Transforms run after reading (and after merging, for ``time_overlap``). Each
entry is either a bare string (no args) or a ``{name, args}`` mapping. They are
applied in order. Built-ins:

``dst_tz`` (alias ``dst_st``)
   Convert local, daylight-saving-aware clock time to a fixed offset. This is
   the typical need for SCADA/Wonderware exports; major scientific collectors
   usually already report in a fixed offset and need no conversion. Most commonly
   used for SCADA gate data, applied *before* coarsening. See CCF and SCADA recipes for examples.
   Args: ``src_tz`` (e.g. ``US/Pacific``), ``target_tz`` (e.g. ``Etc/GMT+8``). 

``coarsen``
   Reduce high-rate irregular data (e.g. gate positions) to an adaptive grid
   while preserving transitions. Useful for SCADA data (CCF gates, Suisun Marsh) which
   are stored on DWR SCADA systmes and come out too fine. 
   Args: ``grid`` (output interval, e.g.
   ``2min``), ``preserve_vals`` (values to snap to, e.g. ``[0.0]``), ``qwidth``
   (quantization width), ``hyst`` (hysteresis as a fraction of ``qwidth``),
   ``heartbeat_freq`` (force a keep-alive sample during idle periods).

``trim_data``
   Drop contiguous all-/any-NaN rows from the leading and/or trailing edge.
   Args: ``side`` (``first`` | ``last`` | ``both``), ``criterion``
   (``all_nan`` | ``any_nan``).

``add_column``
   Add a constant/typed column. Args: ``name`` (required), ``default``,
   ``dtype``.

``linear``
   Apply an affine transform ``value * scale + offset`` to the series. Handy for
   a sign convention (``scale: -1.0`` to flip a stored-negative inflow to a
   positive-inflow convention) or a simple unit/datum shift. Args: ``scale``
   (default ``1.0``), ``offset`` (default ``0.0``).

Custom transforms can be registered at runtime with
``register_transform(name, func)``.

.. note::

   Unit conversion (e.g. legacy feet/cfs, or specific conductance) is handled as
   part of preparing the data. The legacy DMS convention is feet for stage and
   cfs for flow with a sane number of significant digits — hundredths of a foot
   and 1 (or 0.1) cfs are enough for all non-DeltaCD purposes. Prefer a modest
   ``float_format`` (e.g. ``"%.2f"``) over emitting spurious precision.

Common Transforms
~~~~~~~~~~~~~~~~~~

Most recipes need no transforms at all — a clean, fixed-offset regular feed is
staged as-is. The patterns below cover the cases that do come up. Transforms are
listed under a ``transforms:`` key at the recipe-entry level (a sibling of
``collect`` / ``metadata`` / ``output``) and are applied top to bottom.

**No transform (the common case).** Regular scientific data that already reports
in a fixed offset needs nothing:

.. code-block:: yaml

   transforms:      # omit entirely, or leave empty

**Convert SCADA local time to a fixed offset.** Wonderware/SCADA exports are
usually in local, daylight-saving-aware clock time. Convert once, up front:

.. code-block:: yaml

   transforms:
     - name: dst_tz
       args:
         src_tz: US/Pacific     # DST-aware source clock
         target_tz: ${target_tz}   # e.g. Etc/GMT+8 (PST)

**Coarsen high-rate irregular gate data.** For adaptive gates recorded at a high
raw rate, reduce to a regular-ish grid while preserving open/close transitions.
Typically applied *after* ``dst_tz``:

.. code-block:: yaml

   transforms:
     - name: dst_tz
       args:
         src_tz: US/Pacific
         target_tz: ${target_tz}
     - name: coarsen
       args:
         grid: 2min             # output sampling interval
         preserve_vals: [0.0]   # snap to fully-closed state + boundaries
         qwidth: 0.01           # quantization width (ft)
         hyst: 0.5              # re-quantize only when value moves > 0.5*qwidth
         heartbeat_freq: 60min  # force a keep-alive sample during idle periods

**Trim empty edges.** Drop leading/trailing all-NaN rows left by an export that
padded the record:

.. code-block:: yaml

   transforms:
     - name: trim_data
       args:
         side: both             # first | last | both
         criterion: all_nan     # all_nan | any_nan

**Add a constant column.** Occasionally useful to stamp a fixed auxiliary
column:

.. code-block:: yaml

   transforms:
     - name: add_column
       args:
         name: quality
         default: 1
         dtype: Int64

Ordering matters. Convert time zones *before* coarsening (coarsening assumes a
monotonic, correctly-offset index), and note that for ``time_overlap`` recipes
transforms run *after* the files are merged — so a ``dst_tz`` transform cannot
repair a fall-back duplicate that exists in a single raw file. In that case
provide a pre-cleaned, DST-adjusted source file instead.

Metadata
--------

Metadata is validated by ``_check_metadata`` before staging. String fields
(``source``, ``agency``, ``param``, ``station_id``, ``subloc``) must be lower
case.

**Required**

- ``station_id`` — datastore station id (registry key).
- ``source`` — the processing/collection source of *this particular product*.
- ``agency`` — the agency that owns the instrument in the water.
- ``param`` — variable (e.g. ``flow``, ``ec``, ``height``).
- ``unit`` — physical unit (``ft``, ``cfs``/``ft^3/s``, ``uS/cm``, ...).
- ``time_zone`` — a pandas/vtools-compatible fixed offset (e.g. ``Etc/GMT+8``).
- ``freq`` — a literal frequency string, ``infer`` (regular), or ``none``
  (irregular; ``null``/``None`` are accepted synonyms).

**Optional**

- ``subloc`` — sublocation (``default`` when omitted; e.g. ``upper``,
  ``bottom``, ``radial``).
- ``agency_id`` — the station identifier in the *agency's own* record.
- ``station_name``, ``processor``, ``time_zone_label``, and other descriptive
  fields.

**``agency`` vs. ``agency_id`` — what they mean**

For *observed* data these two answer distinct questions:

- ``agency`` — "who has an instrument in the water?" (e.g. ``usgs``, ``dwr``).
- ``agency_id`` — "what does that agency call this station in their own data of
  record?" (their native site number/name).

This is unambiguous for observed feeds. For a *processed* repo (derived, filled,
or model-input series that merely reuse a station location), ``agency`` and
``agency_id`` are often not meaningful and may be better omitted or documented
explicitly. When a processed series blends providers for different variables
(e.g. one station whose flow comes from USGS but whose EC comes from a different
agency), encode the provider per variable (``usgs`` for flow, the EC provider
for EC) so ``agency`` stays truthful; do not force a single agency label that is
wrong for some variables. When in doubt, add a short note in the recipe
explaining what the metadata means for the processed product.

**Metadata sentinels**

Values may be literals or one of these sentinels:

- ``infer_from_filename`` — parsed from the filename template.
- ``registry_lookup`` — looked up from the station registry by ``station_id`` or
  ``agency_id`` (supports ``station_name``, ``agency``, ``agency_id``, and
  coordinate fields).
- ``infer_from_agency_id`` — special value for ``station_id``; resolves the
  station id from the registry by matching ``agency_id``.

Coordinate Policy
-----------------

Coordinates are the single responsibility of the station registry (e.g.
``station_dbase.csv`` / ``structures_registry.csv``). They are auto-populated
from the registry during processing; recipe authors must **not** hard-code them.

The following keys are rejected in recipe ``metadata`` (unless supplied via
``registry_lookup``):

   ``lat``, ``lon``, ``latitude``, ``longitude``, ``agency_lat``,
   ``agency_lon``, ``x``, ``y``, ``projection_x_coordinate``,
   ``projection_y_coordinate``

To fix missing or wrong coordinates, update the registry CSV — not the recipe.
The registry provides ``agency_lat``/``agency_lon`` (agency-reported WGS84,
written as ``latitude``/``longitude``) and ``x``/``y`` (EPSG:26910, potentially
adjusted, written as ``projection_x_coordinate``/``projection_y_coordinate``).

Output and Reconciliation
-------------------------

``output`` controls staging and (optionally) reconciliation:

- ``repo_name`` — must match a repo defined in ``dstore_config.yaml``. The repo's
  ``regular``, ``dtypes``, and ``float_format`` settings are honored during
  reconciliation.
- ``staging.dir`` — a scratch directory that must exist (its leaf is created if
  the parent exists). Standardized files are written here.
- ``staging.write_args`` — kwargs to ``write_ts_csv`` (e.g. ``float_format``,
  ``chunk_years``).
- ``reconcile`` — omit to stop at staging. When present, staged files are merged
  into the repo:

  - ``prefer`` (``staged`` | ``repo``): which side wins on overlapping
    timestamps.
  - ``allow_new_series`` (bool): whether a staged series with no repo namesake
    may be initialized.
  - ``inspection`` (``recent_years``, ``p3``, ``p10``): sampling policy for
    re-inspecting *older shards* (see cautions below).

Reconciliation is *content-smart*: it hashes the data section (ignoring header
churn such as ``date_formatted``) and only rewrites when values actually differ.
The dropbox passes the exact series it just produced to the reconciler, so
unrelated files sharing the staging directory are not swept into the repo.

Recommended Workflow: Backfill, then Top Off
--------------------------------------------

A good pattern for a series that has a legacy history plus an ongoing feed is to
keep both in one recipe file:

1. **A one-time backfill entry.** Load the legacy record (in the right units and
   a sane precision) into the repo. Use ``prefer: staged`` so the backfill wins
   while initializing the series.
2. **Mark it ``skip: true`` once it has run.** The entry stays in the file as a
   self-documenting record of how the history was produced, but does not run
   again.
3. **An ongoing entry** in the same file that tops off the series with new data
   as it arrives.

The ``dropbox_smscg_scada.yaml`` recipe (``radial_init`` backfill +
``smscg_scada`` ongoing) is a working example of this pattern.

Common Pitfalls
---------------

A few habits tend to spread by copy-and-paste. Watch for them in review:

- **``wildcard: time_overlap`` where it is not needed.** True time overlap is
  uncommon, especially for regular data. Use ``time_shard`` or a single-file
  pattern unless files genuinely overlap.
- **``force_regular: False`` on regular data.** Correct for gates; wrong for
  essentially everything else. Fix the underlying gaps instead of dropping the
  frequency contract.
- **``allow_new_series: true`` used casually.** It permits creating a *new*
  parallel series file — which means a misspelled ``station_id`` or ``param``
  silently produces a phantom series next to the real one. Prefer ``false`` for
  ongoing feeds into existing series; enable it deliberately only when
  initializing a genuinely new series.
- **``inspection:`` on unsharded files.** The ``recent_years`` / ``p3`` / ``p10``
  sampling only has meaning for time-sharded (``_2023.csv``) files, where it
  randomly re-checks older shards to bound nightly workload. On a single
  unsharded file it does nothing — do not add it reflexively.
- **Spurious precision.** Match ``float_format`` to the measurement (e.g.
  ``"%.2f"`` ft, ``"%.1f"`` cfs). Do not emit an unsupportable number of
  significant figures.
- **Coordinates in the recipe.** Always fix these in the registry, never in
  ``metadata``.

Failure Handling
----------------

Each recipe entry is processed independently. If one fails, the error is logged
and processing continues with the next entry. At the end, if any entries failed,
a ``RuntimeError`` is raised listing the failed names. Re-run individual failures
with ``--name <entry>``.

Bundled Examples
----------------

See ``dms_datastore/dropbox_recipes/`` for working recipes, including:

- ``dropbox_smscg_scada.yaml`` — irregular gate (SCADA) data: DST conversion,
  adaptive coarsening, and the backfill-then-top-off pattern.
- ``dropbox_daily.yaml`` — template-based inference mode for daily NWIS data.
- ``dropbox_ccf.yaml`` — structure/gate data with transforms.

Key Functions
-------------

- ``dropbox_data`` / ``apply_dropbox_workflow`` — run a recipe.
- ``DataCollector`` — file discovery from patterns.
- ``populate_meta`` / ``infer_meta`` — resolve and infer metadata (registry
  lookups, filename inference).
- ``_check_metadata`` — validate required fields, casing, units, time zone, and
  coordinates.
- ``register_transform`` — register a custom transform.
- ``reconcile_data.update_repo`` — content-smart reconciliation of staged files
  into a repository.
