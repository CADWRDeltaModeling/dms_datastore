---
name: downloader-builder
description: "Use when creating, modifying, reviewing, or debugging a dms_datastore download_<source>.py provider downloader: the three-layer CLI/arg-shaping/work-function structure, the positional signature populate_repo calls, station request shaping helpers, common click options, raw output filename grammar, overwrite/skip and no-data semantics, request chunking under provider record limits, credentials, logging, registration, and test patterns."
---

# Building a provider downloader

A downloader fetches one provider's data (`noaa`, `cdec`, `usgs`/NWIS, `des`, `ncro`, ...) into a **plain destination directory**. That directory is the `raw` staging tier; `reformat` → `auto_screen` take it from there (see the `repository-ingestion` skill). CLI conventions are in [CLI_GUIDE.md](../../docs/CLI_GUIDE.md); this skill adds what is specific to downloaders.

Reference implementations, roughly in order of maturity: `download_noaa.py`, `download_cdec.py`, `download_nwis.py`, `download_des.py`, `download_ncro.py`. `download_cimis.py` is the newest and least mature; it is not a template. Gate/one-off scrapers (`download_dcc`, `download_smscg`, `download_montezuma_gates`, `download_ucdipm`, `download_hycom`, `download_hrrr`) are older or special-purpose; do not copy their interfaces.

## What a downloader is and is not responsible for

- **Does:** resolve a station request, query the provider for a time window, write one raw file per (station, subloc, param) request into `--dest`.
- **May:** read a station registry (via `dstore_config`) to list stations and translate ids. Registries live in `config_data/` and work offline.
- **Does not:** know repository roots, write into a repo, append to or reconcile with existing files, convert units, screen, or regularize. Those belong downstream (`reformat`, `auto_screen`, `update_repo`, dropbox reconcile). A downloader must run in a disconnected directory.

## Three layers

| Layer | Name | Role |
| --- | --- | --- |
| CLI | `download_<source>_cli` | click options, logging setup, read credentials from the environment, call the shaping or work function with named arguments |
| Arg shaping | `download_<source>(...)` | resolve `stationfile` vs `--stations`, parse dates, build the request frame (registry ids, `src_var_id`) |
| Work | `<source>_download(stations, dest_dir, start, end=None, param=None, overwrite=False, ...)` | loop over request rows, query, write files |

`populate_repo.downloaders` calls the work function **positionally** as `fn(request_frame, dest_dir, start, end, param, overwrite)`. Keep that leading order if the source will be populated automatically; add provider-specific options (`freq`, `max_workers`, `daily`) as trailing keywords. Register it in `populate_repo.downloaders` only when it joins the bulk flow.

Keep a lower-level per-station function (`download_station_data`, `download_station`, `process_station`) that takes explicit arguments; it is the unit that tests monkeypatch around.

## Station requests

Build the request frame with the helpers in `process_station_variable.py`:

1. `stationfile_or_stations(stationfile, stations)` — exactly one of the positional station file or `--stations`.
2. `normalize_station_request(stationframe=..., stationlist=..., param=..., default_subloc=...)` → columns `station_id`, `param`, `subloc`. Parses `station@subloc`.
3. `attach_agency_id(df, repo_name="formatted", agency_id_col="agency_id", src_site_id_col=None)` → `agency_id`, and `src_site_id` (the id the provider is queried with, e.g. `cdec_id`).
4. `attach_src_var_id(df, variable_mappings, source="<source>")` → `src_var_id` (provider variable code). `variable_mappings.csv` rows are filtered by `src_name == source`; one `param` may expand to several codes.

Sources whose stations are not in `station_dbase.csv` use their own registry, declared under `registries` in `dstore_config.yaml` and read with `dstore_config.registry_df(name)` — no repo is needed for that. Where users plausibly type either the registry id or the provider's native id, normalize both to the registry id; reject unknown ids with a `ValueError` naming them.

## CLI options

| Option | Convention |
| --- | --- |
| `--dest` (→ `dest_dir`) | default `<source>_download`; created if missing |
| `--start`, `--end` | ISO dates (`2024-01-01`, optionally `2024-01-01 14:00`); see time window below |
| `--param` | project parameter name (`elev`, `flow`, `ec`, `temp`), mapped through `variable_mappings.csv` |
| `--stations` | `multiple=True`; registry ids, `station@subloc` allowed |
| `STATIONFILE` | positional `nargs=-1`, at most one CSV with a `station_id` column; mutually exclusive with `--stations` |
| `--overwrite` | flag; otherwise existing files are skipped |
| `--logdir`, `--debug`, `--quiet` | passed to `resolve_loglevel` / `configure_logging` |
| `-h`, `--help` | `@click.help_option("-h", "--help")` |

Avoid the legacy outliers for new work: `--syear/--eyear` (noaa), `--sdate/--edate` (hycom, hrrr), `--base-dir` (gate scrapers), positional dates (ucdipm).

Where a provider offers several frequencies or products, prefer a **deterministic** choice for new work: one code per run, e.g. `click.Choice([...], case_sensitive=False)`, normalized in the work function. CDEC's `--freq E,H` (try each until one returns data) is established and fine for CDEC, but harder to reason about.

## Time window

- `start`: required (cdec) or defaulting to a provider-wide floor (des uses 1900). Do not derive per-station start dates from the registry; the registry is not an inventory.
- `end` omitted → now / latest available. The file name gets `eyear` `9999` (`endfile = 9999 if end is None else end.year`).
- Probe the provider before deciding what "latest" means. If it returns placeholder rows (all values missing) for future dates, reject a future `end` with a `ValueError` rather than writing placeholders.
- `start > end` → `ValueError`.

## Output files

- Name: `{source}_{station_id}[@{subloc}]_{agency_id}_{param}_{syear}_{eyear}.csv`, lower case, `@subloc` omitted when `default`. It must parse with the `raw` repo `filename_templates` in `dstore_config.yaml`, because `reformat` reads metadata from the raw file name (`interpret_fname(..., repo="raw")`).
- `syear` is the requested start year; `populate_repo.revise_filename_syears` later trims it to the actual data span.
- Content is the provider's data, minimally reshaped:
  - raw provider text with a commented header (`write_header` in noaa, `# key : value` lines in des), or
  - a DataFrame written with `write_ts_csv(df, path, metadata, chunk_years=False)` (nwis, ncro), index named `datetime`.
- Keep provider units, QC codes and time convention (usually LST/PST). Record provenance (provider URL, provider station id, provider variable id/name/unit) in the header.
- **One run writes one file per request window.** No appending, no incremental resume from an existing file. Existing file + no `--overwrite` → skip and log `Skipping existing station because file exists: <path>`.
- Do not encode frequency or product variants in the file name. Variants that would share a name go to different `--dest` directories.

## Requests

- **Record limits:** split the window into chunks on **neat calendar boundaries** that stay under the provider's per-request cap (noaa requests by calendar month). Do not shrink chunks adaptively on timeouts; that produces irregular, irreproducible request boundaries. If a single request could exceed a cap and chunking isn't implemented, raise (`download_nwis._check_daily_limit`).
- **Combining chunks:** collect each chunk as a DataFrame with a `DatetimeIndex`, drop empty chunks, and combine with `vtools.ts_splice(frames, transition="prefer_last")` (as in `download_smscg`). Union the columns first so chunks that lack a column still splice. Or, for raw-text providers, write the chunks in order (noaa).
- **Retries/throttling:** bounded retries for transient failures. A persistent throttle (HTTP 403/429) should abort the whole run rather than produce a patchwork (noaa `NoaaThrottleError`). Space requests when the provider asks for it.
- **Concurrency:** modest. `ThreadPoolExecutor` with `max_workers` 1–6, or asyncio (ncro). Reuse sessions.
- **Credentials:** from an environment variable (`<SOURCE>_API_KEY`). The CLI reads it and raises `click.UsageError` if missing; the work function takes it as an explicit argument. Never a CLI option, never logged.

## Outcomes per request

Follow CDEC's model: each per-station call reports `success`, `exists` (skipped), or `no_data`.

- **No data in the window** (station not yet operating, sensor absent) is not an error. Write no file, log `No data found for station=...` at INFO, and list them at the end (`Failed query stations ...`). Never write an empty file.
- **Request or parse errors** (HTTP errors, API error payloads, malformed responses, timeouts after retries) are failures. Log per station and continue (log-and-quarantine for batch runs; nwis moves unparseable files to `quarantine/`), then raise at the end listing the failed stations. Do not swallow them.
- Argument errors (bad freq, unknown station, `end` in the future) raise `ValueError` before any requests.

## Logging

`logger = logging.getLogger(__name__)` at module level. Only the CLI calls `configure_logging(package_name="dms_datastore", level=..., console=..., logdir=logdir, logfile_prefix="download_<source>")`. DEBUG for URLs and per-chunk row counts, INFO for files written/skipped and no-data summaries, WARNING for retries, ERROR for station failures.

## Before writing code: probe the API

Run a few live requests and record what you find:

- Valid product/item names (some providers reject the whole request on one bad item).
- Per-request record cap and how it is enforced (error vs truncation).
- Behavior before the station existed (empty vs error) and after today (placeholders vs error).
- Time stamping (hour-beginning vs hour-ending; some providers use `2400`), time zone, units.
- Error payload shape and HTTP codes.

## Registration and docs

- `dms_datastore/__main__.py`: `cli.add_command(download_<source>_cli, "download_<source>")`.
- `pyproject.toml` `[project.scripts]`: `download_<source> = "dms_datastore.download_<source>:download_<source>_cli"`.
- `README-commands.md` example, `docsrc/commands.rst` help line, and the example list in the `repository-ingestion` skill.
- New dependencies: avoid; the stdlib, `requests`, `pandas`, and `vtools` cover normal cases. Remove dependencies that a rewrite makes unused.

## Tests

`tests/test_download_<source>.py`:

- Offline unit tests (no marker): monkeypatch the request function to return synthetic records; cover chunk boundaries and the record cap, file naming (`9999` when open-ended), skip vs `--overwrite`, `no_data` writes nothing, request errors propagate, id normalization, argument validation, and the CLI via `click.testing.CliRunner` (`-h` exits 0; arguments reach the work function; missing credential exits 2).
- **Every test that touches the network** — a provider API, any internet URL, or the network repository share — must carry `@pytest.mark.integration`. Normal CI runs `-m "not integration"`; an unmarked network test makes CI flaky or fails it outright. When a credential is needed, also add `pytest.mark.skipif` on the environment variable so local runs without it skip cleanly. The marker is registered in `pyproject.toml` and `--strict-markers` is on, so spell it exactly.
- Keep live tests few and small (one station, a few days); they verify the provider contract, not the logic the offline tests already cover.
- Markers filter tests only after the module imports; a stale import in a test module breaks every run regardless of markers.
