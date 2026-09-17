# Moving a Station from USGS to USBR

This note explains what happens when a station's observing agency changes from
`usgs` to `usbr`, and what you need to do to keep the data pipeline healthy.
The current examples are `lbtoe` and `yby`.

## 2026 bulk migration lessons

The 2026 USGS-to-USBR migration covered an approved list of 40 stations and
updated both registry identity and existing repository files. This is a
historical conversion, not the usual forward-only handoff described below.

- `agency` is the current observing agency, while the first filename token and
  `source` metadata describe provenance. A converted USGS-origin formatted file
  must retain its `usgs_` prefix and `source: usgs`; it must not be renamed to
  `usbr_` merely because its current agency is USBR.
- Converted USGS-origin files use `agency: usbr`, lower-case target
  `station_id` and `agency_id`, preserve the former NWIS identifier as
  `usgs_id`, and contain exactly one
  `agency_history: changed USGS to USBR 2025-2026` entry.
- Existing CDEC-origin files retain `source: cdec` and must not receive a
  `usgs_id`. `cdec_id` is uppercase in the registry; station and agency IDs are
  lower-case.
- `@subloc` is part of the station token and must survive a station rename
  unchanged.
- Screened-tier destination collisions require an explicit, reviewed deletion
  policy. For this migration, both files in approved collision pairs were
  deleted so screening can regenerate them. Formatted-tier collisions must stop
  the migration rather than overwrite or delete history.
- A large network-share migration must complete preflight before it mutates.
  It must also be restart-safe: formatted files whose source prefix and target
  station name remain unchanged must be recognized as complete from their
  metadata, rather than planned again.
- Run long migrations from a user-owned PowerShell terminal with a log file;
  chat-run command timeouts do not indicate that a network-share process has
  failed or completed.

The final logged operation migrated 524 remaining files with exit code zero.
The post-migration preview reported zero planned transitions and zero
collisions.

You make the change by editing one row in
`dms_datastore/config_data/station_dbase.csv`: set the `agency` column to `usbr`
and fill in a `cdec_id` (USBR data is served through CDEC).

## The short version

1. Editing `agency → usbr` (with a `cdec_id`) is all it takes to redirect the
   downloader.
2. The pipeline — download, reformat, screen — handles the switch on its own, and
   the full history still shows up in the screened data.
3. There is **one manual cleanup step**: delete the leftover `usgs_*` files from
   the **screened** repo. If you skip it, reading the screened data will fail.

## How the switch flows through the pipeline

### Downloading (`populate_repo`)

Stations are chosen by their `agency` value. Once it says `usbr`:

- `populate --agencies usgs` no longer touches the station.
- `populate --agencies usbr` now downloads it — from **CDEC**, using the
  `cdec_id`. Those files arrive with a `cdec_*` prefix.

The old `usgs_*` files are left in place; they simply stop being updated. (A
`cdec_id` is required — a USBR station with no `cdec_id` is skipped.)

### Reformatting (`reformat`)

`reformat` works one file at a time and doesn't care about the registry. It just
converts whatever raw files it finds, so it now processes the `cdec_*` files the
same way it used to process `usgs_*` ones. Nothing special happens.

### USGS multivariate step (`usgs_multi`)

This step only splits apart existing multi-variable USGS files. With no new USGS
downloads for these stations, there is nothing new to do — it's simply skipped
for them.

### Screening (`autoscreen`)

Screening is where the history gets stitched back together:

- For each station/variable, `autoscreen` reads the **formatted** repo through
  `read_ts_repo`, which knows that a `usbr` station should be assembled in the
  order `usgs → usbr → cdec`.
- So it combines the **old `usgs` history** with the **new `cdec` data** into a
  single series and screens it as one.
- It writes the result under the new agency name, so the screened files come out
  as `usbr_*` instead of `usgs_*`. The old `usgs_*` screened files are left
  behind.

## Will the first run have the full history?

Yes — in the screened output.

- The CDEC download by itself only brings back recent data, not decades of
  history.
- But screening reads the formatted repo with the `usgs → usbr → cdec` priority,
  so it pulls the deep `usgs` history from the formatted files and splices the
  recent `cdec` data onto the end. The resulting `usbr_*` screened files span the
  whole record.

This works **as long as the old `usgs` formatted files are still there**. If
those had been deleted, the old history would be gone, because CDEC can't supply
it.

## What about the user flags?

Screening recomputes `user_flag` from scratch every run, so the flags in the new
`usbr_*` files are generated fresh — they aren't copied from, or compared
against, the old `usgs_*` files. The only thing that could be lost is a
hand-edited flag sitting in an old screened file. For `lbtoe` and `yby` there are
no manual flags, so there's nothing to worry about.

## The `--partial` option

`--partial` tells `populate` to skip the historical download blocks and only
refresh recent data (2020 onward).

- It's a good fit here: CDEC mostly has recent data anyway, and the history is
  preserved through the formatted repo regardless.
- It never deletes anything — it only adds or overwrites recent files. So it
  won't clean up the old `usgs_*` files for you.

## The one thing you must clean up

The **screened** repo assumes there's exactly one version of each series, and it
matches files by station and variable **regardless of the agency prefix**. So
after the switch you end up with both `usgs_lbtoe_*` and `usbr_lbtoe_*` files
sitting side by side, and a read like:

```python
read_ts_repo(station_id="lbtoe", variable="flow", repo="screened")
```

picks up **both** and complains that their time ranges overlap:

> `ValueError: Overlapping shard windows detected ...`
> `Shards in read_ts_repo are expected to be non-overlapping.`

(The **formatted** repo doesn't have this problem — it knows how to prefer one
source over another.)

**Fix:** once the new `usbr_*` screened files exist, delete the old
`usgs_lbtoe_*` / `usgs_yby_*` files from the **screened** repo. Cleaning up the
old `usgs_*` files in the **formatted** repo is optional (they're harmless there)
but tidy. This step probably would require script-based injection of metadata on the transfer:
prior_agency_note: transferred from usgs
prior_agency_id: 12345678

## Checklist

- [ ] Set `agency = usbr` and add a `cdec_id` in `station_dbase.csv`.
- [ ] Run `populate --agencies usbr` (add `--partial` for a recent-only refresh).
- [ ] Run `reformat`, then `autoscreen` — this produces the `usbr_*` screened files.
- [ ] Delete the leftover `usgs_*` files from the **screened** repo for these stations.
- [ ] (Optional) Delete the leftover `usgs_*` files from the **formatted** repo.
- [ ] Confirm `read_ts_repo(repo="screened", ...)` returns the full series with no error.


## Concerns 
- Need a backup
- Some of the USGS stations with upper/lower sublocations have become USBR stations with different station codes. Should we migrate? Probably, but we don't do this a lot.
- Should station_dbase have an nwis_id column? Or should we at least harvest these? 
- Is the inclusion of agency_id a liability on the datastore?





