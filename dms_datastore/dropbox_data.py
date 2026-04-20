import os
import re
import pandas as pd
import yaml
import glob
import click
import dms_datastore.dstore_config
from vtools import dst_st, ts_merge, ts_splice,ts_coarsen
from omegaconf import OmegaConf
from vtools.data.indexing import infer_freq_robust
from dms_datastore.read_ts import read_ts, infer_freq_robust
from dms_datastore.write_ts import write_ts_csv
from dms_datastore.dstore_config import repo_registry, repo_config, config_file
from dms_datastore.filename import interpret_fname, meta_to_filename, naming_spec
from dms_datastore.reconcile_data import update_repo
import logging
from pathlib import Path
from dms_datastore.logging_config import configure_logging, resolve_loglevel

logger = logging.getLogger(__name__)


# Global variable to store cached data
_cached_spec = None


_TRANSFORMS = {}

def register_transform(name, func):
    if not isinstance(name, str) or not name:
        raise ValueError("transform name must be a non-empty string")
    if name in _TRANSFORMS:
        raise ValueError(f"transform '{name}' is already registered")
    _TRANSFORMS[name] = func


def get_spec(filename):
    global _cached_spec
    # OmegaConf gives us native interpolation (e.g. ${dropbox_home}) and
    # environment substitution (e.g. ${oc.env:VAR,default}).
    cfg = OmegaConf.load(filename)
    # Convert to plain dict/list (what the rest of this module expects)
    # and resolve interpolations immediately.
    _cached_spec = OmegaConf.to_container(cfg, resolve=True)
    return _cached_spec


def reader_for(fstr):
    if fstr == "read_ts":
        return read_ts
    else:
        return None


class DataCollector(object):
    def __init__(self, name, location, file_pattern, recursive=False):
        self.name = name
        self.location = location
        self.file_pattern = file_pattern
        self.recursive = recursive

    def data_file_list(self):
        fpath = os.path.join(self.location, self.file_pattern)
        return glob.glob(fpath, recursive=self.recursive)

    def data_file_glob(self):
        """Return the glob pattern (not expanded)."""
        return os.path.join(self.location, self.file_pattern)


# YAML uses dst_tz; keep dst_st as implementation name.
def _transform_dst_tz(ts, **targs):
    return dst_st(ts, **targs)

 
def _transform_dst_st(ts, **targs):
    return dst_st(ts, **targs)


def _transform_coarsen(
    ts,
    *,
    grid="1min",
    preserve_vals=(),
    qwidth=None,
    hyst=1.0,
    heartbeat_freq="120min",
    **kwargs,
):
    # strict: no silent extra args (you asked for fail-fast)
    if kwargs:
        raise ValueError(f"coarsen transform got unexpected args: {sorted(kwargs.keys())}")
    return ts_coarsen(
        ts,
        grid=grid,
        preserve_vals=preserve_vals,
        qwidth=qwidth,
        hyst=hyst,
        heartbeat_freq=heartbeat_freq,
    )

# register built-ins
register_transform("dst_st", _transform_dst_st)
register_transform("coarsen", _transform_coarsen)
register_transform("dst_tz", _transform_dst_tz)



_FILENAME_FIELD_SENTINELS = {"infer_from_filename", "registry_lookup"}


def _metadata_uses_filename_inference(metadata):
    return any(value == "infer_from_filename" for value in (metadata or {}).values())


def _metadata_uses_registry_lookup(metadata):
    return any(value == "registry_lookup" for value in (metadata or {}).values())


def _pattern_is_template(pattern):
    return isinstance(pattern, str) and "{" in pattern and "}" in pattern


def _template_to_glob(pattern):
    return re.sub(r"\{[^{}]+\}", "*", pattern)


def _registry_lookup_value(row, field_name):
    field_map = {
        "station_name": "name",
        "agency": "agency",
        "agency_id": "agency_id",
        "latitude": "lat",
        "longitude": "lon",
        "projection_x_coordinate": "x",
        "projection_y_coordinate": "y",
    }
    if field_name not in field_map:
        raise ValueError(f"registry_lookup is not supported for metadata field '{field_name}'")

    src = field_map[field_name]
    if src not in row.index:
        raise ValueError(f"Station registry is missing column '{src}' required for '{field_name}'")

    val = row[src]
    if pd.isna(val):
        raise ValueError(f"Station registry column '{src}' is null for station")

    if field_name in ("latitude", "longitude", "projection_x_coordinate", "projection_y_coordinate"):
        return float(val)
    return val


def _infer_meta_from_template_path(fpath, listing):
    pattern = listing["collect"]["file_pattern"]
    name = listing.get("name", "<unnamed>")
    if not _pattern_is_template(pattern):
        raise ValueError(f"{name}: collect.file_pattern must be a filename template when using infer_from_filename")
    return interpret_fname(os.path.basename(fpath), naming=naming_spec(templates=[pattern]))


def _resolve_metadata_value(field_name, raw_value, inferred_meta, registry_row):
    if raw_value == "infer_from_filename":
        if field_name not in inferred_meta or inferred_meta[field_name] in (None, ""):
            raise ValueError(f"Could not infer metadata field '{field_name}' from filename")
        return inferred_meta[field_name]

    if raw_value == "registry_lookup":
        if registry_row is None:
            raise ValueError(f"registry_lookup requested for '{field_name}' but no registry row is available")
        return _registry_lookup_value(registry_row, field_name)

    return raw_value


def _registry_row_for_metadata(name, merged, repo_name):
    rcfg = repo_config(repo_name)
    site_key = rcfg["site_key"]
    slookup = repo_registry(repo_name)

    if site_key in merged and merged[site_key] not in (None, "", "infer_from_filename"):
        sid = str(merged[site_key]).strip()
        if sid in slookup.index:
            return slookup.loc[sid]
        raise ValueError(f"{name}: {site_key} '{sid}' not found in registry for repo {repo_name!r}")

    if "agency_id" in merged and merged["agency_id"] not in (None, "", "infer_from_filename"):
        aid = str(merged["agency_id"]).strip()
        hits = slookup[slookup["agency_id"].astype(str).str.strip() == aid]
        if hits.empty:
            raise ValueError(f"{name}: agency_id '{aid}' not found in registry for repo {repo_name!r}")
        if len(hits) > 1:
            raise ValueError(f"{name}: agency_id '{aid}' matched multiple rows in registry for repo {repo_name!r}")
        return hits.iloc[0]

    return None

def populate_meta(fpath, listing, repo_name, meta_out=None):
    rcfg = repo_config(repo_name)
    site_key = rcfg["site_key"]

    meta = dict(listing.get("metadata", {}) or {})
    name = listing.get("name", "<unnamed>")
    inferred = dict(meta_out or {})

    out = {}
    for field_name, raw_value in meta.items():
        if raw_value == "registry_lookup":
            continue
        out[field_name] = _resolve_metadata_value(field_name, raw_value, inferred, None)

    for field_name, value in inferred.items():
        if field_name not in out:
            out[field_name] = value

    if site_key not in out:
        raise ValueError(f"{name}: Missing '{site_key}' after metadata resolution.")

    if str(out[site_key]) == "infer_from_agency_id":
        if "agency_id" not in out or out["agency_id"] in (None, ""):
            raise ValueError(f"{name}: {site_key}=infer_from_agency_id requires agency_id.")
        slookup = repo_registry(repo_name)
        hits = slookup[slookup["agency_id"].astype(str).str.strip() == str(out["agency_id"]).strip()]
        if hits.empty:
            raise ValueError(f"{name}: agency_id '{out['agency_id']}' not found in registry for repo {repo_name!r}.")
        if len(hits) > 1:
            raise ValueError(f"{name}: agency_id '{out['agency_id']}' matched multiple rows in registry for repo {repo_name!r}.")
        out[site_key] = hits.index[0]

    registry_row = _registry_row_for_metadata(name, out, repo_name)

    for field_name, raw_value in meta.items():
        if raw_value == "registry_lookup":
            out[field_name] = _resolve_metadata_value(field_name, raw_value, inferred, registry_row)

    return out

def infer_meta(fpath, listing, fail="none"):
    print(listing)
    meta_string = listing["metadata_infer"]["regex"]
    print(meta_string)
    meta_re = re.compile(meta_string)
    extractables = listing["metadata_infer"]["groups"]
    meta = {}
    for key, val in extractables.items():
        ndx = int(key)
        print(key, val)
        try:
            m = meta_re.match(fpath)
            meta[val] = m.group(ndx)
        except:
            meta[val] = None
    return meta


def _apply_transforms(ts, transforms):
    """
    Apply transforms in-order.

    Expected YAML:
      transforms:
        - dst_st
        - {name: dst_st, args: {...}}
    """
    if not transforms:
        return ts

    for t in transforms:
        if isinstance(t, str):
            tname = t
            targs = {}
        elif isinstance(t, dict):
            if "name" not in t:
                raise ValueError("Each transform dict must have 'name'")
            allowed_keys = {"name", "args"}
            invalid_keys = set(t.keys()) - allowed_keys
            if invalid_keys:
                raise ValueError(f"Transform dict has invalid keys: {invalid_keys}. Only 'name' and 'args' are allowed.")
            tname = t["name"]
            targs = t.get("args", {}) or {}
        else:
            raise ValueError(f"Transform must be str or dict, got {type(t)}")
 

        if tname not in _TRANSFORMS:
            raise ValueError(f"Unknown transform '{tname}'")
        else:
            logger.debug("dropbox: applying transform=%s args=%s", tname, targs)
        ts = _TRANSFORMS[tname](ts, **targs)

    return ts


def _maybe_rename_value_column(ts, splice_args):
    """
    If splice_args requests a rename, do it here in a simple, explicit way.

    Supported:
      splice_args:
        rename: value        # rename single column to 'value'
      splice_args:
        rename: {old: new}   # dict rename
    """
    if not splice_args:
        return ts
    if "rename" not in splice_args:
        return ts

    ren = splice_args["rename"]
    if isinstance(ren, str):
        # rename first/only column to ren
        try:
            ts = ts.copy()
            ts.columns = [ren]
            return ts
        except Exception as e:
            raise ValueError(f"splice_args.rename='{ren}' failed: {e}")
    if isinstance(ren, dict):
        return ts.rename(columns=ren)
    raise ValueError(f"splice_args.rename must be str or dict, got {type(ren)}")


def _check_metadata(meta, repo_name):
    rcfg = repo_config(repo_name)
    site_key = rcfg["site_key"]

    required = [
        site_key,
        "subloc",
        "source",
        "agency",
        "param",
        "unit",
        "time_zone",
    ]
    # Explicitly forbid legacy key
    if "sublocation" in meta:
        raise ValueError(
            "Metadata key 'sublocation' is no longer supported. "
            "Use 'subloc' instead (e.g., 'default', 'upper', 'lower')."
        )
    for k in required:
        if k not in meta:
            raise ValueError(f"Missing required metadata field '{k}'")

    def _require_lower(name):
        val = meta[name]
        if val is None:
            return
        if not isinstance(val, str):
            raise ValueError(f"Metadata '{name}' must be a string, got {type(val)}")
        if val != val.lower():
            raise ValueError(
                f"Metadata '{name}' must be lower case. Got '{val}'"
            )

    # lowercase-enforced fields

    _require_lower("source")
    _require_lower("agency")
    _require_lower("param")
    _require_lower(site_key)
    subloc = meta["subloc"]

    if subloc is None or str(subloc).strip().lower() in ["", "none"]:
        meta["subloc"] = "default"
        subloc = "default"

    if not isinstance(subloc, str):
        raise ValueError(f"Metadata 'subloc' must be a string, got {type(subloc)}")

    if subloc != subloc.lower():
        raise ValueError(f"Metadata 'subloc' must be lower case. Got '{subloc}'")


    # unit: case preserved, but must be vtools compatible
    if not isinstance(meta["unit"], str):
        raise ValueError(f"Metadata 'unit' must be a string, got {type(meta['unit'])}")

    # time_zone: must be DST-compatible and have common name
    tz = meta["time_zone"]
    if not isinstance(tz, str):
        raise ValueError("Metadata 'time_zone' must be a string")

    try:
        pd.Timestamp("2000-01-01", tz=tz)
    except Exception as e:
        raise ValueError(f"Invalid time_zone '{tz}': {e}. Must be a valid timezone name recognized by pandas and compatible with vtools.dst_st.")

    try:
        # dst_st validates timezone semantics by application
        # We don't apply it, just verify compatibility
        _ = dst_st
    except Exception:
        raise ValueError(f"time_zone '{tz}' is not DST-compatible via vtools.dst_st")

    # spatial checks: either lat/lon OR projected coords
    lat = meta.get("latitude", None)
    lon = meta.get("longitude", None)
    x = meta.get("projection_x_coordinate", None)
    y = meta.get("projection_y_coordinate", None)

    has_ll = (
        isinstance(lat, (int, float))
        and isinstance(lon, (int, float))
        and lat != -9999.0
        and lon != -9999.0
    )

    has_xy = (
        isinstance(x, (int, float))
        and isinstance(y, (int, float))
        and x != -9999.0
        and y != -9999.0
    )

    if not (has_ll or has_xy):
        raise ValueError(
            "Metadata must include valid latitude/longitude or projected_x_coordinates (and y)"
        )


def apply_dropbox_workflow(spec, selected_names=None):
    logger.info("dropbox: loaded %d recipe entries", len(spec["data"]))
    always_skip = True

    selected_names = None if not selected_names else set(selected_names)
    seen_names = set()
    failures = []
    successes = []

    for listing in spec["data"]:
        name = listing.get("name", "<unnamed>")
        seen_names.add(name)

        if selected_names is not None and name not in selected_names:
            continue
        try:
            logger.info("dropbox: processing listing=%s", name)
            if "skip" in listing and always_skip:
                if listing["skip"] in ["True", True]:
                    logger.info("dropbox: skipping listing=%s (skip=True)", name)
                    continue

            item = listing["collect"]
            output = listing.get("output", {}) or {}
            repo_name = output.get("repo_name", None)
            if repo_name is None:
                raise ValueError(f"{name}: missing required 'output.repo_name'")

            staging_cfg = output.get("staging", {}) or {}
            dest = staging_cfg.get("dir", None)
            if dest is None:
                raise ValueError(f"{name}: missing required 'output.staging.dir'")
            if not os.path.exists(dest):
                raise ValueError(f"{name}: output.staging.dir does not exist: {dest}")

            reconcile_cfg = output.get("reconcile", None)

            file_pattern = item["file_pattern"]
            location = item["location"]

            # With OmegaConf, path composition should be done via interpolation in YAML,
            # e.g. "${dropbox_home}/ebmud" rather than Python .format(...).
            for field_name, field_val in [
                ("collect.file_pattern", file_pattern),
                ("collect.location", location),
            ]:
                if isinstance(field_val, str) and "{dropbox_home}" in field_val:
                    raise ValueError(
                        f"{name}: {field_name} still contains '{{dropbox_home}}'. "
                        "Update the YAML to use OmegaConf interpolation instead, e.g. '${dropbox_home}/...'."
                    )

            recursive = bool(item["recursive_search"])

            collector = DataCollector("dummy", location, file_pattern, recursive)
            wildcard = item.get(
                "wildcard", None
            )  # expected: time_shard | time_overlap | None
            uses_filename_inference = _metadata_uses_filename_inference(listing.get("metadata", {}))
            uses_registry_lookup = _metadata_uses_registry_lookup(listing.get("metadata", {}))
            uses_template_pattern = _pattern_is_template(file_pattern)

            if uses_filename_inference and not uses_template_pattern:
                raise ValueError(
                    f"{name}: infer_from_filename requires collect.file_pattern to be a filename template"
                )

            inference_mode = uses_filename_inference or (uses_registry_lookup and uses_template_pattern)
            logger.info(
               "dropbox: listing=%s location=%s pattern=%s wildcard=%s inference_mode=%s",
                name, location, file_pattern, wildcard, inference_mode
           )
            reader = reader_for(item["reader"])
            if reader is None:
                raise ValueError(f"{name}: unknown reader '{item['reader']}'")

            selector = item.get("selector", None)
            input_metadata = listing["metadata"]
            reader_args = item.get("reader_args", {}) or {}
            # transforms live at the listing level in the YAML (sibling to collect/metadata/output).
            # Also accept legacy key "transform" (singular) if present.
            transforms = listing.get("transforms", None)
            if transforms is None:
                transforms = listing.get("transform", []) or []
            transforms = transforms or []
            splice_args = item.get("splice_args", {}) or {}

            # --- Read according to wildcard interpretation
            series_list = []
            meta_source_path = None

            if inference_mode:
                if wildcard is not None:
                    raise ValueError(
                        f"{name}: filename/template inference mode does not support collect.wildcard; omit it"
                    )
                template_glob = _template_to_glob(file_pattern)
                matched = sorted(glob.glob(os.path.join(location, template_glob), recursive=collector.recursive))
                if not matched:
                    raise ValueError(
                        f"{name}: filename template matched no files: {os.path.join(location, template_glob)}"
                    )
                per_file_results = []
                for fpath in matched:
                    logger.debug("dropbox: listing=%s reading file=%s", name, fpath)
                    try:
                        ts = reader(
                            fpath,
                            selector=selector,
                            freq=input_metadata["freq"],
                            **reader_args,
                        )
                    except Exception:
                        logger.exception(
                            "dropbox: READ FAILED listing=%s file=%s pattern=%s selector=%r reader_args=%r",
                            name, fpath, file_pattern, selector, reader_args,
                        )
                        raise

                    ts = _maybe_rename_value_column(ts, splice_args)
                    if not isinstance(ts.index, pd.DatetimeIndex):
                        raise ValueError(f"{name}: reader did not return DatetimeIndex for file {fpath}")
                    per_file_results.append((fpath, ts))

            elif wildcard == "time_shard":
                # vtools read_ts style: pass the glob directly
                fglob = collector.data_file_glob()
                logger.debug("dropbox: listing=%s reading glob=%s", name, fglob)
                try:
                    ts = reader(
                        fglob,
                        selector=selector,
                        freq=input_metadata["freq"],
                        **reader_args,
                    )
                except Exception:
                    logger.exception(
                        "dropbox: READ FAILED listing=%s file=%s pattern=%s selector=%r reader_args=%r",
                        name, fpath, file_pattern, selector, reader_args,
                    )
                    raise           
                ts = _maybe_rename_value_column(ts, splice_args)
                series_list = [ts]
                meta_source_path = fglob

            elif wildcard == "time_overlap":
                # expand/sort/read each; then splice/merge
                allfiles = sorted(collector.data_file_list())
                if not allfiles:
                    raise ValueError(
                        f"{name}: glob matched no files: {collector.data_file_glob()}"
                    )
                meta_source_path = allfiles[0]
                for fpath in allfiles:
                    logger.debug("dropbox: listing=%s reading file=%s", name, fpath)
                    try:
                        ts = reader(
                            fpath,
                            selector=selector,
                            freq=input_metadata["freq"],
                            **reader_args,
                        )
                    except Exception:
                        logger.exception(
                            "dropbox: READ FAILED listing=%s file=%s pattern=%s selector=%r reader_args=%r",
                            name, fpath, file_pattern, selector, reader_args,
                        )
                        raise

                    if not isinstance(ts.index, pd.DatetimeIndex):
                        raise ValueError(
                            f"{name}: reader did not return DatetimeIndex for file {fpath}"
                        )
                    ts = _maybe_rename_value_column(ts, splice_args)
                    series_list.append(ts)

            elif wildcard is None:
                # single file (no wildcard semantics)
                fglob = collector.data_file_glob()
                matched = sorted(glob.glob(fglob, recursive=collector.recursive))
                if not matched:
                    raise ValueError(f"{name}: file pattern matched no files: {fglob}")
                if len(matched) > 1:
                    raise ValueError(
                        f"{name}: collect.wildcard omitted but pattern matched multiple files: {matched}"
                    )
                meta_source_path = matched[0]
                try:
                    ts = reader(
                        meta_source_path,
                        selector=selector,
                        freq=input_metadata["freq"],
                        **reader_args,
                    )
                except Exception:
                    logger.exception(
                        "dropbox: READ FAILED listing=%s file=%s pattern=%s selector=%r reader_args=%r",
                        name, meta_source_path, file_pattern, selector, reader_args,
                    )
                    raise
                
                ts = _maybe_rename_value_column(ts, splice_args)
                series_list = [ts]

            else:
                raise ValueError(
                    f"{name}: collect.wildcard must be 'time_shard', 'time_overlap', or omitted; got '{wildcard}'"
                )

            # --- Combine if needed (combine BEFORE transforms to avoid creating duplicate
            # timestamps via DST conversion, and because overlap feeds should resolve
            # duplicates in the combine step)
            outputs_to_write = []

            if inference_mode:
                for meta_source_path, ts in per_file_results:
                    ts = _apply_transforms(ts, transforms)
                    inferred_meta = _infer_meta_from_template_path(meta_source_path, listing)
                    meta_out = populate_meta(
                        meta_source_path,
                        listing,
                        repo_name=repo_name,
                        meta_out=inferred_meta,
                    )

                    if meta_out["freq"] == "infer":
                        meta_out["freq"] = infer_freq_robust(ts.index)

                    if meta_out["freq"] is None or meta_out["freq"] == "None":
                        meta_out["freq"] = "irregular"

                    _check_metadata(meta_out, repo_name)
                    outputs_to_write.append((ts, meta_out))
            else:
                if len(series_list) == 1:
                    ts = series_list[0]
                else:
                    merge_method = item.get("merge_method", "ts_splice")
                    merge_args = item.get("merge_args", {}) or {}

                    if merge_method == "ts_splice":
                        ts = ts_splice(series_list, **merge_args)
                    elif merge_method == "ts_merge":
                        ts = ts_merge(series_list, **merge_args)
                    else:
                        raise ValueError(
                            f"{name}: merge_method must be 'ts_splice' or 'ts_merge', "
                            f"got '{merge_method}'"
                        )

                ts = _apply_transforms(ts, transforms)
                inferring_meta = "metadata_infer" in listing
                if inferring_meta:
                    inferred_meta = infer_meta(meta_source_path, listing)
                else:
                    inferred_meta = {}

                meta_out = populate_meta(meta_source_path, listing, repo_name, inferred_meta)

                if meta_out["freq"] == "infer":
                    meta_out["freq"] = infer_freq_robust(ts.index)

                if meta_out["freq"] is None or meta_out["freq"] == "None":
                    meta_out["freq"] = "irregular"

                _check_metadata(meta_out, repo_name)
                outputs_to_write.append((ts, meta_out))
            logger.info(
                "dropbox: listing=%s writing %d output file(s) to %s",
                name, len(outputs_to_write), dest
            )
            write_args = dict(staging_cfg.get("write_args", {"float_format": "%.4f"}) or {})
            for ts, meta_out in outputs_to_write:
                fname_out = meta_to_filename(meta_out, repo=repo_name, include_shard=False)
                fname_out = os.path.join(dest, fname_out)
                write_ts_csv(ts, fname_out, metadata=meta_out, **write_args)

            if reconcile_cfg is not None:
                inspection_cfg = reconcile_cfg.get("inspection", {}) or {}

                # Physical destination for reconcile writes/reads:
                # - explicit scratch/debug path if provided
                # - otherwise configured root for repo_name
                repo_data_dir = reconcile_cfg.get("repo_data_dir", repo_name)
                logger.info(
                    "dropbox: listing=%s reconcile staged_dir=%s repo_target=%s",
                    name, dest, repo_data_dir
                )

                update_repo(
                    staged_dir=dest,
                    repo_dir=repo_data_dir,
                    prefer=reconcile_cfg.get("prefer", "staged"),
                    allow_new_series=reconcile_cfg.get("allow_new_series", True),
                    recent_years=inspection_cfg.get("recent_years", 3),
                    p3=inspection_cfg.get("p3", 0.15),
                    p10=inspection_cfg.get("p10", 0.05),
                )
        except Exception as e:
            logger.exception("dropbox: FAILED listing=%s", name)
            failures.append(
                {
                    "name": name,
                    "error_type": type(e).__name__,
                    "message": str(e),
                }
            )
            continue

    if selected_names is not None:
            missing = selected_names - seen_names
            if missing:
                raise ValueError(
                    f"Requested recipe name(s) not found in YAML: {sorted(missing)}"
                )
    logger.info(
        "dropbox: completed run with %d succeeded, %d failed",
        len(successes),
        len(failures),
    )

    if failures:
        failed_names = [f["name"] for f in failures]
        logger.error("dropbox: failed listings=%s", failed_names)
        raise RuntimeError(
            "One or more recipe entries failed. "
            f"Failed listings: {failed_names}. "
            "Use --name <recipe> to rerun and repair individual entries."
        )            

def dropbox_data(spec_fname, selected_names=None):
    spec_fname = (
        spec_fname
        if os.path.exists(spec_fname)
        else config_file(spec_fname)
    )
    spec = get_spec(spec_fname)
    apply_dropbox_workflow(spec, selected_names=selected_names)


@click.command(name="dropbox")
@click.option(
    "--input",
    "spec_fname",
    required=True,
    type=str,
    help="YAML file with dropbox specification.",)
@click.option(
    "--name",
    "selected_names",
    multiple=True,
    help="Run only the named recipe entry or entries from the YAML.",
)
@click.option("--logdir", type=click.Path(path_type=Path), default=None, help="Optional log directory.")
@click.option("--debug", is_flag=True, help="Enable debug logging and per-file output.")
@click.option("--quiet", is_flag=True, help="Disable console logging.")
def dropbox_cli(spec_fname,selected_names, logdir, debug, quiet): 
    """Read unformatted data files and write formatted CSV files per dropbox spec."""
    level, console = resolve_loglevel(debug=debug, quiet=quiet)
    configure_logging(
        package_name="dms_datastore",
        level=level,
        console=console,
        logdir=logdir,
    )
    dropbox_data(spec_fname,selected_names)


if __name__ == "__main__":
    dropbox_cli()
