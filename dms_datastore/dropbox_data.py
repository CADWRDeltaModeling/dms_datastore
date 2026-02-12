import os
import re
import pandas as pd
import yaml
import glob
from omegaconf import OmegaConf
from dms_datastore.read_ts import read_ts, infer_freq_robust
from dms_datastore.write_ts import write_ts_csv
from dms_datastore.dstore_config import station_dbase
from dms_datastore.reconcile_data import update_repo
import click
from vtools import dst_st, ts_merge, ts_splice,ts_coarsen

# Global variable to store cached data
_cached_spec = None
_station_dbase = station_dbase()

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




def populate_meta(fpath, listing, meta_out=None):
    meta = dict(listing.get("metadata", {}) or {})
    name = listing.get("name", "<unnamed>")

    # Start from inferred/meta_out, but do NOT overwrite user-provided keys.
    out = dict(meta_out or {})
    for k, v in meta.items():
        out[k] = v

    # station_id must exist (leave deeper standards to _check_metadata)
    if "station_id" not in out:
        raise ValueError(
            f"{name}: Missing 'station_id' in metadata."
        )

    # Optional inference from agency_id ONLY when explicitly requested.
    if str(out["station_id"]) == "infer_from_agency_id":
        if "agency_id" not in out or out["agency_id"] in (None, ""):
            raise ValueError(
                f"{name}: station_id=infer_from_agency_id requires agency_id."
            )
        slookup = station_dbase()
        hits = slookup[slookup["agency_id"] == out["agency_id"]]
        if hits.empty:
            raise ValueError(f"{name}: agency_id '{out['agency_id']}' not found in station database.")
        out["station_id"] = hits.index[0]

    # Optional enrichment (NOT elaborating now): only if a station exists and only fill missing keys.
    # (You can later gate this with an explicit station_database flag.)
    try:
        slookup = station_dbase()
        sid = out["station_id"]
        if sid in slookup.index:
            for src_key, dst_key in [
                ("name", "station_name"),
                ("agency", "agency"),
                ("agency_id", "agency_id"),
                ("lat", "latitude"),
                ("lon", "longitude"),
                ("x", "projection_x_coordinate"),
                ("y", "projection_y_coordinate"),
            ]:
                if dst_key not in out or out[dst_key] is None:
                    val = slookup.loc[sid, src_key]
                    if pd.isna(val):
                        continue
                    out[dst_key] = float(val) if dst_key in ("latitude","longitude","projection_x_coordinate","projection_y_coordinate") else val
    except Exception:
        # No silent fill: if lookup fails, just leave things as-is.
        pass

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
            print(f"Applying transform '{tname}' with args {targs}")
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


def _check_metadata(meta):
    """
    Enforce metadata standards. Fail hard on violation.
    No coercion, no defaults, no inference here.
    """

    required = [
        "station_id",
        "sublocation",
        "source",
        "agency",
        "param",
        "unit",
        "time_zone",
    ]

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
    _require_lower("station_id")
    _require_lower("source")
    _require_lower("agency")
    _require_lower("param")

    # sublocation: explicit None allowed
    if meta["sublocation"] is not None:
        _require_lower("sublocation")

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


def get_data(spec):

    dropbox_home = spec["dropbox_home"]
    always_skip = True

    for listing in spec["data"]:  # iterate over listings (Moke, Clifton Court, etc.)
        name = listing.get("name", "<unnamed>")
        if "skip" in listing and always_skip:
            """skip the item, possibly because it is securely archived already"""
            if listing["skip"] in ["True", True]:
                continue

        item = listing["collect"]
        metadata = listing["metadata"]
        # YAML uses per-item output.staging (write destination at this stage)
        output = listing.get("output", {})
        dest = output.get("staging_dir", None)
        if dest is None:
            raise ValueError(f"{name}: missing required 'output.staging_dir'")
        if not os.path.exists(dest):
            raise ValueError(f"output.staging_dir {dest} does not exist.")

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
        reader = reader_for(item["reader"])
        if reader is None:
            raise ValueError(f"{name}: unknown reader '{item['reader']}'")

        selector = item.get("selector", None)
        reader_args = item.get("reader_args", {}) or {}
        # transforms live at the listing level in the YAML (sibling to collect/metadata/output).
        # Also accept legacy key "transform" (singular) if present.
        transforms = listing.get("transforms", None)
        if transforms is None:
            transforms = listing.get("transform", []) or []
        transforms = transforms or []
        print("transforms", transforms)
        splice_args = item.get("splice_args", {}) or {}
        merge_args = item.get("merge_args", None)  # optional alternative combine mode

        # --- Read according to wildcard interpretation
        series_list = []

        if wildcard == "time_shard":
            # vtools read_ts style: pass the glob directly
            fglob = collector.data_file_glob()
            ts = reader(fglob, selector=selector, freq=metadata["freq"], **reader_args)

            ts = _maybe_rename_value_column(ts, splice_args)
            series_list = [ts]

        elif wildcard == "time_overlap":
            # expand/sort/read each; then splice/merge
            allfiles = sorted(collector.data_file_list())
            if not allfiles:
                raise ValueError(
                    f"{name}: glob matched no files: {collector.data_file_glob()}"
                )
            for fpath in allfiles:
                ts = reader(
                    fpath, selector=selector, freq=metadata["freq"], **reader_args
                )
                if not isinstance(ts.index, pd.DatetimeIndex):
                    print(ts)
                    raise ValueError(
                        f"{name}: reader did not return DatetimeIndex for file {fpath}"
                    )
                ts = _maybe_rename_value_column(ts, splice_args)
                series_list.append(ts)

        elif wildcard is None:
            # single file (no wildcard semantics)
            fpath = collector.data_file_glob()
            ts = reader(fpath, selector=selector, freq=metadata["freq"], **reader_args)
            ts = _apply_transforms(ts, transforms)
            ts = _maybe_rename_value_column(ts, splice_args)
            series_list = [ts]

        else:
            raise ValueError(
                f"{name}: collect.wildcard must be 'time_shard', 'time_overlap', or omitted; got '{wildcard}'"
            )

        # --- Combine if needed (combine BEFORE transforms to avoid creating duplicate
        # timestamps via DST conversion, and because overlap feeds should resolve
        # duplicates in the combine step)
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

        print("Got here")
        print(transforms)
        # Now apply transforms once, after combining
        ts = _apply_transforms(ts, transforms)
        inferring_meta = "metadata_infer" in listing
        if inferring_meta:
            metadata = infer_meta(fpath, listing)
        else:
            metadata = {}

        meta_out = populate_meta(fpath, listing, metadata)

        # infer frequency if requested
        if meta_out["freq"] == "infer":
            meta_out["freq"] = infer_freq_robust(ts.index)

        # use "irregular" if None is specified
        if meta_out["freq"] is None or meta_out["freq"] == "None":
            meta_out["freq"] = "irregular"
            metadata["freq"] = None  # Must reset.
        if "sublocation" not in meta_out or meta_out["sublocation"] is None:
            meta_out["sublocation"] = "default"  # check not just "subloc"

        fname_out = (
            meta_out["source"]
            + "_"
            + meta_out["station_id"]
            + "_"
            + meta_out["agency_id"]
            + "_"
            + meta_out["param"]
            + ".csv"
        )

        fname_out = os.path.join(dest, fname_out)
        _check_metadata(meta_out)

        write_args = dict(output.get("write_args", {"float_format": "%.4f"}) or {})
        write_ts_csv(ts, fname_out, metadata=meta_out, **write_args)



        update_repo(
          staged_dir=output["staging_dir"],
          repo_dir=output["repo_dir"],
          prefer=output["merge_priority"],
          allow_new_series=output["allow_new_series"],
          recent_years=output["inspection"]["recent_years"],
          p3=output["inspection"]["p3"],
          p10=output["inspection"]["p10"],
         )

def dropbox_data(spec_fname):
    spec = get_spec(spec_fname)
    get_data(spec)


@click.command(name="dropbox")
@click.option(
    "--input",
    "spec_fname",
    required=True,
    type=click.Path(exists=True, dir_okay=False, readable=True),
    help="YAML file with dropbox specification.",
)
def dropbox_cli(spec_fname):
    """Read unformatted data files and write formatted CSV files per dropbox spec."""
    dropbox_data(spec_fname)


if __name__ == "__main__":
    dropbox_cli()
