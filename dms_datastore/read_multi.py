#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import glob
import logging
import pandas as pd

from dms_datastore.read_ts import read_ts, read_yaml_header
from dms_datastore import dstore_config
from dms_datastore.filename import build_repo_globs, interpret_fname
from vtools.functions.merge import ts_merge, ts_splice
from vtools.functions.unit_conversions import *
from vtools.data.vtime import compare_interval, to_timedelta
from decimal import Decimal, InvalidOperation

logger = logging.getLogger(__name__)

__all__ = ["read_ts_repo", "ts_multifile_read", "resolve_providers_for_repo"]


def infer_source_priority(station_id):
    if "source_priority" in dstore_config.config:
        priorities = dstore_config.config["source_priority"]
    else:
        priorities = dstore_config.config.get("source_priority_groups", {})

    db = dstore_config.station_dbase()
    agency = db.loc[station_id, "agency"]
    return priorities[agency] if agency in priorities else None


def resolve_providers_for_repo(key, repo_cfg, provider_priority="infer"):
    if provider_priority != "infer":
        if provider_priority is None:
            return None
        if isinstance(provider_priority, str):
            return [provider_priority]
        return list(provider_priority)

    mode = repo_cfg["provider_resolution_mode"]

    if mode == "assume_unique":
        return None

    if mode == "registry_column":
        registry = dstore_config.repo_registry(repo_cfg=repo_cfg)
        bare_key = key.split("@", 1)[0]

        if bare_key not in registry.index:
            return None

        col = repo_cfg["provider_resolution_column"]
        if col not in registry.columns:
            raise ValueError(
                f"Registry for repo {repo_cfg.get('name')!r} "
                f"is missing provider resolution column {col!r}"
            )

        resolution_key = registry.loc[bare_key, col]
        order_map = repo_cfg.get("provider_resolution_order", {})

        if resolution_key not in order_map:
            return None

        resolved = order_map[resolution_key]
        if isinstance(resolved, str):
            return [resolved]
        return list(resolved)

    raise ValueError(
        f"Unsupported provider_resolution_mode {mode!r} "
        f"for repo {repo_cfg.get('name')!r}"
    )


def fahren2cel(ts):
    tsout = fahrenheit_to_celsius(ts)
    tsout = tsout.round(2)
    return tsout


def read_ts_repo(
    station_id,
    variable,
    subloc=None,
    repo=None,
    provider_priority="infer",
    start=None,
    end=None,
    meta=False,
    force_regular=True,
    modifier=None,
    data_path=None,
    freq_resolver=None,
):
    if repo is None:
        repo = dstore_config.config.get("default_repo", "screened")

    if subloc is not None:
        if "@" in station_id:
            raise ValueError("@ short hand and subloc are mutually exclusive")
        if subloc != "default":
            station_id = f"{station_id}@{subloc}"

    repo_cfg = dstore_config.repo_config(repo)
    repository = data_path if data_path is not None else repo_cfg["root"]

    start = pd.to_datetime(start) if start is not None else None
    end = pd.to_datetime(end) if end is not None else None

    providers = resolve_providers_for_repo(
        station_id,
        repo_cfg,
        provider_priority=provider_priority,
    )

    rel_pats = build_repo_globs(
        repo_cfg,
        key=station_id,
        param=variable,
        subloc=subloc,
        modifier=modifier,
        providers=providers,
        agency_id="*",
        year="*",
        syear="*",
        eyear="*",
    )
    pats = [os.path.join(repository, p) for p in rel_pats]

    retval = ts_multifile(
        pats,
        meta=meta,
        start=start,
        end=end,
        force_regular=force_regular,
        repo=repo_cfg.get("name"),
        freq_resolver=freq_resolver,
    )
    return retval


def detect_dms_unit(fname):
    meta = read_yaml_header(fname)
    unit = meta["unit"] if "unit" in meta else None
    if unit in ["FNU", "NTU"]:
        return "FNU", None
    elif unit in ["uS/cm", "microS/cm"]:
        return "microS/cm", None
    elif unit == "meters":
        return "meters", None
    elif unit == "cfs":
        return "ft^3/s", None
    elif unit == "deg_f":
        return "deg_c", fahren2cel
    else:
        return unit, None


def filter_date(metafname, start=None, end=None):
    start = pd.to_datetime(start)
    end = pd.to_datetime(end)
    if "year" in metafname:
        yr = int(metafname["year"])
        if start is None:
            syr = 0
        else:
            syr = start if type(start) == int else start.year
        if end is None:
            eyr = 3000
        else:
            eyr = end if type(end) == int else end.year
        return yr < syr or yr > eyr
    else:
        return False


def _freq_to_label(freq):
    if freq is None:
        return None
    try:
        off = pd.tseries.frequencies.to_offset(freq)
    except Exception:
        return str(freq)
    return off.freqstr


def _freq_to_offset(freq):
    if freq is None:
        return None
    return pd.tseries.frequencies.to_offset(freq)


def _series_freq_label(ts):
    freq = getattr(ts.index, "freq", None)
    if freq is None:
        return None
    return _freq_to_label(freq)


def _choose_target_freq(freq_labels, mode, ordered_items):
    offsets = [_freq_to_offset(f) for f in freq_labels]
    if any(x is None for x in offsets):
        raise ValueError(
            f"Cannot resolve frequencies because some series have no regular freq: {freq_labels}"
        )

    if mode == "finer":
        best = offsets[0]
        for off in offsets[1:]:
            if compare_interval(off, best) < 0:
                best = off
        return best

    if mode == "coarser":
        best = offsets[0]
        for off in offsets[1:]:
            if compare_interval(off, best) > 0:
                best = off
        return best

    if mode == "latest":
        if len(ordered_items) == 0:
            raise ValueError("No series provided to latest-frequency chooser")
        last_item = ordered_items[-1]
        return _freq_to_offset(last_item["freq_label"])

    raise ValueError(f"Unknown target selection mode {mode!r}")


def _transform_asfreq(ts, target_freq):
    return ts.asfreq(target_freq)

def _decimal_places_from_value(v):
    """
    Infer decimal places from a numeric value by converting through string form.
    Returns None for NaN/non-finite values.
    """
    if pd.isna(v):
        return None

    try:
        d = Decimal(str(v)).normalize()
    except (InvalidOperation, ValueError):
        return None

    exp = d.as_tuple().exponent
    return 0 if exp >= 0 else -exp


def _infer_series_decimal_places(ts, max_places=4):
    """
    Infer a typical decimal precision from observed (non-NaN) source values.

    Uses the median number of decimal places across all numeric values, capped
    to max_places.
    """
    vals = ts.to_numpy().ravel()
    places = [_decimal_places_from_value(v) for v in vals]
    places = [p for p in places if p is not None]

    if not places:
        return None

    # robust to a few oddball values
    inferred = int(pd.Series(places).median())
    return max(0, min(inferred, max_places))

def _transform_interp(ts, target_freq):
    # infer source precision before interpolation
    round_places = _infer_series_decimal_places(ts)

    # build a canonical grid aligned to the target frequency, not to the
    # first timestamp in the source file
    start = ts.index.min().floor(target_freq)
    end = ts.index.max().ceil(target_freq)

    new_index = pd.date_range(
        start=start,
        end=end,
        freq=target_freq,
    )

    out = ts.reindex(ts.index.union(new_index)).sort_index()
    out = out.interpolate(method="time")
    out = out.reindex(new_index)
    out.index.name = ts.index.name

    if round_places is not None:
        out = out.round(round_places)

    return out


def _parse_freq_resolver(freq_resolver):
    if freq_resolver is None:
        return None

    if isinstance(freq_resolver, str):
        mapping = {
            "interp_to_finer": ("finer", "interp"),
            "as_freq_finer": ("finer", "asfreq"),
            "interp_to_coarser": ("coarser", "interp"),
            "as_freq_coarser": ("coarser", "asfreq"),
            "interp_to_latest": ("latest", "interp"),
            "as_freq_latest": ("latest", "asfreq"),
        }
        if freq_resolver not in mapping:
            raise ValueError(
                f"Unknown freq_resolver {freq_resolver!r}. "
                f"Expected one of: {sorted(mapping.keys())}"
            )
        return mapping[freq_resolver]

    if isinstance(freq_resolver, dict):
        target_mode = freq_resolver.get("target")
        method = freq_resolver.get("method")
        target_freq = freq_resolver.get("target_freq")

        if target_freq is not None:
            return ("explicit", method, _freq_to_offset(target_freq))

        if target_mode not in ("finer", "coarser", "latest"):
            raise ValueError(
                "freq_resolver dict must specify target in "
                "{'finer','coarser','latest'} or explicit target_freq"
            )
        if method not in ("interp", "asfreq"):
            raise ValueError(
                "freq_resolver dict must specify method in {'interp','asfreq'}"
            )
        return (target_mode, method)

    raise ValueError(
        "freq_resolver must be None, a recognized string, or a dict specification"
    )


def _resolve_frequency_transition(ordered_items, freq_resolver):
    freq_labels = [item["freq_label"] for item in ordered_items]
    unique = []
    for f in freq_labels:
        if f not in unique:
            unique.append(f)

    if len(unique) <= 1:
        target = _freq_to_offset(unique[0]) if unique else None
        return target, None

    parsed = _parse_freq_resolver(freq_resolver)
    if parsed is None:
        raise ValueError(
            "Found these frequencies: "
            f"{unique}. No frequency resolver is configured for this transition. "
            "See the freq_resolver argument documentation."
        )

    if parsed[0] == "explicit":
        _, method, target = parsed
    else:
        target_mode, method = parsed
        target = _choose_target_freq(unique, target_mode, ordered_items)

    if method == "asfreq":
        transform = _transform_asfreq
    elif method == "interp":
        transform = _transform_interp
    else:
        raise ValueError(f"Unknown transform method {method!r}")

    return target, transform


def _apply_freq_resolution(ordered_items, freq_resolver):
    target_freq, transform = _resolve_frequency_transition(ordered_items, freq_resolver)
    if target_freq is None:
        return ordered_items, None

    if transform is None:
        return ordered_items, target_freq

    resolved = []
    for item in ordered_items:
        ts = transform(item["ts"], target_freq)
        new_label = _series_freq_label(ts)
        resolved.append(
            {
                **item,
                "ts": ts,
                "freq_label": new_label,
            }
        )

    final_labels = []
    for item in resolved:
        lbl = item["freq_label"]
        if lbl not in final_labels:
            final_labels.append(lbl)

    if len(final_labels) > 1:
        raise ValueError(
            "Frequency resolver did not produce a single reconciled frequency. "
            f"Resulting frequencies: {final_labels}"
        )

    return resolved, target_freq



def ts_multifile(
    pats,
    selector=None,
    column_names=None,
    start=None,
    end=None,
    meta=False,
    force_regular=True,
    repo=None,
    freq_resolver=None
):
    """
    Read and merge/splice multiple time series files based on provided patterns.

    Parameters
    ----------
    pats : str or list of str
        File patterns for time series data.
    selector : str, optional
        Column name to select if multiple columns exist.
    column_names : str or list of str, optional
        New column names for the output.
    start : str, pandas.Timestamp, or None, optional
        Start date for filtering data.
    end : str, pandas.Timestamp, or None, optional
        End date for filtering data.
    meta : bool, optional
        Whether to return metadata.
    force_regular : bool, optional
        Whether to enforce a regular time step.

    Returns
    -------
    pandas.DataFrame or tuple
        Merged time series data, or a tuple of metadata and data if meta=True.
    """

    start = pd.to_datetime(start) if start is not None else None
    end = pd.to_datetime(end) if end is not None else None

    if not (isinstance(pats, list)):
        pats = [pats]

    units = []
    metas = []
    some_files = False
    pats_revised = []  # for culling empty patterns
    for fp in pats:
        tsfiles = sorted(glob.glob(fp))
        if len(tsfiles) == 0:
            print(f"No files for pattern {fp}")
            continue
        else:
            pats_revised.append(fp)
        # assume consistency within each pattern
        unit, transform = detect_dms_unit(tsfiles[0])
        units.append((unit, transform))
        example_header = read_yaml_header(tsfiles[0])
        example_header["unit"] = unit
        metas.append(example_header)
        some_files = True
    pats = pats_revised
    if not some_files:
        print(f"No files for pats")
        return None


    bigts = []  # list of merged time series, one per pattern

    for fp, utrans in zip(pats, units):  # loop through patterns
        items = []
        tsfiles = sorted(glob.glob(fp))
        unit, transform = utrans

        for tsfile in tsfiles:  # loop through files in pattern
            # read one by one, not by pattern/wildcard
            metafname = interpret_fname(os.path.basename(tsfile), repo=repo)
            if filter_date(metafname, start, end):
                continue

            ts = read_ts(tsfile, force_regular=force_regular)

            dup_mask = ts.index.duplicated(keep=False)
            if dup_mask.any():
                dup_index = ts.index[dup_mask]
                unique_dups = dup_index.unique()

                first = unique_dups[0]
                last = unique_dups[-1]

                example_first = ts.loc[first]
                example_last = ts.loc[last]

                raise ValueError(
                    f"Duplicate index detected in file {tsfile}\n"
                    f"Duplicate timestamps: {len(unique_dups)} "
                    f"(total duplicate rows: {dup_mask.sum()})\n"
                    f"First duplicate: {first}\n"
                    f"Last duplicate: {last}\n\n"
                    f"Example at first duplicate:\n{example_first}\n\n"
                    f"Example at last duplicate:\n{example_last}"
                )

            if ts.shape[1] > 1:
                if selector is not None:
                    ts = ts[selector].to_frame()

            if column_names is not None:
                if isinstance(column_names, str):
                    cols = [column_names]
                else:
                    cols = list(column_names)
                ts.columns = cols

            # possibly apply unit transition
            ts = ts if transform is None else transform(ts)

            items.append(
                {
                    "path": tsfile,
                    "ts": ts,
                    "freq_label": _series_freq_label(ts),
                }
            )

        if len(items) == 0:
            print(f"No series for subpattern: {fp}")
            continue

        items, _ = _apply_freq_resolution(items, freq_resolver)

        patfull = ts_merge(list(reversed([item["ts"] for item in items])))
        bigts.append(patfull)

    if len(bigts) == 0:
        return None

    # now organize across patterns
    if len(bigts) == 1:
        fullout = bigts[0]
    else:
        pattern_items = [
            {
                "path": f"<pattern_{i}>",
                "ts": ts,
                "freq_label": _series_freq_label(ts),
            }
            for i, ts in enumerate(bigts)
        ]

        pattern_items, _ = _apply_freq_resolution(pattern_items, freq_resolver)

        fullout = ts_splice(
            [item["ts"] for item in pattern_items],
            transition="prefer_first",
        )

    fullout = fullout.loc[start:end]  # Will have already been filtered to about 1 year
    retval = (metas, fullout) if meta else fullout
    return retval


def ts_multifile_read(
    pats,
    transforms=None,
    selector=None,
    column_name=None,
    start=None,
    end=None,
    freq_resolver=None,
):
    """
    Read and merge multiple time series files with optional transformations.

    Parameters
    ----------
    pats : str or list of str
        File patterns for time series data.
    transforms : list of callable, optional
        Transformation functions to apply to each file.
    selector : str, optional
        Column name to select if multiple columns exist.
    column_name : str, optional
        New column name for the output.
    start : str, pandas.Timestamp, or None, optional
        Start date for filtering data.
    end : str, pandas.Timestamp, or None, optional
        End date for filtering data.

    Returns
    -------
    pandas.DataFrame
        Merged time series data.
    """

    if not isinstance(pats, list):
        pats = [pats]

    if transforms is None:
        transforms = [None] * len(pats)

    start = pd.to_datetime(start) if start is not None else None
    end = pd.to_datetime(end) if end is not None else None

    tss = []
    for fp, trans in zip(pats, transforms):
        tsfiles = sorted(glob.glob(fp))
        for tsfile in tsfiles:
            ts = read_ts(tsfile)

            dup_mask = ts.index.duplicated(keep=False)
            if dup_mask.any():
                dup_index = ts.index[dup_mask]
                unique_dups = dup_index.unique()

                first = unique_dups[0]
                last = unique_dups[-1]

                example_first = ts.loc[first]
                example_last = ts.loc[last]

                raise ValueError(
                    f"Duplicate index detected in file {tsfile}\n"
                    f"Duplicate timestamps: {len(unique_dups)} "
                    f"(total duplicate rows: {dup_mask.sum()})\n"
                    f"First duplicate: {first}\n"
                    f"Last duplicate: {last}\n\n"
                    f"Example at first duplicate:\n{example_first}\n\n"
                    f"Example at last duplicate:\n{example_last}"
                )

            if ts.shape[1] > 1:
                if selector is None:
                    ts = ts.mean(axis=1).to_frame()
                else:
                    ts = ts[selector].to_frame()

            if column_name is not None:
                ts.columns = [column_name]

            ts = ts if trans is None else trans(ts)
            tss.append(ts)

    if len(tss) == 0:
        for p in pats:
            print(p)
        raise ValueError("Patterns produced no matches")

    items = [
        {
            "path": f"<file_{i}>",
            "ts": ts,
            "freq_label": _series_freq_label(ts),
        }
        for i, ts in enumerate(tss)
    ]

    items, _ = _apply_freq_resolution(items, freq_resolver)

    full = ts_merge([item["ts"] for item in items])
    full = full.loc[start:end]
    return full



