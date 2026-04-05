#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import glob
import pandas as pd
from dms_datastore.read_ts import read_ts, read_yaml_header
from dms_datastore import dstore_config
from dms_datastore.filename import build_repo_globs, interpret_fname
from vtools.functions.merge import ts_merge, ts_splice
from vtools.functions.unit_conversions import *

__all__ = ["read_ts_repo", "ts_multifile_read", "resolve_providers_for_repo"]



def infer_source_priority(station_id):
    """
    Legacy observation-oriented priority inference.
    """
    if "source_priority" in dstore_config.config:
        priorities = dstore_config.config["source_priority"]
    else:
        priorities = dstore_config.config.get("source_priority_groups", {})

    db = dstore_config.station_dbase()
    agency = db.loc[station_id, "agency"]
    return priorities[agency] if agency in priorities else None


def resolve_providers_for_repo(key, repo_cfg, provider_priority="infer"):
    """
    Resolve candidate provider values for a repo request.

    Parameters
    ----------
    key : str
        Site identifier, possibly with @subloc suffix.
    repo_cfg : dict
        Configured repo spec.
    provider_priority : "infer", None, str, list[str]
        Explicit provider override or inference request.

    Returns
    -------
    list[str] or None
        Ordered candidate providers, or None to indicate no provider filtering.
    """
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
        site_key = repo_cfg["site_key"]
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
    force_regular=False,
    modifier=None,
    data_path=None,
):
    """
    Read time series data from a configured repository.

    Parameters
    ----------
    station_id : str
        Logical key/station identifier.
    variable : str
        Parameter/variable name.
    subloc : str or None, optional
        Sublocation name. Mutually exclusive with station_id shorthand using '@'.
    repo : str
        Required repo name or explicit repo path.
    src_priority : str, list[str], or None, optional
        Source resolution policy.
    start, end : datetime-like, optional
        Requested time window.
    meta : bool, default=False
        If True, return metadata and data.
    force_regular : bool, default=False
        Passed through to read_ts.
    modifier : str or None, optional
        Optional parameter modifier used by template repos such as processed.
    data_path : str or None, optional
        Optional path to stored files if using repo for config and alternate location for data.

    Returns
    -------
    pandas.DataFrame or tuple
    """
    if repo is None:
        raise ValueError("repo must be provided explicitly to read_ts_repo")

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
    )
    return retval

def detect_dms_unit(fname):
    """
    Detect the unit of measurement from a metadata file.

    Parameters
    ----------
    fname : str
        Path to the metadata file.

    Returns
    -------
    tuple
        A tuple containing the unit as a string and a transformation function (if applicable),
        otherwise None.
    """
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
    """
    Filter metadata from interpret_filename to see if it falls in on a date range.

    Parameters
    ----------
    metafname : dict
        Metadata dictionary containing a "year" key.
    start : int, str, or pandas.Timestamp, optional
        Start year or timestamp for filtering.
    end : int, str, or pandas.Timestamp, optional
        End year or timestamp for filtering.

    Returns
    -------
    bool
        True if the file should be excluded based on the date filter, False otherwise.
    """
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


def ts_multifile(
    pats,
    selector=None,
    column_names=None,
    start=None,
    end=None,
    meta=False,
    force_regular=True,
    repo=None,
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
        tsfiles = glob.glob(fp)
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
    bigts = []  # list of time series from each pattern in pats
    patternfreq = []
    total_series = 0
    for fp, utrans in zip(pats, units):  # loop through patterns
        tsfiles = glob.glob(fp)
        tss = []
        unit, transform = utrans
        commonfreq = None
        for tsfile in tsfiles:  # loop through files in pattern
            # read one by one, not by pattern/wildcard
            metafname = interpret_fname(tsfile, repo=repo)
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
            
            if ts.shape[1] > 1:  # not sure about why we do this here
                if selector is not None:
                    ts = ts[selector].to_frame()
            if column_names is not None:
                if isinstance(column_names, str):
                    column_names = [column_names]
                ts.columns = column_names
            # possibly apply unit transition
            ts = ts if transform is None else transform(ts)
            tss.append(ts)
            tsfreq = ts.index.freq if hasattr(ts.index, "freq") else None
            if commonfreq is None:
                commonfreq = tsfreq
            elif tsfreq < commonfreq:
                print(
                    f"frequency change detected from {commonfreq} to {tsfreq} within pattern"
                )
                commonfreq = tsfreq
                if commonfreq == "D":
                    severe = True
                    print("Severe")  # Need to test on CLC
        patternfreq.append(commonfreq)
        # Series within a pattern are assumed compatible, so use merge, which will fill across series
        if len(tss) == 0:
            print(f"No series for subpattern: {fp}")
        else:
            if force_regular:
                tss = [x.asfreq(commonfreq) for x in tss]
            
            patfull = ts_merge(tss)
            total_series = total_series + len(tss)
            if commonfreq is not None:
                patfull = patfull.asfreq(commonfreq)
            bigts.append(patfull)

    # now organize freq across patterns
    cfrq = None  # this will be the common frequency
    for f in patternfreq:
        if cfrq is None:
            cfrq = f
        elif f < cfrq:
            cfrq = f
    fullout = ts_splice(bigts, transition="prefer_first")
    if cfrq is not None:
        fullout = fullout.asfreq(cfrq)
    fullout = fullout.loc[start:end]  # Will have already been filtered to about 1 year
    retval = (metas, fullout) if meta else fullout
    return retval


def ts_multifile_read(
    pats, transforms=None, selector=None, column_name=None, start=None, end=None
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
    if not (isinstance(pats, list)):
        pats = [pats]
    if transforms is None:
        transforms = [None] * len(pats)
    tss = []
    for fp, trans in zip(pats, transforms):
        tsfiles = glob.glob(fp)
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

    commonfreq = None
    for ts in tss:
        tsfreq = ts.index.freq if hasattr(ts.index, "freq") else None
        if tsfreq is not None:
            if commonfreq is None:
                commonfreq = tsfreq
            elif tsfreq < commonfreq:
                print(f"frequency change detected from {commonfreq} to {tsfreq}")
                commonfreq = tsfreq
    full = ts_merge(tss)
    if commonfreq is not None:
        full = full.asfreq(commonfreq)
    return full



