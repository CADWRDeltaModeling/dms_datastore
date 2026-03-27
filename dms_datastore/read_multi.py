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

__all__ = ["read_ts_repo", "read_ts_repo", "ts_multifile_read", "infer_source_priority"]

def resolve_repo_sources(repo_cfg, key, src_priority):
    """
    Resolve which source values should be substituted into repo templates.

    This is search policy, not naming policy.
    """
    search_cfg = repo_cfg.get("search", {})
    use_source_slot = search_cfg.get("use_source_slot", True)

    if not use_source_slot:
        return [""]

    if src_priority == "infer":
        inferred = infer_source_priority_repo(key, repo_cfg)
        return inferred if inferred else ["*"]

    if src_priority is None or src_priority == "*":
        return ["*"]

    if isinstance(src_priority, str):
        return [src_priority]

    return list(src_priority)

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


def infer_source_priority_repo(key, repo_cfg):
    mode = repo_cfg.get("source_priority_mode", "none")

    if mode == "none":
        return None

    if mode == "repo_default":
        group = repo_cfg.get("source_priority_group")
        return dstore_config.source_priority_group(group)

    if mode == "by_registry_column":
        registry = dstore_config.registry_df(
            repo_cfg.get("registry"),
            key_column=repo_cfg.get("key_column", "id"),
        )
        bare_key = key.split("@")[0]
        col = repo_cfg.get("source_priority_column", "agency")
        if bare_key not in registry.index:
            return None
        priority_key = registry.loc[bare_key, col]
        groups = dstore_config.config.get("source_priority_groups", {})
        if priority_key in groups:
            return groups[priority_key]
        # compatibility with old flat source_priority
        old_groups = dstore_config.config.get("source_priority", {})
        return old_groups.get(priority_key)

    raise ValueError(
        f"Unsupported source_priority_mode {mode!r} for repo {repo_cfg.get('name')}"
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
    src_priority="infer",
    start=None,
    end=None,
    meta=False,
    force_regular=False,
    modifier=None,
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
    repository = repo_cfg["root"]

    start = pd.to_datetime(start) if start is not None else None
    end = pd.to_datetime(end) if end is not None else None

    sources = resolve_repo_sources(repo_cfg, station_id, src_priority)

    rel_pats = build_repo_globs(
        repo_cfg,
        key=station_id,
        param=variable,
        subloc=subloc,
        modifier=modifier,
        sources=sources,
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
    for ts in bigts: print(ts.columns)
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



