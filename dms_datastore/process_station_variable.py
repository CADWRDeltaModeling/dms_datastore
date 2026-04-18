#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
process_station_variable
========================

Utilities for normalizing and enriching station/variable requests prior to
data retrieval.

This module decomposes what was historically a single "process station-variable"
step into explicit, testable stages:

1. normalize_station_request
   Parse user inputs (CLI strings, CSV rows) into a canonical dataframe with:
   - station_id
   - subloc (optional but explicit when required)
   - param

2. attach_agency_id
   Resolve each station_id to an agency_id using the repository registry
   (typically the "continuous" repo inventory).

3. attach_src_var_id
   Map (agency_id, param) → src_var_id using a configurable mapping.
   This mapping is intentionally *non-unique* and may collapse multiple
   source-specific variables (e.g., "StreamFlow", "ReservoirDischarge")
   into a single logical parameter ("flow").

Design principles
-----------------
- Fail fast on malformed or ambiguous input.
- Avoid implicit defaults when they can hide real distinctions.
- Treat sublocation ("subloc") as first-class where applicable.
- Keep lookup logic simple and explicit rather than "magical".

Notes on sublocation handling
----------------------------
Sublocation is suppressed only when it is truly non-applicable or
unambiguous. If a station supports multiple sublocations, leaving it
unspecified is considered an error or an incomplete request.

This avoids silently collapsing distinct data streams into a single,
ambiguous output.
"""
import os
from collections.abc import Mapping

import pandas as pd
from dms_datastore import dstore_config
import logging
logger = logging.getLogger(__name__)


_MAPPING_CACHE = {}


_UNSPECIFIED_SUBLOC = {"", "nan", "none", "null"}


def stationfile_or_stations(stationfile, stations):
    """
    Resolve mutually exclusive station input forms.

    Parameters
    ----------
    stationfile : sequence of str or None
        Click-style positional stationfile input. At most one file is allowed.
    stations : sequence of str or None
        Explicit station arguments, typically from a CLI option.

    Returns
    -------
    str or sequence of str
        The selected station input. A file path is returned unchanged if a
        station file is used; otherwise the explicit station list is returned.

    Raises
    ------
    ValueError
        If neither input is provided, if both are provided, if more than one
        station file is given, or if the supplied station file does not exist.

    Notes
    -----
    This is a small input-selection helper. It does not parse station syntax
    or perform any lookup work.
    """

    if not (stations or stationfile):
        raise ValueError("Either station or stationfile required")
    if stations and stationfile:
        raise ValueError("Station and stationfile inputs are mutually exclusive")
    if stationfile:
        if len(stationfile) > 1:
            raise ValueError("Only one stationfile may be input")
        stationfile = stationfile[0]
        if not os.path.exists(stationfile):
            raise ValueError(f"File does not exist: {stationfile}")
        return stationfile
    return stations


def _split_station_and_subloc(value):
    if pd.isna(value):
        return "", None
    text = str(value).replace("'", "").strip().lower()
    if "@" in text:
        station_id, subloc = text.split("@", 1)
        subloc = subloc.strip().lower()
        if subloc in _UNSPECIFIED_SUBLOC:
            subloc = None
        return station_id.strip(), subloc
    return text, None



def _normalize_subloc_value(value):
    if pd.isna(value):
        return None
    text = str(value).strip().lower()
    if text in _UNSPECIFIED_SUBLOC:
        return None
    return text



def normalize_station_request(
    station=None,
    stationlist=None,
    stationframe=None,
    param=None,
    default_subloc=None,
):
    """
    Normalize accepted request inputs to a canonical dataframe.

    Parameters
    ----------
    station : str, optional
        Single station specification. A station may include an inline
        sublocation suffix such as ``"mrz@upper"``.
    stationlist : str or sequence, optional
        Either a CSV path or a list-like collection of station specifications.
    stationframe : pandas.DataFrame, optional
        Pre-built request dataframe. Must contain ``station_id`` or a column
        that can be renamed to it.
    param : str, optional
        Parameter to broadcast onto rows that do not already provide ``param``.
    default_subloc : str or None, optional
        Default sublocation value to use for rows whose sublocation remains
        unspecified after parsing.

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns ``station_id``, ``param``, and ``subloc``.

    Raises
    ------
    ValueError
        If not exactly one of ``station``, ``stationlist``, or ``stationframe``
        is provided, or if the normalized request lacks required fields.

    Notes
    -----
    This function only parses and standardizes request structure. It does not
    consult registries or variable mappings.
    """

    provided = sum(x is not None for x in (station, stationlist, stationframe))
    if provided != 1:
        raise ValueError(
            "Exactly one of station, stationlist, or stationframe must be provided"
        )

    if station is not None:
        df = pd.DataFrame({"station_id": [station]})
    elif stationlist is not None:
        if isinstance(stationlist, str):
            df = pd.read_csv(stationlist, sep=",", comment="#", header=0)
        elif isinstance(stationlist, (list, tuple, pd.Index, pd.Series)):
            df = pd.DataFrame({"station_id": list(stationlist)})
        else:
            raise ValueError("stationlist must be a path, list-like, or tuple")
    else:
        if not isinstance(stationframe, pd.DataFrame):
            raise ValueError("stationframe must be a pandas DataFrame")
        df = stationframe.copy()

    # todo: rewrite in terms of site_key rather than "station_id"    
    if "station_id" not in df.columns:
        raise ValueError("Request must contain a station_id column")

    if param is not None:
        if "param" in df.columns:
            missing = df["param"].isna() | (df["param"].astype(str).str.strip() == "")
            df.loc[missing, "param"] = param
        else:
            df["param"] = param

    if "param" not in df.columns:
        raise ValueError("Request must contain a param column or param argument")
    

    parsed = df["station_id"].apply(_split_station_and_subloc)
    parsed_df = pd.DataFrame(parsed.tolist(), columns=["station_id", "_parsed_subloc"], index=df.index)
    df["station_id"] = parsed_df["station_id"]

    if "subloc" in df.columns:
        df["subloc"] = df["subloc"].apply(_normalize_subloc_value)
        missing_subloc = df["subloc"].isna()
        df.loc[missing_subloc, "subloc"] = parsed_df.loc[missing_subloc, "_parsed_subloc"]
    else:
        df["subloc"] = parsed_df["_parsed_subloc"]

    if default_subloc is not None:
        df["subloc"] = df["subloc"].fillna(default_subloc)

    if param is not None:
        if "param" in df.columns:
            missing = df["param"].isna() | (df["param"].astype(str).str.strip() == "")
            df.loc[missing, "param"] = param
        else:
            df["param"] = param

    df["param"] = df["param"].astype(str).str.strip()
    if "subloc" in df.columns:
        df["subloc"] = df["subloc"].apply(_normalize_subloc_value)

    return df[["station_id", "param", "subloc"]]



def _load_mapping(mapping):
    if isinstance(mapping, pd.DataFrame):
        return mapping.copy()

    if isinstance(mapping, Mapping):
        rows = []
        for param, src in mapping.items():
            if isinstance(src, (list, tuple, set)):
                for one in src:
                    rows.append({"param": param, "src_var_id": one})
            else:
                rows.append({"param": param, "src_var_id": src})
        return pd.DataFrame(rows)

    if isinstance(mapping, str):
        cache_key = os.path.abspath(mapping)
        if cache_key not in _MAPPING_CACHE:
            _MAPPING_CACHE[cache_key] = pd.read_csv(
                mapping,
                sep=",",
                comment="#",
                header=0,
                dtype=str,
            )
        return _MAPPING_CACHE[cache_key].copy()

    raise ValueError("mapping must be a DataFrame, mapping object, or CSV path")

def attach_agency_id(
    df,
    repo_name="formatted",
    agency_id_col="agency_id",
    on_missing="raise",
):
    """
    Attach source-facing station identifiers from a registry.

    Parameters
    ----------
    df : pandas.DataFrame
        Request dataframe containing ``station_id``.
    repo_name : str, default "formatted"
        Repository name used to resolve the configured registry and site key
        when ``registry_df`` is not supplied.
    agency_id_col : str, default "agency_id"
        Name of the registry column that should be copied into the canonical
        output column ``agency_id``.
    on_missing : {"raise", "drop", "keep_na"}, default "raise"
        Policy for rows whose ``agency_id_col`` cannot be resolved.

        - ``"raise"``:
          Raise ``ValueError`` if any requested stations are unresolved.
        - ``"drop"``:
          Drop unresolved rows and return only resolved rows.
        - ``"keep_na"``:
          Keep unresolved rows and leave canonical ``agency_id`` missing.

    Returns
    -------
    pandas.DataFrame
        Copy of the request dataframe with registry columns merged in and with
        canonical column ``agency_id`` attached.

    Raises
    ------
    ValueError
        If the configured site key is missing, if ``agency_id_col`` is not
        present in the registry, or if ``on_missing="raise"`` and one or more
        requested stations cannot be resolved.
    """
    valid_policies = {"raise", "drop", "keep_na"}
    if on_missing not in valid_policies:
        raise ValueError(
            f"Invalid on_missing policy {on_missing!r}. "
            f"Expected one of {sorted(valid_policies)}"
        )

    repo_cfg = dstore_config.repo_config(repo_name)
    registry_name = repo_cfg["registry"]
    site_key = repo_cfg["site_key"]
    registry = dstore_config.registry_df(registry_name).copy()
    if site_key not in registry.columns:
            raise ValueError(f"Registry site key column {site_key!r} not found")

    if registry.index.name == site_key:
        registry = registry.reset_index(drop=(site_key in registry.columns))


    if agency_id_col not in registry.columns:
        raise ValueError(
            f"Requested agency id column {agency_id_col!r} not found in registry"
        )


    lookup = registry[[site_key, agency_id_col]].copy()
    lookup = lookup.rename(columns={site_key: "station_id"})

    merged = df.merge(lookup, on="station_id", how="left")


    missing = merged[agency_id_col].isna() | (
            merged[agency_id_col].astype(str).str.strip() == ""
        )

    if missing.any():
        missing_rows = merged.loc[missing, ["station_id"]].copy()
        if "param" in merged.columns:
            missing_rows["param"] = merged.loc[missing, "param"]
        missing_ids = sorted(missing_rows["station_id"].dropna().unique().tolist())

        if on_missing == "raise":
            raise ValueError(
                f"Unable to resolve {agency_id_col!r} for stations: {missing_ids}"
            )

        if on_missing == "drop":
            for _, row in missing_rows.drop_duplicates().iterrows():
                if "param" in missing_rows.columns:
                    logger.warning(
                        "Ignoring station %s param %s due to lack of %s in registry",
                        row["station_id"],
                        row["param"],
                        agency_id_col,
                    )
                else:
                    logger.warning(
                        "Ignoring station %s due to lack of %s in registry",
                        row["station_id"],
                        agency_id_col,
                    )
            merged = merged.loc[~missing].copy()

        elif on_missing == "keep_na":
            for _, row in missing_rows.drop_duplicates().iterrows():
                if "param" in missing_rows.columns:
                    logger.warning(
                        "Keeping station %s param %s with unresolved %s",
                        row["station_id"],
                        row["param"],
                        agency_id_col,
                    )
                else:
                    logger.warning(
                        "Keeping station %s with unresolved %s",
                        row["station_id"],
                        agency_id_col,
                    )

    if agency_id_col == "agency_id":
        # Canonical column is already the requested source-facing identifier.
        return merged

    merged["agency_id"] = pd.NA
    present = merged[agency_id_col].notna()
    merged.loc[present, "agency_id"] = (
        merged.loc[present, agency_id_col].astype(str).str.replace("'", "", regex=True)
    )

    return merged

def attach_subloc(df, subloc_lookup=None, default_subloc="default"):
    """
    Attach or expand sublocation values.

    Parameters
    ----------
    df : pandas.DataFrame
        Request dataframe containing ``station_id`` and optionally ``subloc``.
    subloc_lookup : pandas.DataFrame or str or None, optional
        Sublication lookup table or CSV path. If omitted, the configured
        sublocation table is read via ``dstore_config``.
    default_subloc : str, default "default"
        Sublication label assigned when a station has no listed sublocations
        and the incoming request leaves sublocation unspecified.

    Returns
    -------
    pandas.DataFrame
        Request dataframe with explicit ``subloc`` values. Rows with
        unspecified sublocation are expanded when the lookup lists multiple
        sublocations for a station.

    Raises
    ------
    ValueError
        If ``station_id`` is absent from the input dataframe.

    Notes
    -----
    This function preserves explicit sublocation requests and only expands
    rows whose sublocation is unspecified.
    """

    if "station_id" not in df.columns:
        raise ValueError("df must contain station_id before attach_subloc")
    if "subloc" not in df.columns:
        df = df.copy()
        df["subloc"] = None

    if subloc_lookup is None:
        subloc_df = dstore_config.sublocation_df().copy()
    elif isinstance(subloc_lookup, pd.DataFrame):
        subloc_df = subloc_lookup.copy()
    else:
        subloc_df = pd.read_csv(
            subloc_lookup,
            sep=",",
            comment="#",
            header=0,
            dtype={"station_id": str, "subloc": str},
        )

    subloc_df = subloc_df.copy()
    subloc_df["station_id"] = subloc_df["station_id"].astype(str).str.replace("'", "", regex=True).str.strip().str.lower()
    subloc_df["subloc"] = subloc_df["subloc"].apply(_normalize_subloc_value)
    subloc_df = subloc_df.loc[subloc_df["subloc"].notna(), ["station_id", "subloc"]].drop_duplicates()

    subloc_map = (
        subloc_df.groupby("station_id")["subloc"].apply(list).to_dict()
        if not subloc_df.empty else {}
    )

    rows = []
    for _, row in df.iterrows():
        rec = row.to_dict()
        station_id = rec["station_id"]
        requested_subloc = _normalize_subloc_value(rec.get("subloc"))
        if requested_subloc is None:
            known_sublocs = subloc_map.get(station_id, [])
            if known_sublocs:
                for subloc in known_sublocs:
                    out = rec.copy()
                    out["subloc"] = subloc
                    rows.append(out)
            else:
                out = rec.copy()
                out["subloc"] = default_subloc
                rows.append(out)
        else:
            rec["subloc"] = requested_subloc
            rows.append(rec)

    return pd.DataFrame(rows).drop_duplicates().reset_index(drop=True)



def attach_src_var_id(df, mapping, source=None):
    """
    Attach source-facing variable identifiers from a parameter mapping.

    Parameters
    ----------
    df : pandas.DataFrame
        Request dataframe containing ``param``.
    mapping : collections.abc.Mapping or pandas.DataFrame or str
        Mapping source. Accepted forms are:
        - mapping-like object from project parameter name to source variable id
        - dataframe containing ``param`` and ``src_var_id`` or ``var_name``
        - CSV path to such a table
    source : str, optional
        Source selector used to filter mapping rows when the mapping contains
        a ``src_name`` column.

    Returns
    -------
    pandas.DataFrame
        Request dataframe with ``src_var_id`` attached.

    Raises
    ------
    ValueError
        If the mapping lacks the columns required to resolve ``src_var_id``.

    Notes
    -----
    Mapping is intentionally allowed to be non-unique. A single request row
    may expand to multiple rows when multiple source variables correspond to
    the same project parameter.
    """

    if "param" not in df.columns:
        raise ValueError("df must contain param before attach_src_var_id")

    map_df = _load_mapping(mapping)
    map_df.columns = [str(c).strip() for c in map_df.columns]

    rename_map = {}
    if "var_name" in map_df.columns and "param" not in map_df.columns:
        rename_map["var_name"] = "param"
    if rename_map:
        map_df = map_df.rename(columns=rename_map)

    if source is not None and "src_name" in map_df.columns:
        map_df = map_df.loc[map_df["src_name"] == source, :].copy()

    if "param" not in map_df.columns:
        raise ValueError("mapping must contain param or var_name column")

    if "src_var_id" not in map_df.columns:
        if set(map_df.columns) == {"param"}:
            map_df["src_var_id"] = map_df["param"]
        else:
            raise ValueError("mapping must contain src_var_id column")

    map_df["param"] = map_df["param"].astype(str).str.strip()
    map_df["src_var_id"] = map_df["src_var_id"].astype(str).str.strip()

    keep_cols = [
        c
        for c in map_df.columns
        if c in ["param", "src_var_id", "src_var_name", "src_name", "comment"]
    ]
    merged = df.merge(map_df[keep_cols].drop_duplicates(), on="param", how="left")
    merged["src_var_id"] = merged["src_var_id"].fillna(merged["param"])
    return merged



def process_station_list(
    stationlist,
    id_col="station_id",
    agency_id_col="agency_id",
    param_col=None,
    param=None,
    subloc_col=None,
    subloc_lookup=None,
    subloc="default",
    station_lookup=None,
    param_lookup=None,
    source="cdec",
):
    """Legacy compatibility wrapper for processing station/parameter inputs into fully resolved requests. Don't use.

    This is the high-level orchestration function that applies:
    1. normalization
    2. agency_id attachment
    3. src_var_id attachment

    Parameters
    ----------
    stations : str or list or pandas.DataFrame
        Station specification (see normalize_station_request).

    params : str or list of str, optional
        Parameter(s) to associate with stations.

    registry_df : pandas.DataFrame
        Station registry used to resolve agency_id.

    src_var_map : dict or pandas.DataFrame or str
        Mapping used to resolve src_var_id.

    Returns
    -------
    pandas.DataFrame
        Fully resolved dataframe with:
        - station_id
        - subloc
        - param
        - agency_id
        - src_var_id

    Raises
    ------
    ValueError
        If any stage fails due to ambiguity or missing data.

    Notes
    -----
    - This function enforces strict, fail-fast behavior.
    - Sublocation completeness is expected before downstream use;
      ambiguous or missing sublocations should be resolved prior
      to calling data retrieval routines.
    """
    if param_col is not None and param is not None:
        raise ValueError("Cannot use both param_col and param arguments")

    if isinstance(stationlist, str):
        station_df = pd.read_csv(stationlist, sep=",", comment="#", header=0)
    elif isinstance(stationlist, (list, tuple, pd.Index, pd.Series)):
        station_df = pd.DataFrame({id_col: list(stationlist)})
    else:
        station_df = stationlist.copy()

    if "station_id" not in df.columns:
        raise ValueError("Request must contain a station_id column")

    if param is not None:
        if "param" in df.columns:
            missing = df["param"].isna() | (df["param"].astype(str).str.strip() == "")
            df.loc[missing, "param"] = param
        else:
            df["param"] = param

    if "param" not in df.columns:
        raise ValueError("Request must contain a param column or param argument")

    if subloc_col is not None and subloc_col in station_df.columns and "subloc" not in station_df.columns:
        station_df = station_df.rename(columns={subloc_col: "subloc"})

    if id_col in station_df.columns and "station_id" not in station_df.columns:
        station_df = station_df.rename(columns={id_col: "station_id"})

    station_df = normalize_station_request(
        stationframe=station_df,
        param=param,
        default_subloc=None,
    )
    station_df = attach_subloc(station_df, subloc_lookup=subloc_lookup, default_subloc=subloc)

    if station_lookup is not None:
        registry = pd.read_csv(station_lookup, sep=",", comment="#", header=0, dtype=str)
        registry.columns = [str(c).strip() for c in registry.columns]
        if id_col in registry.columns and "station_id" not in registry.columns:
            registry = registry.rename(columns={id_col: "station_id"})
        station_df = attach_agency_id(
            station_df,
            agency_id_col=agency_id_col,
            registry_df=registry,
        )
    else:
        station_df["agency_id"] = station_df["station_id"]

    if param_lookup is not None:
        station_df = attach_src_var_id(station_df, param_lookup, source=source)
    elif "src_var_id" not in station_df.columns:
        station_df["src_var_id"] = station_df["param"]

    return station_df



def read_station_subloc(fpath):
    """
    Read a station-sublocation table.

    Parameters
    ----------
    fpath : str or path-like
        Path to a CSV file with columns including ``station_id``, ``subloc``, and ``z``.

    Returns
    -------
    pandas.DataFrame
        Dataframe indexed by ``station_id`` and ``subloc`` with column ``z``.

    Notes
    -----
    The returned structure is suitable for merging with a station database via
    ``merge_station_subloc``.
    """

    df = pd.read_csv(fpath, sep=",", header=0, index_col=["station_id", "subloc"], comment="#")
    df["z"] = df.z
    return df[["z"]]



def merge_station_subloc(station_dbase, station_subloc, default_z):
    """
    Merge a station database with a station-sublocation table.

    Parameters
    ----------
    station_dbase : pandas.DataFrame
        Station database keyed by station identifier.
    station_subloc : pandas.DataFrame
        Sublication table indexed by ``station_id`` and ``subloc``.
    default_z : float
        Default elevation assigned to stations with no explicit sublocation
        entry.

    Returns
    -------
    pandas.DataFrame
        Combined dataframe indexed by ``station_id`` and ``subloc``.

    Notes
    -----
    Stations without explicit sublocation entries receive a synthesized
    ``default`` sublocation row.
    """


    base = station_dbase.reset_index().copy()
    sub = station_subloc.reset_index().copy()

    if "station_id" not in base.columns:
        raise ValueError("station_dbase must contain 'station_id'")

    if "station_id" not in sub.columns:
        raise ValueError("station_subloc must contain 'station_id'")

    merged = base.merge(sub, on="station_id", how="left")
    merged.fillna({"subloc": "default", "z": default_z}, inplace=True)
    merged.set_index(["station_id", "subloc"], inplace=True)
    return merged
