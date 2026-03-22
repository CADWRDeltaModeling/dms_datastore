#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re


def extract_year_fname(fname):
    re1 = re.compile(r".*_(\d{4})(?:\..{3})")
    yr = int(re1.match(fname).group(1))
    return yr

def interpret_fname(fname, repo=None):
    """Convert filename to metadata dictionary.

    Legacy convention:
        source_station[@subloc]_agency_id_param[_... ]_year.csv
        source_station[@subloc]_agency_id_param[_... ]_syear_eyear.csv

    Processed convention (narrow first patch):
        source_station[@subloc]_param.csv
        source_station[@subloc]_param_year.csv
        source_station[@subloc]_param_syear_eyear.csv

    For processed, param may contain a modifier via '@', e.g. elev@harmonic.
    """
    fname = os.path.split(fname)[1]
    meta = {"filename": fname}

    # -------------------------
    # processed short-form path
    # -------------------------
    if repo == "processed":
        stem, ext = os.path.splitext(fname)
        if not ext:
            raise ValueError(f"Naming convention not matched for {fname}")

        parts = stem.split("_")
        if len(parts) < 3:
            raise ValueError(f"Naming convention not matched for {fname}")

        meta["agency"] = parts[0]

        station_token = parts[1]
        if "@" in station_token:
            station_id, subloc = station_token.split("@", 1)
        else:
            station_id, subloc = station_token, None

        meta["station_id"] = station_id
        meta["subloc"] = subloc

        # processed does not require agency_id in the filename
        meta["agency_id"] = None

        # Allowed forms:
        #   src_station_param
        #   src_station_param_YYYY
        #   src_station_param_YYYY_YYYY
        if len(parts) == 3:
            param_token = parts[2]
        elif len(parts) == 4 and parts[3].isdigit() and len(parts[3]) == 4:
            param_token = parts[2]
            meta["year"] = parts[3]
        elif (
            len(parts) == 5
            and parts[3].isdigit() and len(parts[3]) == 4
            and parts[4].isdigit() and len(parts[4]) == 4
        ):
            param_token = parts[2]
            meta["syear"] = parts[3]
            meta["eyear"] = parts[4]
        else:
            raise ValueError(f"Naming convention not matched for {fname}")

        if "@" in param_token:
            param, modifier = param_token.split("@", 1)
            meta["param"] = param
            meta["modifier"] = modifier
        else:
            meta["param"] = param_token

        return meta

    # -------------------------
    # legacy/default path
    # -------------------------
    datere = re.compile(
        r"([a-z0-9]+)_([a-z0-9@]+)_([a-z0-9]+)_([a-z0-9]+).*_(\d{4})_(\d{4})(?:\..{3})"
    )
    datere1 = re.compile(
        r"([a-z0-9]+)_([a-z0-9@]+)_([a-z0-9]+)_([a-z0-9]+).*_(\d{4})(?:\..{3})"
    )
    m = datere.match(fname)
    if m is None:
        m = datere1.match(fname)
        single_date = True
    else:
        single_date = False

    if m is not None:
        meta["filename"] = m.group(0)
        meta["agency"] = m.group(1)
        station_id = m.group(2)
        if "@" in station_id:
            station_id, subloc = station_id.split("@")
        else:
            subloc = None

        meta["station_id"] = station_id
        meta["subloc"] = subloc
        meta["agency_id"] = m.group(3)
        meta["param"] = m.group(4)
        if single_date:
            meta["year"] = m.group(5)
        else:
            meta["syear"] = m.group(5)
            meta["eyear"] = m.group(6)
        return meta

    raise ValueError(f"Naming convention not matched for {fname}")


def meta_to_filename(meta):
    """Convert dictionary of file/station/data info to a filename.
    This routine is complementary to interpret_fname.

    Parameters
    ----------
    meta : dict
        A dictionary of data that includes: station_id and subloc if pertinent,
        agency (representing source), agency_id of station, param, start and end year.
        These should match conventions: id, station_id and agency should match the station
        database, param should be a standard parameter name listed in variables.csv.

    Returns
    -------
    filename : str
        Filename corresponding to metadata

    """
    if not "station_id" in meta:
        raise ValueError(f"station_id not in meta: {meta}")
    else:
        sid = meta["station_id"]
    if not "subloc" in meta:
        subloc["meta"] = None
    print(meta)
    station_id = sid if meta["subloc"] is None else f"{sid}@{meta['subloc']}"
    if "syear" in meta and "eyear" in meta:
        year_part = f"{meta['syear']}_{meta['eyear']}"
    else:
        year_part = f"{meta['year']}"
    return (
        f"{meta['agency']}_{station_id}_{meta['agency_id']}"
        + f"_{meta['param']}_{year_part}.csv"
    )
