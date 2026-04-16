#!/usr/bin/env python
"""Download robot for water data library
The main function in this file is cdec_download.

For help/usage:
python cdec_download.py --help
"""
import sys  # noqa
import click
import requests
import re
import zipfile
import os
import string
import datetime as dt
import time
import numpy as np
import pandas as pd
import concurrent.futures
from dms_datastore.process_station_variable import (
    stationfile_or_stations,
    normalize_station_request,
    attach_subloc,
    attach_agency_id,
    attach_src_var_id,
)
from dms_datastore import dstore_config
from dms_datastore.logging_config import configure_logging, resolve_loglevel   
import logging
logger = logging.getLogger(__name__)

__all__ = ["cdec_download"]

cdec_base_url = "cdec.water.ca.gov"


def download_station_data(
    row, dest_dir, start, end, endfile, param, overwrite, freq
):
    station = row.station_id
    try:
        cdec_id = row.cdec_id.lower()
    except Exception:
        cdec_id = station

    agency_id = row.agency_id
    p = row.param
    z = row.src_var_id
    subloc = row.subloc
    semantic_key = (station, p)

    yearname = f"{start.year}_{endfile}"

    if subloc == "default":
        path = os.path.join(
            dest_dir, f"cdec_{station}_{agency_id}_{p}_{yearname}.csv"
        ).lower()
    else:
        path = os.path.join(
            dest_dir, f"cdec_{station}@{subloc}_{agency_id}_{p}_{yearname}.csv"
        ).lower()

    result = {
        "station": station,
        "paramname": p,
        "param_code": z,
        "semantic_key": semantic_key,
        "path": path,
        "found": False,
        "skipped": False,
        "reason": None,
        "durations_tried": ["E", "H", "D", "M"] if freq is None else [freq],
        "sensor_codes_tried": [z],
    }

    if os.path.exists(path) and overwrite is False:
        logger.info("Skipping existing station because file exists: %s", path)
        result["skipped"] = True
        result["reason"] = "exists"
        return result

    stime = start.strftime("%m-%d-%Y")
    etime = end if end == "Now" else end.strftime("%m-%d-%Y")

    logger.debug(f"Downloading station {station} parameter {p} sensor code {z}")

    found = False
    for code in [z]:

        if freq is None:
            dur_codes = ["E"]   # new default
        else:
            dur_codes = [f.strip().upper() for f in freq.split(",")]

        for dur in dur_codes:
            station_query = (
                f"http://{cdec_base_url}/dynamicapp/req/CSVDataServletPST"
                f"?Stations={cdec_id}&SensorNums={code}&dur_code={dur}"
                f"&Start={stime}&End={etime}"
            )

            maxattempt = 5
            station_html = ""
            for iattempt in range(maxattempt):
                try:
                    response = requests.get(station_query)
                    station_html = response.text.replace("\r", "")
                    break
                except Exception:
                    time.sleep(1)
                    station_html = ""

            if (station_html.startswith("Title") and len(station_html) > 16) or (
                station_html.startswith("STATION_ID") and len(station_html) > 90
            ):
                with open(path, "w") as f:
                    f.write(station_html)
                logger.debug("Found, duration code: %s", dur)
                found = True
                result["found"] = True
                result["reason"] = "success"
                result["duration_found"] = dur
                break

        if found:
            break

    if not found:
        result["reason"] = "no_data"

    return result

def cdec_download(
    stations, dest_dir, start, end=None, param=None, overwrite=False, freq=None, max_workers=6
):
    """Download robot for CDEC."""
    if end is None:
        end = dt.datetime.now()
        endfile = 9999
    else:
        endfile = end.year

    if not os.path.exists(dest_dir):
        os.mkdir(dest_dir)

    bottom_codes = {"92", "102"}

    stations = stations.copy()
    stations["src_var_id"] = stations["src_var_id"].astype(str).str.strip()

    subloc_inconsist = (
        stations.subloc.isin(["default", "nan", "upper", "top"])
        & stations.src_var_id.isin(bottom_codes)
    )
    stations = stations.loc[~subloc_inconsist, :]

    subloc_inconsist = (
        stations.subloc.isin(["lower", "bot", "bottom"])
        & ~stations.src_var_id.isin(bottom_codes)
    )
    stations = stations.loc[~subloc_inconsist, :]

    results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for _, row in stations.iterrows():
            futures.append(
                executor.submit(
                    download_station_data,
                    row,
                    dest_dir,
                    start,
                    end,
                    endfile,
                    param,
                    overwrite,
                    freq,
                )
            )

        for future in concurrent.futures.as_completed(futures):
            try:
                results.append(future.result())
            except Exception as e:
                logger.error(f"Exception occurred during download: {e}")

    grouped = {}
    for result in results:
        if result is None:
            continue
        key = result["semantic_key"]
        grouped.setdefault(key, []).append(result)

    final_failures = []
    skips = []

    for key, group in grouped.items():
        if any(item.get("found") for item in group):
            success_codes = [str(item["param_code"]) for item in group if item.get("found")]
            logger.debug(f"Semantic success for {key} via code(s): {', '.join(success_codes)}")
            continue

        if all(item.get("skipped") for item in group):
            skips.extend([item["path"] for item in group if item.get("path")])
            continue

        final_failures.append(key)

        durs = sorted({dur for item in group for dur in item.get("durations_tried", [])})
        codes = sorted({str(item["param_code"]) for item in group})
        logger.info(f"No data found for station={key[0]} param={key[1]} durations={durs}, sensor codes={codes}")

    if len(final_failures) == 0:
        logger.debug("No failed station variable combinations")
    else:
        logger.info("Failed query stations:")
        for failure in final_failures:
            logger.info(failure)

    return results

def download_cdec(
    dest_dir,
    id_col,
    param_col,
    start,
    end,
    param,
    stations,
    stationfile,
    overwrite,
    freq,
):
    """Download robot for CDEC water data."""
    cdec_column = id_col
    param_column = param_col
    destdir = dest_dir
    stime = dt.datetime(*list(map(int, re.split(r"[^\d]", start))))
    if end is not None:
        etime = dt.datetime(*list(map(int, re.split(r"[^\d]", end))))
    else:
        etime = pd.Timestamp.now()
    if param_column is not None and param is not None:
        raise ValueError("param_col and param cannot both be specified")
    if param_column is None and param is None:
        param_column = "param"

    request = stationfile_or_stations(stationfile, stations)

    if isinstance(request, str):
        req_df = pd.read_csv(request, sep=",", comment="#", header=0)
        df = normalize_station_request(
            stationframe=req_df,
            param=param,
            default_subloc=None,
        )
    else:
        df = normalize_station_request(
            stationlist=request,
            param=param,
            default_subloc=None,
        )

    df = attach_subloc(df, default_subloc="default")
    df = attach_agency_id(df, repo_name="formatted", agency_id_col="agency_id")
    vlookup = dstore_config.config_file("variable_mappings")
    df = attach_src_var_id(df, vlookup, source="cdec")

    # stations,variables = process_station_list(stationfile,cdec_column,param_column)
    # if not variables: variables = [param]*len(stations)
    cdec_download(df, destdir, stime, etime, param, overwrite, freq)


@click.command()
@click.option(
    "--dest",
    "dest_dir",
    default="cdec_download",
    help="Destination directory for downloaded files.",
)
@click.option(
    "--id_col",
    default="id",
    type=str,
    help="Column in station file representing CDEC ID. IDs with > 3 characters will be ignored.",
)
@click.option(
    "--param_col",
    type=str,
    default=None,
    help="Column in station file representing the parameter to download.",
)
@click.option(
    "--start",
    required=True,
    help="Start time, format 2009-03-31 14:00",
)
@click.option(
    "--end",
    default=None,
    help="End time, format 2009-03-31 14:00",
)
@click.option(
    "--param",
    default=None,
    help="Variable to download",
)
@click.option(
    "--stations",
    multiple=True,
    default=None,
    help="Id or name of one or more stations.",
)
@click.option(
    "--overwrite",
    is_flag=True,
    default=False,
    help="Overwrite existing files (if False they will be skipped, presumably for speed)",
)
@click.option(
    "--freq",
    default="E",
    help="Frequency code(s): E (event), H (hourly), D (daily), M (monthly). "
     "Default is E. Multiple values allowed as comma-separated list (e.g., E,H,D)."
)
@click.argument("stationfile", nargs=-1)
def download_cdec_cli(
    dest_dir,
    id_col,
    param_col,
    start,
    end,
    param,
    stations,
    stationfile,
    overwrite,
    freq,
):
    """CLI for downloading CDEC water data."""

    download_cdec(
        dest_dir,
        id_col,
        param_col,
        start,
        end,
        param,
        stations,
        stationfile,
        overwrite,
        freq,
    )


if __name__ == "__main__":
    download_cdec_cli()
