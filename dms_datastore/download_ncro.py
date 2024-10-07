#!/usr/bin/env python
import argparse
import ssl
import urllib.request
import requests
import pandas as pd
import re
import zipfile
import os
import io
import string
import datetime as dt
import numpy as np
import concurrent
import concurrent.futures
import time
from dms_datastore.process_station_variable import (
    process_station_list,
    stationfile_or_stations,
)
from dms_datastore import dstore_config
from .logging_config import logger

__all__ = ["download_ncro_por"]

ncro_inventory_file = "ncro_por_inventory.txt"


def station_dbase():
    dbase_fname = dstore_config.config_file("station_dbase")
    dbase_df = pd.read_csv(dbase_fname, header=0, comment="#", index_col="id")
    is_ncro = dbase_df.agency.str.lower().str.contains("ncro")
    logger.info(is_ncro[is_ncro.isnull()])
    return dbase_df.loc[is_ncro, :]


def download_ncro_inventory(dest, cache=True):
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ctx.options |= 0x4
    url = "https://data.cnra.ca.gov/dataset/fcba3a88-a359-4a71-a58c-6b0ff8fdc53f/resource/cdb5dd35-c344-4969-8ab2-d0e2d6c00821/download/station-trace-download-links.csv"

    max_attempt = 10
    for attempt in range(1, (max_attempt + 1)):
        logger.info(f"Downloading inventory for NCRO attempt #{attempt}")
        try:
            response = urllib.request.urlopen(url, context=ctx).read()
            fio = io.BytesIO(response)
            idf = pd.read_csv(
                fio,
                header=0,
                parse_dates=["start_time", "end_time"],
            )

            idf = idf.loc[
                (idf.station_type != "Groundwater") & (idf.output_interval == "RAW"), :
            ]
            logger.info(idf)

            idf.to_csv(
                os.path.join(dest, ncro_inventory_file),
                sep=",",
                index=False,
                date_format="%Y-%d%-mT%H:%M",
            )
            return idf
        except:
            if attempt == max_attempt:
                raise Exception("Could not open inventory.")
            continue


def ncro_variable_map():
    varmap = pd.read_csv("variable_mappings.csv", header=0, comment="#")
    return varmap.loc[varmap.src_name == "wdl", :]


# station_number,station_type,start_time,end_time,parameter,output_interval,download_link

mappings = {
    "Water Temperature": "temp",
    "Stage": "elev",
    "Conductivity": "ec",
    "Electrical Conductivity at 25C": "ec",
    "Fluorescent Dissolved Organic Matter": "fdom",
    "Water Temperature ADCP": "temp",
    "Dissolved Oxygen": "do",
    "Chlorophyll": "cla",
    "Dissolved Oxygen (%)": None,
    "Dissolved Oxygen Percentage": None,
    "Velocity": "velocity",
    "pH": "ph",
    "Turbidity": "turbidity",
    "Flow": "flow",
    "Salinity": "salinity",
    "ECat25C": "ec",
    "StreamFlow": "flow",
    "WaterTemp": "temp",
    "WaterTempADCP": "temp",
    "DissolvedOxygen": "do",
    "DissolvedOxygenPercentage": None,
    "StreamLevel": "elev",
    "WaterSurfaceElevationNAVD88": "elev",
    "fDOM": "fdom",
}


def download_station_period_record(row, dbase, dest, variables, failures, ctx):
    """Downloads station/param combo period of record"""
    agency_id = row.station_number
    param = row.parameter
    if param in mappings.keys():
        var = mappings[param]
        if var is None:
            return
        if var not in variables:
            return
    else:
        logger.info(f"Problem on row: {row}")
        if type(param) == float:
            if np.isnan(param):
                return  # todo: this is a fix for an NCRO-end bug. Really the ValueError is best
        raise ValueError(f"No standard mapping for NCRO parameter {param}.")

    # printed.add(param)
    var = mappings[param]
    link_url = row.download_link
    sdate = row.start_time
    edate = row.end_time
    entry = None
    ndx = ""
    for suffix in ["", "00", "Q"]:
        full_id = dbase.agency_id + suffix
        entry = dbase.index[full_id == agency_id]
        if len(entry) > 1:
            raise ValueError(f"multiple entries for agency id {agency_id} in database")
        elif not entry.empty:
            station_id = str(entry[0])

    if station_id == "":
        raise ValueError(
            f"Item {agency_id} not found in station database after accounting for Q and 00 suffixes"
        )

    fname = f"ncro_{station_id}_{agency_id}_{var}_{sdate.year}_{edate.year}.csv".lower()
    fpath = os.path.join(dest, fname)
    logger.info(f"Processing: {agency_id} {param} {sdate} {edate}")
    logger.info(link_url)

    attempt = 0
    max_attempt = 20
    station_html = ""
    while attempt < max_attempt:
        attempt = attempt + 1
        try:
            if attempt > 16:
                logger.info(f"{station_id} attempt {attempt}")
                if attempt > 16:
                    logger.info(fname)

            ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
            ctx.options |= 0x4
            response = urllib.request.urlopen(link_url, context=ctx)
            # response = urllib.request.get(url).content
            station_html = response.read().decode().replace("\r", "")

            break
        except Exception as e:
            if attempt == max_attempt:
                logger.warning(
                    f"Failure in URL request or reading the response after {attempt} tries for station {station_id} param {param}. Link=\n{link_url}\nException below:"
                )
                logger.exception(e)
                failures.append((station_id, agency_id, var, param))
                attampt = 0
                return
            else:
                time.sleep(
                    attempt
                )  # Wait one second more second each time to clear any short term bad stuff
    if len(station_html) > 30 and not "No sites found matching" in station_html:
        found = True
        if attempt > 1:
            logger.info(f"{station_id} found on attempt {attempt}")
        with open(fpath, "w") as f:
            f.write(station_html)
    else:
        logger.info(f"{station_id} not found after attempt {attempt}")
        logger.info("Station %s produced no data" % station)
        failures.append((station_id, agency_id, var, param))
    return


def download_ncro_period_record(inventory, dbase, dest, variables=None):

    if variables is None:
        variables = ["flow", "elev", "ec", "temp", "do", "ph", "turbidity", "cla"]
    global mappings
    # mappings = ncro_variable_map()
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ctx.options |= 0x4
    failures = []
    # Use ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        # Schedule the download tasks and handle them asynchronously
        futures = []
        for ndx, row in inventory.iterrows():
            future = executor.submit(
                download_station_period_record,
                row,
                dbase,
                dest,
                variables,
                failures,
                ctx,
            )
            futures.append(future)
        # Optionally, handle the results of the tasks
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()  # This line can be used to handle results or exceptions from the tasks
            except Exception as e:
                logger.error(f"Exception occurred during download: {e}")

    logger.debug("Failures in download_ncro")
    for f in failures:
        logger.debug(f)


def download_ncro_por(dest, variables=None):
    idf = download_ncro_inventory(dest)
    dbase = station_dbase()
    upper_station = idf.station_number.str.upper()
    is_in_dbase = (
        upper_station.isin(dbase.agency_id)
        | upper_station.isin(dbase.agency_id + "00")
        | upper_station.isin(dbase.agency_id + "Q")
    )
    if variables is None:
        variables = [
            "flow",
            "velocity",
            "elev",
            "ec",
            "temp",
            "do",
            "ph",
            "turbidity",
            "cla",
        ]
    download_ncro_period_record(idf.loc[is_in_dbase, :], dbase, dest, variables)


def create_arg_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--por",
        dest="por",
        action="store_true",
        help="Do period of record download. Must be explicitly set to true in anticipation of other options",
    )
    parser.add_argument(
        "--dest",
        dest="dest_dir",
        default=".",
        help="Destination directory for downloaded files.",
    )
    parser.add_argument(
        "--param",
        dest="param",
        nargs="+",
        default=None,
        help="Parameters to download.",
    )
    return parser


def main():
    parser = create_arg_parser()
    args = parser.parse_args()
    destdir = args.dest_dir
    por = args.por
    variables = args.param
    dest = "."
    download_ncro_por(destdir, variables)


if __name__ == "__main__":
    main()
