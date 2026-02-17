#!/usr/bin/env python
"""Download robot for Nationla Water Informaton System (NWIS)
The main function in this file is nwis_download.

For help/usage:
python nwis_download.py --help
"""
import click
import sys
import pandas as pd
import traceback
import requests
import re
import zipfile
import os
import string
import datetime as dt
import numpy as np
import concurrent.futures
import shutil
import json
import yaml
from dms_datastore.write_ts import write_ts_csv

from dms_datastore.process_station_variable import (
    process_station_list,
    stationfile_or_stations,
)
from pathlib import Path
from dms_datastore import dstore_config
from dms_datastore.logging_config import configure_logging, resolve_loglevel, LoggingConfig
import logging
logger = logging.getLogger(__name__)


def convert_json_yaml(json_data):
    # Convert JSON to YAML
    print("printing out json converted to yaml")
    yaml_data = yaml.dump(json_data)
    print("dumped")
    # Print YAML data
    print(yaml_data)


def _quarantine_file(fname, quarantine_dir="quarantine"):
    if not os.path.exists(quarantine_dir):
        os.makedirs("quarantine")
    shutil.move(fname, "quarantine")


def parse_usgs_json(parseinput, outfile, report_empty=False):
    """
    Parameters
    parseinput : json data or filename containing USGS json
    outfile : str
    Output file name

    """
    # Read the JSON data from a file
    if os.path.exists(parseinput):
        from_file = True
        with open(parseinput, "r") as file:
            data = json.load(file)
    else:
        from_file = False
        data = json.loads(parseinput)

    # Extract the time series data
    time_series_data = data["value"]["timeSeries"]
    # convert_json_yaml(time_series_data)

    # Initialize an empty list to collect DataFrames for each time series
    dfs = []
    # Initialize a list for mapping methodID (now subloc) to metadata
    subloc_map = []

    # Iterate through each time series
    if len(time_series_data) > 1:
        raise ValueError(
            "parser not ready for multiple timeSeries entries in json, which probably means multiple variables"
        )
    if len(time_series_data) == 0:
        if report_empty:
            reportfname = parseinput if from_file else report_empty
            logger.info(f"No time series: {reportfname}")
            return
        else:
            return

    for series in time_series_data:
        # Extract site name, variable name, and variable code
        source_info = series["sourceInfo"]  # yaml-like
        var_info = series["variable"]
        site_code = source_info["siteCode"][0]["value"]
        var_code_val = series["variable"]["variableCode"][0]["value"]

        default_tz_offset = source_info["timeZoneInfo"]["defaultTimeZone"]["zoneOffset"]
        default_tz_label = source_info["timeZoneInfo"]["defaultTimeZone"][
            "zoneAbbreviation"
        ]
        source_info["timeZoneInfo"][
            "parseNoteDWR"
        ] = f"Timestamps converted to {default_tz_label}. siteUsesDaylightSavingsTime not relevant."

        unique_qual = []  # diagnostic to discover what we might see

        # We will process each 'values' entry (values[0], values[1], etc.)
        for value_index, value_data in enumerate(series["values"]):
            # Extract the methodID (renamed to subloc) and methodDescription from the 'method' field inside 'values'
            if "method" in value_data and len(value_data["method"]) > 0:
                subloc = value_data["method"][0]["methodID"]
                method_description = value_data["method"][0]["methodDescription"]
            else:
                subloc = (
                    f"Unknown_subloc_{value_index}"  # Assign a default or placeholder
                )
                method_description = "Unknown method description"

            for item in value_data["value"]:
                item["qualifiers"] = ",".join(item["qualifiers"])
                item["dateTime"] = item["dateTime"][0:10] + "T" + item["dateTime"][11:]

            # Create a DataFrame from the datetime, value, and qualifiers pairs
            values_df = pd.DataFrame(value_data["value"])
            if values_df.empty:
                logger.debug(f"No data retrieved for subloc/method: {subloc}")
                continue

            # Convert dateTime to pandas datetime format
            values_df["dateTime"] = pd.to_datetime(values_df["dateTime"], utc=True)
            # Create a DataFrame with a UTC datetime column
            values_df["dateTime"] = values_df["dateTime"].dt.tz_convert(
                default_tz_offset.lower()
            )

            # Set dateTime as the index
            values_df.set_index("dateTime", inplace=True)

            unique_qual.append(values_df.qualifiers.unique())

            # Use the subloc (methodID) as the identifier in the MultiIndex
            identifier = f"{subloc}"

            # Rename 'value' and 'qualifiers' columns with the subloc as the identifier
            values_df.columns = pd.MultiIndex.from_product(
                [[identifier], values_df.columns], names=["subloc", "variable"]
            )

            # Flatten the columns with underscores
            values_df.columns = values_df.columns.to_flat_index().map("_".join)

            # Append the DataFrame to the list
            dfs.append(values_df)

            # Append the metadata (methodID, renamed as subloc, mapping)
            subloc_map.append(
                {
                    "subloc": subloc,
                    "site_code": site_code,
                    "method_description": method_description,
                }
            )

    if len(dfs) == 0:
        raise ValueError(f"No data sets found in file for output {outfile}")

    # Concatenate all DataFrames on the dateTime index, using an outer join to align by time
    result_df = pd.concat(dfs, axis=1)
    result_df.index.name = "dateTime"

    # Create a second DataFrame for subloc to metadata mapping
    subloc_df = pd.DataFrame(subloc_map)

    # Convert the DataFrame to a list of dictionaries
    subloc_dict = subloc_df.to_dict(orient="records")

    site_metadata = {
        "format_modifier": "parse-usgs-json",
        "source_info": source_info,
        "variables": var_info,
        "sublocations": subloc_map,
        "time_zone": default_tz_label,
        "time_zone_offset": default_tz_offset,
        "variable_code": var_code_val,
    }

    site_meta_yaml = yaml.dump(site_metadata, default_flow_style=False)

    # Convert the list of dictionaries to YAML format
    # subloc_yaml = yaml.dump(subloc_dict, default_flow_style=False)

    write_ts_csv(result_df, outfile, site_metadata, chunk_years=False)
    return result_df


def download_station(
    row, dest_dir, start, end, param, overwrite, endfile, successes, failures, skips
):

    agency_id = row.agency_id
    station = row.station_id
    param = row.src_var_id
    paramname = row.param
    subloc = row.subloc

    if (station, paramname) in successes:
        return

    yearname = (
        f"{start.year}_{endfile}"  # if start.year != end.year else f"{start.year}"
    )
    outfname = f"usgs_{station}_{agency_id}_{paramname}_{yearname}.csv"
    # Water quality data; does not work in command line.
    if str(paramname).startswith("qual"):
        outfname = f"usgs_{station}_{agency_id}_{paramname}_{param}_{yearname}.csv"
    outfname = outfname.lower()
    path = os.path.join(dest_dir, outfname)
    if os.path.exists(path) and not overwrite:
        logger.info("Skipping existing station because file exists: %s" % station)
        skips.append(path)
        return
    else:
        logger.info(f"Attempting to download station: {station} variable {param}")
    stime = start.strftime("%Y-%m-%d")
    etime = end.strftime("%Y-%m-%d")
    found = False
    station_query_base = f"https://nwis.waterservices.usgs.gov/nwis/iv/?sites={agency_id}&startDT={stime}&endDT={etime}&format=json"
    if param:
        station_query = station_query_base + f"&variable={int(param):05}"
        # station_query = station_query_base % (station,stime,etime,param)
    else:
        station_query = station_query_base
    # Water quality data; does not work in command line.
    if str(paramname).startswith("qual"):
        station_query_base = f"https://waterdata.usgs.gov/nwis/qwdata?site_no={agency_id}&begin_date={stime}&end_date={etime}&format=serial_rdb"
        station_query_base = f"https://nwis.waterdata.usgs.gov/nwis/qwdata?site_no={agency_id}&begin_date={stime}&end_date={etime}&format=json"
        if param:
            station_query = station_query_base + f"&parameter_cd={int(param):05}"
        else:
            station_query = station_query_base
    logger.info(f"USGS Query for ({station},{paramname}): {station_query}")
    max_attempt = 3
    session = requests.Session()
    station_html = ""
    found = False
    for attempt in range(1, (max_attempt + 1)):
        logger.debug(f"attempt: {attempt} variable {int(param):05}, {station}, {agency_id}")
        try:
            response = session.get(
                station_query,
                headers={"User-Agent": "Mozilla/6.0"},
                timeout=75,
            )
            response.raise_for_status()
            station_html = response.text
            logger.debug("Request successful, got text")
            break
        except Exception as e:
            # html part failed
            if attempt == max_attempt:
                failures.append(station)
                found = False
                logger.debug(
                    "Multiple failures in USGS web request for ({station},{paramname},{param})"
                )
            else:
                continue  # Failed communication or download, next attempt
        # If we got here, we got content. Find out whether it is legit data and parse it from json to csv
        # Otherwise record it as not having provided data
        # Assume, simply by virtue of size and the fact that the download code was success, that data are empty
        found = True
    if (
        len(station_html) < 1000
    ):  # Most USGS data fewer than 2000 characters have no good data
        logger.info(
            f"Small file for station {station} param name {paramname} param code {int(param):05}"
        )
        found = False
    else:
        # We know download code was success and file is big enough to test.
        # The length threshold is conservative so could still be empty or bogus
        if "No sites found matching" in station_html or "\"timeSeries\":[]" in station_html:
            found = False
            logger.debug(
                f"Based on typical indicators, attempt yielded no data for vari {int(param):05}.txt"
            )
        else:
            # Try parsing -- final word detecting non-data
            logger.info(f"Parsing USGS JSON: {path} param {param}")
            try:
                df = parse_usgs_json(
                    station_html, path, report_empty=f"{station} {paramname} ({param})"
                )
            except Exception as exc:
                # Parsing broke. The quarantine allows it to be looked at for improvement of the parser.
                # But here we just log it as no data and move on.
                logger.info(
                    f"Parsing of {station} {paramname} ({param}) JSON to csv failed. Writing to quarantine"
                )
                with open(path, "w") as f:
                    f.write(station_html)
                _quarantine_file(path)
                found = False
            if df is not None and not df.empty:
                found = True
                successes.add((station, paramname))
                print(f"Apparent success in attempt {attempt} param {int(param):05}")

            else:
                found = False  # Looke promising but yielded no data
                print("attempt yielded no data")

    if not found:
        logger.debug(f"Station {station} query failed or produced no data")
        if (station, paramname) not in failures:
            failures.append((station, paramname))


def nwis_download(stations, dest_dir, start, end=None, param=None, overwrite=False, max_workers=4):
    """Download robot for NWIS
    Requires a list of stations, destination directory and start/end date
    These dates are passed on to CDEC ... actual return dates can be
    slightly different
    """
    if end is None:
        end = dt.datetime.now()
        endfile = 9999
    else:
        endfile = end.year
    if not os.path.exists(dest_dir):
        os.mkdir(dest_dir)

    failures = []
    skips = []
    successes = set()
    # Use ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Schedule the download tasks and handle them asynchronously
        futures = []
        for ndx, row in stations.iterrows():
            future = executor.submit(
                download_station,
                row,
                dest_dir,
                start,
                end,
                param,
                overwrite,
                endfile,
                successes,
                failures,
                skips,
            )
            futures.append(future)

        # Optionally, handle the results of the tasks
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()  # This line can be used to handle results or exceptions from the tasks
            except Exception as e:
                logger.debug(traceback.print_tb(e.__traceback__))
                logger.error(f"Exception occurred during download: {e}")

    if len(failures) == 0:
        logger.info("No failed stations")
    else:
        logger.info("Failed query stations: ")
        for failure in failures:
            logger.info(failure)


def parse_start_year(txt):
    date_re = re.compile(
        r"(19|20)\d\d[- /.](0[1-9]|1[012])[- /.](0[1-9]|[12][0-9]|3[01])"
    )
    if os.path.exists(txt):
        # assume file
        for iline, line in enumerate(open(txt, "r")):
            if iline > 1000:
                return None
            if line.startswith("#"):
                continue
            m = date_re.search(line)
            if m is not None:
                return int(m.group(0)[0:4])
        return None
    else:
        for iline, line in enumerate(iter(txt.splitlines())):
            if iline > 1000:
                return None
            if line.startswith("#"):
                continue
            m = date_re.search(line)
            if m is not None:
                return int(m.group(0)[0:4])
    return None


def download_nwis(dest_dir, start, end, param, stations, overwrite, stationfile):
    """Download robot for NWIS (National Water Information System)."""
    destdir = dest_dir
    stime = dt.datetime(*list(map(int, re.split(r"[^\d]", start))))
    if end:
        etime = dt.datetime(*list(map(int, re.split(r"[^\d]", end))))
    else:
        etime = dt.datetime.now()

    stationfile = stationfile_or_stations(stationfile, stations)
    slookup = dstore_config.config_file("station_dbase")
    vlookup = dstore_config.config_file("variable_mappings")
    df = process_station_list(
        stationfile,
        param=param,
        station_lookup=slookup,
        agency_id_col="agency_id",
        param_lookup=vlookup,
        source="usgs",
    )
    nwis_download(df, destdir, stime, etime, param, overwrite)


@click.command()
@click.help_option("-h", "--help")
@click.option(
    "--dest",
    "dest_dir",
    default="nwis_download",
    help="Destination directory for downloaded files.",
)
@click.option(
    "--start",
    required=None,
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
    help='Parameter(s) to be downloaded, e.g. 00065 = gage height (ft.), 00060 = streamflow (cu ft/sec) and 00010 = water temperature in degrees Celsius. See "http://help.waterdata.usgs.gov/codes-and-parameters/parameters" for complete listing. (if not specified, all the available parameters will be downloaded)',
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
@click.option("--logdir", type=click.Path(path_type=Path), default="logs")
@click.option("--debug", is_flag=True)
@click.option("--quiet", is_flag=True)
@click.help_option("-h", "--help")
@click.argument("stationfile", nargs=-1)
def download_nwis_cli(dest_dir, start, end, param, stations, overwrite, stationfile,logdir, debug, quiet):
    """CLI for downloading NWIS (National Water Information System)."""
    
    
    level, console = resolve_loglevel(
        debug=debug,
        quiet=quiet,
    )
    configure_logging(
            package_name="dms_datastore",
            level=level,
            console=console,
            logdir=logdir,
            logfile_prefix="download_nwis",  # per-CLI identity
        
    )    
    download_nwis(dest_dir, start, end, param, stations, overwrite, stationfile)


if __name__ == "__main__":
    download_nwis_cli()
