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
from urllib.parse import urlencode
from dms_datastore.write_ts import write_ts_csv

from dms_datastore.process_station_variable import (
    attach_agency_id,
    attach_src_var_id,
    normalize_station_request,
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


def _read_json_input(parseinput):
    """Read JSON from a filename or a JSON string."""
    if isinstance(parseinput, (str, os.PathLike)) and os.path.exists(parseinput):
        with open(parseinput, "r", encoding="utf-8") as file:
            return True, json.load(file)
    return False, json.loads(parseinput)


def parse_usgs_daily_json(parseinput, outfile, report_empty=False, metadata=None):
    """Parse USGS OGC daily FeatureCollection JSON to a DMS1-compatible CSV.

    Parameters
    ----------
    parseinput : str or path-like
        JSON data or filename containing USGS daily JSON.
    outfile : str
        Output file name.
    report_empty : bool or str
        If truthy, log an informational message instead of raising when no
        features are present. If a string is supplied it will be used in the log.
    metadata : dict, optional
        Additional metadata to merge into the output header.
    """
    from_file, data = _read_json_input(parseinput)

    features = data.get("features", [])
    if len(features) == 0:
        if report_empty:
            reportfname = parseinput if from_file else report_empty
            logger.info(f"No daily time series features: {reportfname}")
            return
        else:
            return

    records = []
    for feat in features:
        props = feat.get("properties", {})
        time_val = props.get("time")
        value_val = props.get("value")
        if time_val in (None, ""):
            continue
        records.append({"datetime": time_val, "value": value_val})

    if len(records) == 0:
        if report_empty:
            reportfname = parseinput if from_file else report_empty
            logger.info(f"Daily response had features but no time/value records: {reportfname}")
            return
        else:
            return

    result_df = pd.DataFrame.from_records(records)
    result_df["datetime"] = pd.to_datetime(result_df["datetime"], errors="raise")
    result_df["value"] = pd.to_numeric(result_df["value"], errors="coerce")
    result_df = result_df.sort_values("datetime")
    result_df = result_df.set_index("datetime")
    result_df.index.name = "datetime"
    result_df = result_df[["value"]]

    out_meta = {
        "format_modifier": "parse-usgs-daily-json",
        "data_source": "usgs_ogc_daily",
        "time_stamp": data.get("timeStamp"),
        "number_returned": data.get("numberReturned"),
    }
    if metadata:
        out_meta.update(metadata)

    write_ts_csv(result_df, outfile, out_meta, chunk_years=False)
    return result_df


def parse_usgs_json(parseinput, outfile, report_empty=False):
    """
    Parameters
    parseinput : json data or filename containing USGS json
    outfile : str
    Output file name

    """
    from_file, data = _read_json_input(parseinput)

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


DAILY_PAGE_LIMIT = 50000


def _count_daily_days(start, end):
    """Count inclusive whole days in a daily request window."""
    return (end.date() - start.date()).days + 1


def _check_daily_limit(start, end, limit=DAILY_PAGE_LIMIT):
    """Raise if the requested daily span could exceed the one-request cap."""
    ndays = _count_daily_days(start, end)
    if ndays > limit:
        raise ValueError(
            f"Requested daily span is {ndays} days, which exceeds the current one-request "
            f"limit of {limit}. Narrow the date range or implement pagination."
        )


def _build_usgs_daily_query(agency_id, start, end, param=None, limit=DAILY_PAGE_LIMIT):
    """Build a one-page OGC API query for USGS daily values."""
    params = {
        "f": "json",
        "lang": "en-US",
        "limit": limit,
        "properties": "time,value",
        "skipGeometry": "true",
        "sortby": "+time",
        "monitoring_location_id": f"USGS-{agency_id}",
        "time": f"{start.strftime('%Y-%m-%dT00:00:00Z')}/{end.strftime('%Y-%m-%dT00:00:00Z')}",
    }
    if param:
        params["parameter_code"] = f"{int(param):05d}"
    return "https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items?" + urlencode(params)


def _build_usgs_iv_query(agency_id, start, end, param=None, paramname=None):
    """Build a legacy NWIS IV or water-quality query for one station/parameter."""
    stime = start.strftime("%Y-%m-%d")
    etime = end.strftime("%Y-%m-%d")
    station_query_base = (
        f"https://nwis.waterservices.usgs.gov/nwis/iv/?sites={agency_id}"
        f"&startDT={stime}&endDT={etime}&format=json"
    )
    if param:
        station_query = station_query_base + f"&variable={int(param):05}"
    else:
        station_query = station_query_base

    if str(paramname).startswith("qual"):
        station_query_base = (
            f"https://nwis.waterdata.usgs.gov/nwis/qwdata?site_no={agency_id}"
            f"&begin_date={stime}&end_date={etime}&format=json"
        )
        if param:
            station_query = station_query_base + f"&parameter_cd={int(param):05}"
        else:
            station_query = station_query_base
    return station_query


def _build_station_query(agency_id, start, end, param=None, paramname=None, daily=False):
    """Build the outbound request URL for one station/parameter candidate."""
    if daily:
        _check_daily_limit(start, end)
        return _build_usgs_daily_query(agency_id, start, end, param=param)
    return _build_usgs_iv_query(agency_id, start, end, param=param, paramname=paramname)


def _request_station_text(station_query, station, agency_id, param, max_attempt=3, timeout=75):
    """Request text from USGS with bounded retries.

    Parameters
    ----------
    station_query : str
        Fully formed request URL.
    station : str
        Internal station identifier used for logging.
    agency_id : str
        Agency/site code used in the outgoing request.
    param : str or None
        Source parameter code used in the outgoing request.
    max_attempt : int, optional
        Maximum number of request attempts.
    timeout : int or float, optional
        Request timeout in seconds.

    Returns
    -------
    tuple[str, int]
        Response text and the successful attempt number.
    """
    session = requests.Session()
    last_exc = None
    for attempt in range(1, max_attempt + 1):
        logger.debug(f"attempt: {attempt} variable {int(param):05}, {station}, {agency_id}")
        try:
            response = session.get(
                station_query,
                headers={"User-Agent": "Mozilla/6.0"},
                timeout=timeout,
            )
            response.raise_for_status()
            logger.debug("Request successful, got text")
            return response.text, attempt
        except Exception as exc:
            last_exc = exc
            if attempt == max_attempt:
                break
    raise last_exc


def download_station(
    row, dest_dir, start, end, param, overwrite, endfile, daily=False
):
    """Download and parse one station/parameter candidate.

    Parameters
    ----------
    row : pandas.Series
        Normalized station request row containing station and variable metadata.
    dest_dir : str or path-like
        Destination directory for output files.
    start : datetime.datetime
        Inclusive start time for the request.
    end : datetime.datetime
        Inclusive end time for the request.
    param : str or None
        Optional parameter override from the caller. The row-specific source
        variable id is used for the actual request.
    overwrite : bool
        If False, existing files are skipped.
    endfile : int
        Year token used in the output filename.
    daily : bool, optional
        If True, use the USGS OGC daily endpoint and daily parser.

    Returns
    -------
    dict
        Result record describing the attempt. This is aggregated in the main
        thread so semantic success can be decided across multiple candidate
        source codes for the same ``(station, paramname)``.
    """
    agency_id = row.agency_id
    station = row.station_id
    param = row.src_var_id
    paramname = row.param
    semantic_key = (station, paramname)

    yearname = f"{start.year}_{endfile}"
    outfname = f"usgs_{station}_{agency_id}_{paramname}_{yearname}.csv"
    if (not daily) and str(paramname).startswith("qual"):
        outfname = f"usgs_{station}_{agency_id}_{paramname}_{param}_{yearname}.csv"
    outfname = outfname.lower()
    path = os.path.join(dest_dir, outfname)

    result = {
        "station": station,
        "paramname": paramname,
        "param_code": param,
        "semantic_key": semantic_key,
        "path": path,
        "query": None,
        "found": False,
        "skipped": False,
        "reason": None,
    }

    if os.path.exists(path) and not overwrite:
        logger.info("Skipping existing station because file exists: %s" % station)
        result["skipped"] = True
        result["reason"] = "exists"
        return result

    logger.debug(f"Attempting to download station: {station} variable {param}")
    station_query = _build_station_query(
        agency_id=agency_id,
        start=start,
        end=end,
        param=param,
        paramname=paramname,
        daily=daily,
    )
    result["query"] = station_query
    logger.debug(f"USGS Query for ({station},{paramname}): {station_query}")

    try:
        station_html, attempt = _request_station_text(station_query, station, agency_id, param)
    except Exception:
        logger.debug(f"Station {station} query failed or produced no data")
        result["reason"] = "request_failed"
        return result

    if daily:
        try:
            daily_json = json.loads(station_html)
        except json.JSONDecodeError:
            logger.info(
                f"Daily response for {station} {paramname} ({param}) was not valid JSON"
            )
            with open(path, "w", encoding="utf-8") as f:
                f.write(station_html)
            _quarantine_file(path)
            result["reason"] = "invalid_json"
            return result

        features = daily_json.get("features", [])
        if len(features) == 0:
            logger.debug(
                f"Daily response yielded no features for station {station} variable {param}"
            )
            result["reason"] = "no_data"
            return result

        logger.info(f"Parsing USGS daily JSON: {path} param {param}")
        try:
            meta = {
                "agency": "usgs",
                "source": "usgs",
                "station_id": station,
                "agency_id": agency_id,
                "param": paramname,
                "src_var_id": param,
            }
            df = parse_usgs_daily_json(
                station_html,
                path,
                report_empty=f"{station} {paramname} ({param})",
                metadata=meta,
            )
        except Exception:
            logger.info(
                f"Parsing of daily {station} {paramname} ({param}) JSON to csv failed. Writing to quarantine"
            )
            with open(path, "w", encoding="utf-8") as f:
                f.write(station_html)
            _quarantine_file(path)
            result["reason"] = "parse_failed"
            return result
    else:
        if len(station_html) < 1000:
            logger.info(
                f"Small file for station {station} param name {paramname} param code {int(param):05}"
            )
            result["reason"] = "small_response"
            return result
        if "No sites found matching" in station_html or "\"timeSeries\":[]" in station_html:
            logger.debug(
                f"Based on typical indicators, attempt yielded no data for vari {int(param):05}.txt"
            )
            result["reason"] = "no_data"
            return result

        logger.info(f"Parsing USGS JSON: {path} param {param}")
        try:
            df = parse_usgs_json(
                station_html, path, report_empty=f"{station} {paramname} ({param})"
            )
        except Exception:
            logger.info(
                f"Parsing of {station} {paramname} ({param}) JSON to csv failed. Writing to quarantine"
            )
            with open(path, "w", encoding="utf-8") as f:
                f.write(station_html)
            _quarantine_file(path)
            result["reason"] = "parse_failed"
            return result

    if df is not None and not df.empty:
        result["found"] = True
        result["reason"] = "success"
        print(f"Apparent success in attempt {attempt} param {int(param):05}")
    else:
        print("attempt yielded no data")
        result["reason"] = "no_data"

    return result


def nwis_download(stations, dest_dir, start, end=None, param=None, overwrite=False, max_workers=4, daily=False):
    """Download robot for NWIS.

    Parameters
    ----------
    stations : pandas.DataFrame
        Normalized station request table.
    dest_dir : str or path-like
        Destination directory for downloaded files.
    start : datetime.datetime
        Inclusive start time for the request.
    end : datetime.datetime, optional
        Inclusive end time for the request. If omitted, current time is used.
    param : str or None, optional
        Optional parameter override passed through the existing interface.
    overwrite : bool, optional
        If False, existing files are skipped.
    max_workers : int, optional
        Number of worker threads used for downloads.
    daily : bool, optional
        If True, use the USGS OGC daily endpoint and daily parser.

    Notes
    -----
    Success and failure are aggregated after all worker threads complete. This
    prevents misleading final failure messages when multiple candidate source
    parameter codes map to the same semantic parameter name.
    """
    if end is None:
        end = dt.datetime.now()
        endfile = 9999
    else:
        endfile = end.year
    if not os.path.exists(dest_dir):
        os.mkdir(dest_dir)

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
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
                daily,
            )
            futures.append(future)

        for future in concurrent.futures.as_completed(futures):
            try:
                results.append(future.result())
            except Exception as e:
                logger.debug(traceback.print_tb(e.__traceback__))
                logger.error(f"Exception occurred during download: {e}")

    grouped = {}
    for result in results:
        if result is None:
            continue
        key = result["semantic_key"]
        grouped.setdefault(key, []).append(result)

    final_failures = []
    for key, group in grouped.items():
        if any(item.get("found") for item in group):
            success_codes = [str(item["param_code"]).zfill(5) for item in group if item.get("found")]
            logger.debug(f"Semantic success for {key} via code(s): {', '.join(success_codes)}")
            continue
        if all(item.get("skipped") for item in group):
            continue
        final_failures.append(key)

    if len(final_failures) == 0:
        logger.info("No failed stations")
    else:
        logger.info("Failed query stations: ")
        for failure in final_failures:
            logger.info(failure)

    return results


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


def download_nwis(dest_dir, start, end, param, stations, overwrite, stationfile, daily=False):
    """Download robot for NWIS (National Water Information System)."""
    destdir = dest_dir
    stime = dt.datetime(*list(map(int, re.split(r"[^\d]", start))))
    if end:
        etime = dt.datetime(*list(map(int, re.split(r"[^\d]", end))))
    else:
        etime = dt.datetime.now()

    station_input = stationfile_or_stations(stationfile, stations)
    if isinstance(station_input, str):
        req = pd.read_csv(station_input, sep=",", comment="#", header=0)
    else:
        req = station_input

    df = normalize_station_request(
        stationframe=req if isinstance(req, pd.DataFrame) else None,
        stationlist=req if not isinstance(req, pd.DataFrame) else None,
        param=param,
        default_subloc=None,  # USGS can't tease apart sublocations
    )
    df = attach_agency_id(df, repo_name="formatted", agency_id_col="agency_id")
    vlookup = dstore_config.config_file("variable_mappings")
    df = attach_src_var_id(df, vlookup, source="usgs")
    nwis_download(df, destdir, stime, etime, param, overwrite, daily=daily)


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
@click.option(
    "--daily",
    is_flag=True,
    default=False,
    help="Use the USGS OGC daily-values endpoint and write DMS1-compatible daily output.",
)
@click.option("--logdir", type=click.Path(path_type=Path), default="logs")
@click.option("--debug", is_flag=True)
@click.option("--quiet", is_flag=True)
@click.argument("stationfile", nargs=-1)
def download_nwis_cli(dest_dir, start, end, param, stations, overwrite, daily, stationfile,logdir, debug, quiet):
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
    download_nwis(dest_dir, start, end, param, stations, overwrite, stationfile, daily=daily)


if __name__ == "__main__":
    download_nwis_cli()
