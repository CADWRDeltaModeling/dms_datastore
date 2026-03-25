#!/usr/bin/env python
import asyncio
import click
import ssl
import httpx
import pandas as pd
import re
import zipfile
import os
import io
import string
import datetime as dt
import numpy as np
import time
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dms_datastore import read_ts
from dms_datastore.write_ts import write_ts_csv
from dms_datastore.process_station_variable import (
    process_station_list,
    stationfile_or_stations,
)

from dms_datastore import dstore_config
from pathlib import Path
from dms_datastore.logging_config import configure_logging, resolve_loglevel 
import logging
logger = logging.getLogger(__name__)

# REQUEST_CHUNK_YEARS controls the maximum span of an individual request.
# If ALIGN_CHUNKS_TO_YEAR_MODULUS is True, chunk boundaries are aligned to
# multiples of REQUEST_CHUNK_YEARS (e.g., 2010/2015/2020 for 5-year chunks).
# -----------------------------------------------------------------------------

REQUEST_CHUNK_YEARS = 5
ALIGN_CHUNKS_TO_YEAR_MODULUS = True
RETAIN_INVENTORY_DAYS = 14
NCRO_MAX_WORKERS = 4
NCRO_HTTP_TIMEOUT = 60.0  # seconds; increase if inventory downloads time out
INVENTORY_MAX_WORKERS = 6
INVENTORY_MAX_ATTEMPTS = 8
NCRO_MIN_EXPECTED_ENTRIES = 515
NCRO_MAX_FAILED_UPDATES = 7

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
    "DissolvedOxygenPercentage": None,
    "StreamLevel": "elev",
    "WaterSurfaceElevationNAVD88": "elev",
    "fDOM": "fdom",
}


mapping_df = pd.DataFrame(list(mappings.items()), columns=["src_var_id", "var_name"])
mapping_df["src_name"] = "ncro"


def similar_ncro_station_names(site_id):
    """This routine is here to convert a single site_id to a short list of related names.
    The reason for the routine is that NCRO surface water stations identifiers
    don't correspond well to our abstraction of a station.
    There are station ids that are stripped down B1234, or that have added 00 digits B9123400
    or that have added Q B91234Q
    """
    if site_id.lower().endswith("q"):
        base_id = site_id[:-1]
    elif site_id.lower().endswith("00") and len(site_id) > 6:
        base_id = site_id[:-2]
    else:
        base_id = site_id
    return [base_id.upper(), base_id.upper() + "Q", base_id.upper() + "00"]


ncro_inventory_file = "ncro_por_inventory.txt"
ncro_inventory = None
inventory_dir = os.path.split(__file__)[0]
inventoryfile = os.path.join(inventory_dir, "ncro_inventory_full.csv")


def _inventory_header_defaults():
    return {"last_update": None, "failed_updates": 0}


def _parse_inventory_header(path):
    meta = _inventory_header_defaults()
    if not os.path.exists(path):
        return meta

    with open(path, "r", encoding="utf-8") as fobj:
        for line in fobj:
            if not line.startswith("#"):
                break
            payload = line[1:].strip()
            if ":" not in payload:
                continue
            key, value = payload.split(":", 1)
            key = key.strip()
            value = value.strip()
            if key == "last_update" and value:
                meta["last_update"] = pd.to_datetime(value).date()
            elif key == "failed_updates" and value:
                meta["failed_updates"] = int(value)

    if meta["last_update"] is None:
        meta["last_update"] = dt.date.fromtimestamp(os.stat(path).st_mtime)

    return meta


def _read_inventory_file(path):
    meta = _parse_inventory_header(path)
    df = pd.read_csv(path, comment="#", parse_dates=["start_time", "end_time"])
    unnamed = [col for col in df.columns if str(col).startswith("Unnamed:")]
    if unnamed:
        df = df.drop(columns=unnamed)
    if "index" in df.columns:
        df = df.drop(columns=["index"])
    return df, meta


def _write_inventory_file(df, path, *, last_update, failed_updates):
    df = df.drop_duplicates()
    df = df.sort_values(by=["site", "trace", "start_time", "end_time"]).reset_index(drop=True)
    with open(path, "w", encoding="utf-8", newline="") as fobj:
        if last_update is not None:
            fobj.write(f"# last_update: {last_update:%Y-%m-%d}\n")
        else:
            fobj.write("# last_update: \n")
        fobj.write(f"# failed_updates: {int(failed_updates)}\n")
        df.to_csv(fobj, index=False)


def _inventory_is_reliable(df):
    return df is not None and len(df) >= NCRO_MIN_EXPECTED_ENTRIES


def _inventory_refresh_needed(inventory_prev, meta_prev, force_update):
    if force_update:
        return True
    if inventory_prev is None:
        return True
    if not _inventory_is_reliable(inventory_prev):
        return True

    last_update = meta_prev.get("last_update")
    if last_update is None:
        return True

    age_days = (dt.date.today() - last_update).days
    return age_days >= RETAIN_INVENTORY_DAYS


def _is_retryable_inventory_exception(exc):
    if isinstance(exc, (httpx.ReadTimeout, httpx.RemoteProtocolError, httpx.ConnectError)):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        return status == 429 or 500 <= status < 600
    if isinstance(exc, httpx.TransportError):
        return True
    return False


def _flatten_inventory_payload(origname, data2):
    flattened_data = []
    for site in data2["return"]["sites"]:
        if site is None or "site" not in site:
            logger.debug(f"Bad file for {origname}")
            continue
        site_name = site["site"]
        for trace in site["traces"]:
            trace_data = trace.copy()
            trace_data["site"] = site_name
            flattened_data.append(trace_data)

    df2 = pd.DataFrame(flattened_data)
    if df2.empty:
        return df2

    df2 = df2.loc[df2["trace"].str.endswith("RAW"), :].copy()
    if df2.empty:
        return df2

    df2["start_time"] = pd.to_datetime(df2["start_time"])
    df2["end_time"] = pd.to_datetime(df2["end_time"])
    return df2


def _fetch_inventory_for_station(agency_id, abort_event):
    origname = agency_id
    names = similar_ncro_station_names(origname)
    url = f"https://wdlhyd.water.ca.gov/hydstra/sites/{','.join(names)}/traces"

    for attempt in range(1, INVENTORY_MAX_ATTEMPTS + 1):
        if abort_event.is_set():
            return None
        try:
            with httpx.Client(timeout=NCRO_HTTP_TIMEOUT) as client:
                response = client.get(url)
                response.raise_for_status()
                data2 = response.json()
            return _flatten_inventory_payload(origname, data2)
        except Exception as exc:
            if not _is_retryable_inventory_exception(exc) or attempt == INVENTORY_MAX_ATTEMPTS:
                logger.warning(f"NCRO Inventory: failed for agency_id {origname}: {exc}")
                raise

            sleep_time = min(2 ** (attempt - 1), 20.0)
            logger.debug(
                f"NCRO Inventory: retry {attempt}/{INVENTORY_MAX_ATTEMPTS} for agency_id "
                f"{origname} after error: {exc}; sleeping {sleep_time:.1f}s"
            )
            if abort_event.wait(sleep_time):
                return None


def _download_inventory_from_server():
    logger.debug("NCRO Inventory: Starting to download inventory from NCRO")

    dbase = dstore_config.station_dbase()
    dbase = dbase.loc[dbase["agency"].str.contains("ncro"), :]
    agency_ids = list(dict.fromkeys(dbase["agency_id"].tolist()))

    abort_event = threading.Event()
    dfs = []
    failures = []

    with ThreadPoolExecutor(max_workers=INVENTORY_MAX_WORKERS) as executor:
        future_to_agency = {
            executor.submit(_fetch_inventory_for_station, agency_id, abort_event): agency_id
            for agency_id in agency_ids
        }

        for future in as_completed(future_to_agency):
            agency_id = future_to_agency[future]
            try:
                df2 = future.result()
            except Exception as exc:
                failures.append((agency_id, str(exc)))
                abort_event.set()
                for other_future in future_to_agency:
                    other_future.cancel()
                break

            if df2 is not None and not df2.empty:
                dfs.append(df2)

    if failures:
        raise RuntimeError(
            "NCRO Inventory refresh failed for "
            + ", ".join(f"{agency_id} ({err})" for agency_id, err in failures)
        )

    if not dfs:
        raise RuntimeError("NCRO Inventory refresh returned no inventory records")

    df_full = pd.concat(dfs, axis=0, ignore_index=True)
    df_full = df_full.drop_duplicates()
    df_full = df_full.sort_values(by=["site", "trace", "start_time", "end_time"]).reset_index(drop=True)
    return df_full


def _handle_failed_inventory_refresh(inventory_prev, meta_prev, reason):
    reliable_prev = _inventory_is_reliable(inventory_prev)
    previous_failed_updates = int(meta_prev.get("failed_updates", 0))
    new_failed_updates = previous_failed_updates + 1

    if reliable_prev and new_failed_updates <= NCRO_MAX_FAILED_UPDATES:
        _write_inventory_file(
            inventory_prev,
            inventoryfile,
            last_update=meta_prev.get("last_update"),
            failed_updates=new_failed_updates,
        )
        logger.error(
            f"NCRO Inventory: refresh failed ({reason}). Retaining previous inventory "
            f"and incrementing failed_updates to {new_failed_updates}."
        )
        return inventory_prev

    if reliable_prev:
        raise RuntimeError(
            f"NCRO Inventory: refresh failed ({reason}) and failed_updates would exceed "
            f"the limit of {NCRO_MAX_FAILED_UPDATES}."
        )

    raise RuntimeError(
        f"NCRO Inventory: refresh failed ({reason}) and no reliable prior inventory exists."
    )


def load_inventory(force_update=False):
    global ncro_inventory, inventoryfile

    if ncro_inventory is not None and not force_update:
        return ncro_inventory

    inventory_prev = None
    meta_prev = _inventory_header_defaults()
    if os.path.exists(inventoryfile):
        logger.debug("reading existing inventory file " + inventoryfile)
        inventory_prev, meta_prev = _read_inventory_file(inventoryfile)

    if not _inventory_refresh_needed(inventory_prev, meta_prev, force_update):
        ncro_inventory = inventory_prev
        return ncro_inventory

    try:
        inventory_new = _download_inventory_from_server()
    except Exception as exc:
        ncro_inventory = _handle_failed_inventory_refresh(inventory_prev, meta_prev, str(exc))
        return ncro_inventory

    if len(inventory_new) < NCRO_MIN_EXPECTED_ENTRIES:
        reason = (
            f"download completed but only returned {len(inventory_new)} entries; "
            f"minimum expected is {NCRO_MIN_EXPECTED_ENTRIES}"
        )
        ncro_inventory = _handle_failed_inventory_refresh(inventory_prev, meta_prev, reason)
        return ncro_inventory

    _write_inventory_file(
        inventory_new,
        inventoryfile,
        last_update=dt.date.today(),
        failed_updates=0,
    )
    ncro_inventory = inventory_new
    return ncro_inventory


async def _async_download_trace(client, site, trace, stime, etime):
    """Download time series trace associated with one request"""
    url_trace = f"https://wdlhyd.water.ca.gov/hydstra/sites/{site}/traces/{trace}/points?start-time={stime.strftime('%Y%m%d%H%M%S')}&end-time={etime.strftime('%Y%m%d%H%M%S')}"
    max_attempt = 4

    attempt = 0
    while attempt < max_attempt:
        attempt = attempt + 1
        try:
            if attempt > 1:
                logger.debug(f"{url_trace} download attempt {attempt}")
                if attempt > 16:
                    logger.debug(fname)
            logger.debug(f"Submitting request to URL {url_trace} attempt {attempt}")
            # time1=time.time()
            streaming = False
            response = await client.get(url_trace, timeout=200.0)
            response.raise_for_status()
            if streaming:
                pass
                # station_html = ""
                # for chunk in response.iter_lines(chunk_size=4096):  # Iterate over lines
                #    if chunk:  # Filter out keep-alive new chunks
                #        station_html += chunk.decode()+"\n"
            station_html = response.text.replace("\r", "")
            break
        except Exception as e:
            logger.debug(f"Exception on attempt {attempt}: " + str(e))
            if attempt == max_attempt:
                logger.warning(
                    f"Failed all attempts to download trace for station {site} trace {trace} url {url_trace}: {e}"
                )
                return None
            else:
                await asyncio.sleep(
                    1
                )  # Wait one second more second each time to clear any short term bad stuff
    return station_html


def parse_json_to_series(json_txt):
    jsdata = json.loads(json_txt)

    traces = jsdata["return"]["traces"]
    if len(traces) > 1:
        raise ValueError("Multiple trace json responses not supported")

    # Preallocate lists for columns
    sites, times, values, qualities = [], [], [], []

    # Populate the lists efficiently
    for trace_entry in traces:
        site = trace_entry["site"]
        site_details = trace_entry["site_details"]
        trace = trace_entry["trace"]
        trace_details = trace_entry["trace_details"]

        for record in trace:
            times.append(record["t"])
            values.append(record["v"])
            qualities.append(record["q"])

        # Create DataFrame directly from lists
    df = pd.DataFrame(
        {
            "datetime": pd.to_datetime(
                times, format="%Y%m%d%H%M%S"
            ),  # Vectorized timestamp parsing
            "value": pd.to_numeric(
                values, errors="coerce"
            ),  # Vectorized conversion to float
            "qaqc_flag": qualities,
        }
    )

    # Set 't' as the index
    df.set_index("datetime", inplace=True)

    return site, site_details, trace_details, df


def ncro_metadata(station_id, agency_id, site_details, trace_details, paramname):
    meta = {}
    meta["provider"] = "DWR-NCRO"
    meta["station_id"] = station_id
    meta["agency_station_id"] = agency_id
    meta["agency_station_name"] = site_details["name"]
    meta["agency_unit"] = trace_details["unit"]
    meta["agency_param_desc"] = trace_details["desc"]
    meta["param"] = paramname
    return meta


async def _async_download_trace_chunked(client, site, trace, stime, etime):
    """Download one site/trace by splitting into smaller requests.

    Returns: (site, site_details, trace_details, df) or None.
    """

    dfs = []
    site_details = None
    trace_details = None

    for cstart, cend in iter_time_chunks(stime, etime):
        txt = await _async_download_trace(client, site, trace, cstart, cend)
        if txt is None:
            continue
        parsed_site, parsed_site_details, parsed_trace_details, df = parse_json_to_series(txt)
        site_details = parsed_site_details
        trace_details = parsed_trace_details
        dfs.append(df)

    if not dfs:
        return None

    out = pd.concat(dfs, axis=0)
    out = out[~out.index.duplicated(keep="last")]
    out = out.sort_index()
    return site, site_details, trace_details, out


def iter_time_chunks(
    stime,
    etime,
    chunk_years=REQUEST_CHUNK_YEARS,
    align_to_modulus=ALIGN_CHUNKS_TO_YEAR_MODULUS,
):
    """Yield (chunk_start, chunk_end) windows covering [stime, etime].

    - chunk_end is exclusive, except for the final chunk where it may equal etime.
    - If align_to_modulus is True, boundaries fall on Jan 1 of years that are
      multiples of chunk_years (e.g., 2010/2015/2020 for chunk_years=5).

    NOTE: This is about request sizing, not resampling.
    """

    stime = pd.to_datetime(stime).to_pydatetime()
    etime = pd.to_datetime(etime).to_pydatetime()
    if etime <= stime:
        return

    cur = stime
    while cur < etime:
        if align_to_modulus:
            next_mod_year = (cur.year // chunk_years + 1) * chunk_years
            boundary = dt.datetime(next_mod_year, 1, 1)
            if boundary <= cur:
                boundary = dt.datetime(next_mod_year + chunk_years, 1, 1)
            nxt = boundary
        else:
            nxt = (pd.Timestamp(cur) + pd.DateOffset(years=chunk_years)).to_pydatetime()

        chunk_end = min(nxt, etime)
        if chunk_end <= cur:
            raise ValueError(f"Invalid chunk interval: start={cur!r} end={chunk_end!r}")
        yield cur, chunk_end
        cur = chunk_end

 
async def _async_download_one_trace_to_csv(
    *,
    client,
    semaphore,
    station_id: str,
    agency_id: str,
    paramname: str,
    site: str,
    trace: str,
    dest_dir: str,
    stime,
    etime,
    overwrite: bool,
):
    """Worker: download one (site, trace) and write a CSV.

    Parallel execution pattern is lifted from download2.py:
      - submit many of these from ncro_download()
      - then wait/harvest with as_completed().

    Request-size mitigation is handled by download_trace_chunked(), which uses
    REQUEST_CHUNK_YEARS / ALIGN_CHUNKS_TO_YEAR_MODULUS already defined in this file.
    """
    async with semaphore:
        result = await _async_download_trace_chunked(client, site, trace, stime, etime)
        if result is None:
            logger.debug(f"Empty return for site {site} trace {trace}")
            return None

        site, site_details, trace_details, df = result
        logger.debug("Chunked query produced trace")

        fname = f"ncro_{station_id}_{site}_{paramname}_{stime.year}_{etime.year}.csv".lower()
        fpath = os.path.join(dest_dir, fname)
        if os.path.exists(fpath) and not overwrite:
            logger.info(f"Skipping existing file (use --overwrite to replace): {fpath}")
            return None

        meta = ncro_metadata(station_id, agency_id, site_details, trace_details, paramname)
        write_ts_csv(
            df,
            fpath,
            metadata=meta,
            chunk_years=False,
            format_version="dwr-ncro-json",
        )
        return fpath


async def _ncro_download_async(stations, dest_dir, stime, etime, overwrite, update_inventory=False):
    failures = []
    inventory = load_inventory(force_update=update_inventory)
    _ = dstore_config.station_dbase()

    timeout = httpx.Timeout(200.0, connect=30.0)
    limits = httpx.Limits(
        max_connections=NCRO_MAX_WORKERS,
        max_keepalive_connections=NCRO_MAX_WORKERS,
    )
    semaphore = asyncio.Semaphore(NCRO_MAX_WORKERS)

    tasks = []
    task_meta = []

    async with httpx.AsyncClient(timeout=timeout, limits=limits) as client:
        for ndx, row in stations.iterrows():
            agency_id = row.agency_id
            station_id = row.station_id
            station_id = row.station_id
            param = row.src_var_id
            paramname = row.param

            subinventory = inventory.loc[
                (inventory.site.isin(similar_ncro_station_names(row.agency_id)))
                & (inventory.param == param)
                & (inventory.start_time <= etime)
                & (inventory.end_time >= stime),
                :,
            ]
            logger.debug(
                f"Found {len(subinventory)} matching traces for station {station_id} param {param}"
            )

            if subinventory.empty:
                logger.debug(
                    f"Skipping station {station_id} agency_id {agency_id} param {param} -- no data in inventory for requested period"
                )
                continue

            for tsndx, tsrow in subinventory.iterrows():
                site = tsrow.site
                trace = tsrow.trace

                proposed_fname = (
                    f"ncro_{station_id}_{site}_{paramname}_{stime.year}_{etime.year}.csv".lower()
                )
                proposed_path = os.path.join(dest_dir, proposed_fname)
                if os.path.exists(proposed_path) and not overwrite:
                    logger.info(f"Skipping existing file (use --overwrite to replace): {proposed_path}")
                    continue

                task = asyncio.create_task(
                    _async_download_one_trace_to_csv(
                        client=client,
                        semaphore=semaphore,
                        station_id=station_id,
                        agency_id=agency_id,
                        paramname=paramname,
                        site=site,
                        trace=trace,
                        dest_dir=dest_dir,
                        stime=stime,
                        etime=etime,
                        overwrite=overwrite,
                    )
                )
                tasks.append(task)
                task_meta.append((station_id, site, trace))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        for (station_id, site, trace), result in zip(task_meta, results):
            if isinstance(result, Exception):
                logger.info(
                    f"Exception occurred during download: station={station_id} site={site} trace={trace} err={result}"
                )
                failures.append((station_id, site, trace, str(result)))

    return failures


def ncro_download(stations, dest_dir, start, end=None, param=None, overwrite=False, update_inventory=False):
    """Download robot for NCRO
    Requires a list of stations, destination directory and start/end date
    """

    if end == None:
        end = dt.datetime.now()
    if not os.path.exists(dest_dir):
        os.mkdir(dest_dir)

    failures = []
    
    stime = pd.to_datetime(start)
    try:
        etime = pd.to_datetime(end)
    except:
        etime = pd.Timestamp.now()

    failures = asyncio.run(
        _ncro_download_async(
            stations=stations,
            dest_dir=dest_dir,
            stime=stime,
            etime=etime,
            overwrite=overwrite,
            update_inventory=update_inventory,
        )
    )
    return failures


def test():
    destdir = "."
    overwrite = True
    stime = pd.Timestamp(2015, 1, 1)
    etime = dt.datetime.now()
    params = ["do", "elev", "flow", "velocity", "ph", "cla", "turbidity", "temp"]
    params = ["fdom"]
    params = ["ssc"]
    for param in params:
        stations = ["orm", "old", "oh1", "bet"]
        stationfile = stationfile_or_stations(stationfile=None, stations=stations)
        slookup = dstore_config.config_file("station_dbase")
        vlookup = mapping_df
        # vlookup = dstore_config.config_file("variable_mappings")
        df = process_station_list(
            stationfile,
            param=param,
            station_lookup=slookup,
            agency_id_col="agency_id",
            param_lookup=vlookup,
            source="ncro",
        )
        print(df)
        ncro_download(df, destdir, stime, etime, overwrite=overwrite)


def test_read():
    fname = "ncro_old_b95380_temp_2015_2024.csv"
    fname = "ncro_orm_b95370_cla_*.csv"
    ts = read_ts.read_ts(fname)
    print(ts)


@click.command(
    help=(
        "Download NCRO timeseries data for selected stations/parameters. "
        "Use --inventory-only to refresh inventory metadata without downloading timeseries files."
    )
)
@click.option(
    "--dest",
    "dest_dir",
    default="ncro_download",
    help="Destination directory for downloaded files.",
)
@click.option("--start", default=None, help="Start time, format 2009-03-31 14:00")
@click.option("--end", default=None, help="End time, format 2009-03-31 14:00")
@click.option("--param", default=None, help="Parameter(s) to be downloaded.")
@click.option("--stations", multiple=True, help="Id or name of one or more stations.")
@click.option("--logdir", type=click.Path(path_type=Path), default=None)
@click.option("--debug", is_flag=True)
@click.option("--quiet", is_flag=True)
@click.option(
    "--inventory-only",
    is_flag=True,
    help="Download/refresh NCRO inventory metadata only, then exit without downloading timeseries data.",
)
@click.option(
    "--update-inventory",
    is_flag=True,
    help="Force an NCRO inventory refresh attempt even if the cached inventory is still fresh.",
)
@click.help_option("-h", "--help")
@click.argument("stationfile", nargs=-1)
@click.option(
    "--overwrite",
    is_flag=True,
    help="Overwrite existing files (if False they will be skipped, presumably for speed)",
)
def download_ncro_cli(
    dest_dir,
    stationfile,
    overwrite,
    start,
    end,
    param,
    stations,
    logdir=None,
    debug=False,
    quiet=False,
    inventory_only=False,
    update_inventory=False,
):

    level, console = resolve_loglevel(
        debug=debug,
        quiet=quiet,
    )
    logger.debug(f"Logging level set to {logging.getLevelName(level)} and console={console}")

    configure_logging(
          package_name="dms_datastore",
          level=level,
          console=not quiet,
          logdir=logdir,
          logfile_prefix="download_ncro"
    )
    logger.debug("Starting NCRO download")
    if inventory_only:
        inventory = load_inventory(force_update=update_inventory)
        logger.debug(f"NCRO inventory download complete. Records: {len(inventory)}")
        return

    if start is None:
        stime = pd.Timestamp(2024, 1, 1)
    else:
        stime = dt.datetime(*list(map(int, re.split(r"[^\d]", start))))
    if end is None:
        etime = dt.datetime.now()
    else:
        etime = dt.datetime(*list(map(int, re.split(r"[^\d]", end))))

    stationfile = stationfile_or_stations(
        list(stationfile) if stationfile else None, list(stations) if stations else None
    )
    slookup = dstore_config.config_file("station_dbase")
    vlookup = mapping_df
    # vlookup = dstore_config.config_file("variable_mappings")
    df = process_station_list(
        stationfile,
        param=param,
        station_lookup=slookup,
        agency_id_col="agency_id",
        param_lookup=vlookup,
        source="ncro",
    )

    ncro_download(df, dest_dir, stime, etime, overwrite=overwrite, update_inventory=update_inventory)


if __name__ == "__main__":
    download_ncro_cli()

