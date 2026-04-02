#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Scripts to populate raw/incoming with populate() obtaining des, usgs, noaa, usgs, usbr
usgs: files may have two series
des: naive download will produce files from different instruments with time overlaps
     the script    run rationalize_time_partitions for des

ncro: typically done with download_ncro which is a period of record downloader
      ncro is not realtime run populate2 to get the update for ncro
run revise_time to correct start and end times.

What are steps to update just realtime

Need to add something for the daily stations and for O&M (Clifton Court, Banks)
"""

import glob
import os
import shutil
import traceback
import click
import concurrent.futures
import pandas as pd
from pathlib import Path
from dms_datastore.process_station_variable import (
    attach_agency_id,
    attach_src_var_id,
    normalize_station_request,
    read_station_subloc,
    merge_station_subloc,
)
from dms_datastore import dstore_config
from dms_datastore.filename import interpret_fname, meta_to_filename, naming_spec
from dms_datastore.read_ts import read_ts
from dms_datastore.download_nwis import nwis_download
from dms_datastore.download_noaa import noaa_download
from dms_datastore.download_cdec import cdec_download
from dms_datastore.download_ncro import ncro_download, mapping_df
from dms_datastore.rationalize_time_partitions import rationalize_time_partitions
from dms_datastore.logging_config import configure_logging, resolve_loglevel
import logging
logger = logging.getLogger(__name__)

from dms_datastore.download_des import des_download

__all__ = [
    "revise_filename_syears",
    "revise_filename_syear_eyear",
    "populate_repo",
    "populate_ncro_realtime",
    "populate_ncro_repo"
]

NSAMPLE_DATA = 200

# Raw/incoming naming profile used only for parsing and renaming downloader outputs.
# First slot is the acquisition/serving agency label used by the downloader output.
RAW_NAMING = naming_spec(
    templates=[
        "{agency}_{station_id@subloc}_{agency_id}_{param}_{syear}_{eyear}.csv",
        "{agency}_{station_id@subloc}_{agency_id}_{param}_{year}.csv",
    ]
)

downloaders = {
    "dwr_des": des_download,
    "noaa": noaa_download,
    "usgs": nwis_download,
    "usbr": cdec_download,
    "dwr": cdec_download,
    "cdec": cdec_download,
    "ncro": ncro_download,
}


def _quarantine_file(fname, quarantine_dir="quarantine"):
    if not os.path.exists(quarantine_dir):
        os.makedirs("quarantine")
    shutil.copy(fname, "quarantine")


def _raw_meta_from_fname(fname):
    """Parse a downloader/raw filename with the raw naming profile."""
    return interpret_fname(os.path.basename(fname), naming=RAW_NAMING)


def _rename_with_meta(fname, new_meta, *, force=True):
    """Render a new raw-style filename from metadata and rename on disk."""
    direct = os.path.dirname(fname)
    newbase = meta_to_filename(new_meta, naming=RAW_NAMING)
    newname = os.path.join(direct, newbase)
    if fname == newname:
        return None
    if force:
        os.replace(fname, newname)
    else:
        os.rename(fname, newname)
    return newname


def revise_filename_syears(pat, force=True, outfile="rename.txt"):
    """Revise start year of files matching pat to the first year of valid data."""
    filelist = glob.glob(pat)

    renames = []
    for fname in filelist:
        meta = _raw_meta_from_fname(fname)
        ts = read_ts(fname, nrows=200, force_regular=False)
        if ts.first_valid_index() is None:
            raise ValueError(f"Issue obtaining start time from file: {fname}")

        newstart = str(ts.first_valid_index().year)
        new_meta = dict(meta)
        if "syear" in new_meta:
            new_meta["syear"] = newstart
        elif "year" in new_meta:
            new_meta["year"] = newstart
        else:
            raise ValueError(f"No year-like field found in parsed raw filename: {fname}")

        newname = _rename_with_meta(fname, new_meta, force=force)
        if newname is not None:
            logger.info(f"Renaming {fname} to {newname}")
            renames.append((fname, newname))

    _write_renames(renames, outfile)


def revise_filename_syear_eyear(pat, force=True, outfile="rename.txt"):
    """Revise start and end year of raw files to match valid data years."""
    logger.info(f"Beginning revise_filename_syear_eyear for pattern: {pat}")

    filelist = glob.glob(pat)
    bad = []
    renames = []
    for fname in filelist:
        meta = _raw_meta_from_fname(fname)
        ts = None
        try:
            ts = read_ts(fname, force_regular=False)
        except Exception:
            file_size = os.path.getsize(fname)
            if file_size < 25000:
                os.remove(fname)
                bad.append(fname + " (small,deleted)")
                logger.info(
                    f"Small file {fname} caused read exception. Deleted during rename"
                )
            else:
                _quarantine_file(fname, "quarantine")
                bad.append(fname + " (not small, not deleted)")
                logger.info(
                    f"non-small file {fname} caused read exception. Not deleted during rename"
                )
            continue

        if ts is None:
            logger.info(f"File {fname} produced None during read")
            bad.append(fname + " returned None for time series")
            os.remove(fname)
            continue

        if ts.first_valid_index() is None:
            if ts.isnull().all(axis=None):
                logger.info(f"All values are bad. Deleting file {fname}")
                bad.append(fname + " (all bad, deleting)")
                os.remove(fname)
                continue
            raise ValueError(f"Issue obtaining start time from file: {fname}")

        if not hasattr(ts.first_valid_index(), "year"):
            logger.info(
                f"Index in file {fname} not a time stamp: {ts.first_valid_index()}"
            )
            bad.append(fname + " (first index not a time stamp)")
            os.remove(fname)
            continue

        new_meta = dict(meta)
        newstart = str(ts.first_valid_index().year)
        if "year" in new_meta:
            new_meta["year"] = newstart
        else:
            new_meta["syear"] = newstart
            oldend = str(new_meta.get("eyear", "9999"))
            new_meta["eyear"] = oldend if oldend == "9999" else str(ts.last_valid_index().year)

        newname = _rename_with_meta(fname, new_meta, force=force)
        if newname is None:
            logger.debug(f"Not renaming {fname}")
        else:
            logger.info(f"Renaming {fname} to {newname}")
            renames.append((fname, newname))

    _write_renames(renames, outfile)
    if len(bad) > 0:
        logger.info("Bad files:")
        for b in bad:
            logger.info(b)
    logger.info(f"Renaming complete for pattern: {pat}")


def populate_repo(
    agency, param, dest, start, end, overwrite=False, ignore_existing=None
):
    """Populate repository for the given agency/source and parameter."""
    maximize_subloc = False

    slookup = dstore_config.config_file("station_dbase")
    if "ncro" in agency:
        vlookup = mapping_df
        agency = "ncro"
    else:
        vlookup = dstore_config.config_file("variable_mappings")

    subloclookup = dstore_config.config_file("sublocations")
    df = pd.read_csv(slookup, sep=",", comment="#", header=0, dtype={"agency_id": str})
    filter_agency = "dwr_ncro" if agency == "ncro" else agency
    df = df.loc[df.agency.str.lower() == filter_agency, :]
    df["agency_id"] = df["agency_id"].str.replace("'", "", regex=True)

    dfsub = read_station_subloc(subloclookup)
    df = merge_station_subloc(df, dfsub, default_z=-0.5)

    df = df.reset_index()

    if ignore_existing is not None:
        df = df[~df["station_id"].isin(ignore_existing)]

    dest_dir = dest
    source = "cdec" if agency in ["dwr", "usbr"] else agency
    agency_id_col = "cdec_id" if source == "cdec" else "agency_id"

    df = df[["station_id", "subloc"]]

    stationlist = normalize_station_request(
        stationframe=df,
        param=param,
        default_subloc="default",
    )
    stationlist = attach_agency_id(stationlist, repo_name="formatted", agency_id_col=agency_id_col)
    stationlist = attach_src_var_id(stationlist, vlookup, source=source)
    if maximize_subloc:
        stationlist["subloc"] = "default"
        if param not in ["flow", "elev"]:
            sl1 = stationlist.copy()
            sl1["subloc"] = "upper"
            sl2 = stationlist.copy()
            sl2["subloc"] = "lower"
            stationlist = pd.concat([stationlist, sl1, sl2], axis=0)

    result = downloaders[agency](stationlist, dest_dir, start, end, param, overwrite)
    return result if result is not None else []


def _write_renames(renames, outfile):
    writedf = pd.DataFrame.from_records(renames, columns=["from", "to"])
    writedf.to_csv(outfile, sep=",", header=True)


def existing_stations(pat):
    allfiles = glob.glob(pat)
    existing = set()
    for f in allfiles:
        meta = _raw_meta_from_fname(f)
        existing.add(meta["station_id"])
    return existing


def list_ncro_stations(dest):
    """List stations available in dest for ncro realtime update."""
    allfiles = glob.glob(os.path.join(dest, "ncro_*.csv"))

    stationlist = []
    for x in allfiles:
        try:
            meta = _raw_meta_from_fname(x)
            stationlist.append((meta["station_id"], meta["param"], "cdec", meta["agency_id"]))
        except Exception:
            logger.info(x)
            raise ValueError(f"Unable to parse station and parameter from name {x}")

    return pd.DataFrame(
        data=stationlist, columns=["id", "param", "agency", "agency_id_from_file"]
    )


def populate_repo2(df, dest, start, overwrite=False, ignore_existing=None):
    """Currently used by ncro realtime."""
    vlookup = dstore_config.config_file("variable_mappings")
    df["station_id"] = df["id"].str.replace("'", "")
    df["subloc"] = "default"

    if ignore_existing is not None:
        df = df[~df["id"].isin(ignore_existing)]

    source = "cdec"
    agency_id_col = "agency_id_from_file"
    stationlist = normalize_station_request(stationframe=df, default_subloc="default")
    stationlist = attach_agency_id(stationlist, repo_name="formatted", agency_id_col=agency_id_col)
    stationlist = attach_src_var_id(stationlist, vlookup, source=source)
    end = None
    downloaders["cdec"](stationlist, dest, start, end, overwrite)


def populate(dest, all_agencies=None, varlist=None, partial_update=False):
    logger.info(f"dest: {dest} agencies: {all_agencies}")
    doneagency = []
    station_failures = []

    purge = False
    ignore_existing = None
    if all_agencies is None:
        all_agencies = ["usgs", "dwr_des", "dwr_ncro", "usbr", "noaa", "dwr"]

    if not isinstance(all_agencies, list):
        all_agencies = [all_agencies]

    for agency in all_agencies:
        if agency == "noaa":
            if varlist is None or len(varlist) == 0:
                varlist = ["elev"]
        else:
            if varlist is None or len(varlist) == 0:
                varlist = [
                    "flow",
                    "elev",
                    "ec",
                    "temp",
                    "do",
                    "turbidity",
                    "velocity",
                    "ph",
                    "ssc",
                ]

        if agency == "dwr_des":
            for var in varlist:
                logger.info(f"Calling populate_repo with agency {agency} variable: {var}")
                if not partial_update:
                    station_failures += populate_repo(
                        agency,
                        var,
                        dest,
                        pd.Timestamp(1980, 1, 1),
                        pd.Timestamp(1999, 12, 31, 23, 59),
                        ignore_existing=ignore_existing,
                    )
                    station_failures += populate_repo(
                        agency,
                        var,
                        dest,
                        pd.Timestamp(2000, 1, 1),
                        pd.Timestamp(2019, 12, 31, 23, 59),
                        ignore_existing=ignore_existing,
                    )
                station_failures += populate_repo(
                    agency, var, dest, pd.Timestamp(2020, 1, 1), None, overwrite=True
                )
                ext = "rdb" if agency == "usgs" else ".csv"
                revise_filename_syear_eyear(os.path.join(dest, f"{agency}*_{var}_*.{ext}"))
                logger.info(f"Done with agency {agency} variable: {var}")

        else:
            for var in varlist:
                if not partial_update:
                    logger.info(
                        f"Calling populate_repo (1) with agency {agency} variable: {var}  start: 1980-01-01"
                    )
                    station_failures += populate_repo(
                        agency,
                        var,
                        dest,
                        pd.Timestamp(1980, 1, 1),
                        pd.Timestamp(1999, 12, 31, 23, 59),
                        ignore_existing=ignore_existing,
                    )
                    logger.info(
                        f"Calling populate_repo (2) with agency {agency} variable: {var} start: 2000-01-01"
                    )
                    station_failures += populate_repo(
                        agency,
                        var,
                        dest,
                        pd.Timestamp(2000, 1, 1),
                        pd.Timestamp(2019, 12, 31, 23, 59),
                        ignore_existing=ignore_existing,
                    )
                logger.info(
                    f"Calling populate_repo (3) with agency {agency} variable: {var}  start: 2020-01-01"
                )
                end_download = (
                    pd.Timestamp(2039, 12, 31, 23, 59)
                    if ((agency == "noaa") and (var == "predictions"))
                    else None
                )

                station_failures += populate_repo(
                    agency,
                    var,
                    dest,
                    pd.Timestamp(2020, 1, 1),
                    end_download,
                    overwrite=True,
                )
                ext = "rdb" if agency == "usgs" else ".csv"
                revise_filename_syear_eyear(os.path.join(dest, f"{agency}*_{var}_*.{ext}"))
                logger.info(f"Done with agency {agency} variable: {var}")
        logger.info(f"Done with agency {agency} for all variables")
        doneagency.append(agency)
    logger.info("Completed population for these agencies: ")
    for agent in doneagency:
        logger.info(agent)
    return station_failures


def purge(dest):
    if purge:
        for pat in ["*.csv", "*.rdb"]:
            allfiles = glob.glob(os.path.join(dest, pat))
            for fname in allfiles:
                os.remove(fname)


def populate_ncro_realtime(dest, realtime_start=pd.Timestamp(2021, 1, 1)):
    end = None
    ncrodf = list_ncro_stations(dest)
    populate_repo2(ncrodf, dest, realtime_start, overwrite=True)



def populate_ncro_repo(dest, variables):
    download_ncro_por(dest, variables)
    populate_ncro_realtime(dest)


def ncro_only(dest):
    populate_ncro_repo(dest, variables)
    revise_filename_syear_eyear(os.path.join(dest, f"ncro_*.csv"))
    revise_filename_syear_eyear(os.path.join(dest, f"cdec_*.csv"))


def populate_main(dest, agencies=None, varlist=None, partial_update=False, failures_file=None):
    do_purge = False
    if not os.path.exists(dest):
        raise ValueError(f"Destination directory {os.path.abspath(dest)} does not exist. Please create it before running populate.")
    else:
        if do_purge:
            purge(dest)

    agency_failures = []
    station_failures = []
    if agencies is None or len(agencies) == 0:
        all_agencies = ["usgs", "dwr_des", "usbr", "noaa", "dwr_ncro", "dwr"]
    else:
        all_agencies = agencies
    do_ncro = ("ncro" in all_agencies) or ("dwr_ncro" in all_agencies)
    do_des = ("des" in all_agencies) or ("dwr_des" in all_agencies)

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        future_to_agency = {
            executor.submit(populate, dest, agency, varlist, partial_update): agency
            for agency in all_agencies
        }

    for future in concurrent.futures.as_completed(future_to_agency):
        agency = future_to_agency[future]
        try:
            result = future.result()
            if result:
                station_failures.extend(result)
        except Exception as exc:
            agency_failures.append(agency)
            trace = traceback.format_exc()
            logger.info(f"{agency} generated an exception: {exc} with trace:\n{trace}")
            station_failures.append({
                "agency": agency,
                "station_id": None,
                "agency_id": None,
                "param": None,
                "subloc": None,
                "exc_type": type(exc).__name__,
                "message": str(exc),
            })
        if "ncro" in agency:
            populate_ncro_realtime(dest)

    if do_des:        
        rationalize_time_partitions(
            "des*_*.csv",
            spec="des_rationalize_time_spec",
            root_dir=dest,
            dry_run=False,
            warn_on_remaining_overlap=True,
        )

    if do_ncro:
        revise_filename_syear_eyear(os.path.join(dest, f"ncro_*.csv"))
    revise_filename_syear_eyear(os.path.join(dest, f"cdec_*.csv"))
    logger.info("These agency queries failed")

    # Write failures CSV
    if failures_file is None:
        logdir = Path("logs")
        logdir.mkdir(exist_ok=True)
        failures_file = logdir / "populate_repo_failures.csv"
    failures_file = Path(failures_file)
    failures_file.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        station_failures,
        columns=["agency", "station_id", "agency_id", "param", "subloc", "exc_type", "message"],
    ).to_csv(failures_file, index=False)
    logger.info(f"Failures written to {failures_file} ({len(station_failures)} entries)")


def populate_debug_ncro_rename(dest, agencies=None, varlist=None):
    do_purge = False
    if not os.path.exists(dest):
        raise ValueError(f"Destination directory {os.path.abspath(dest)} does not exist. Please create it before running populate.")
    else:
        if do_purge:
            purge(dest)

    if agencies is None or len(agencies) == 0:
        all_agencies = ["usgs", "dwr_des", "usbr", "noaa", "dwr_ncro", "dwr"]
    else:
        all_agencies = agencies
    do_ncro = ("ncro" in all_agencies) or ("dwr_ncro" in all_agencies)
    if do_ncro:
        revise_filename_syear_eyear(os.path.join(dest, f"ncro_*.csv"))
    revise_filename_syear_eyear(os.path.join(dest, f"cdec_*.csv"))
    logger.info("These agency queries failed")


@click.command()
@click.option(
    "--dest",
    required=True,
    help="Directory where files will be stored.",
)
@click.option(
    "--agencies",
    multiple=True,
    default=None,
    help="Agencies to download. If none, a default list is used",
)
@click.option(
    "--variables",
    multiple=True,
    default=None,
    help="Variables to download. If none, a default list is used",
)
@click.option(
    "--partial",
    is_flag=True,
    default=False,
    help="Partial update assuming existing files and only updating from 2020 onwards",
)
@click.option("--logdir", type=click.Path(path_type=Path), default="logs")
@click.option("--debug", is_flag=True)
@click.option("--quiet", is_flag=True)
@click.option(
    "--failures-file",
    type=click.Path(path_type=Path),
    default=None,
    help="Path for the failures CSV. Defaults to {logdir}/populate_repo_failures.csv.",
)
@click.help_option("-h", "--help")
def populate_main_cli(dest, agencies, variables, partial, logdir="logs", debug=False, quiet=False, failures_file=None):
    """Populate repository with data from various agencies."""

    level, console = resolve_loglevel(
        debug=debug,
        quiet=quiet,
    )
    configure_logging(
          package_name="dms_datastore",
          level=level,
          console=console,
          logdir=logdir,
          logfile_prefix="populate_repo"
    )
    varlist = list(variables) if variables else None
    agencies_list = list(agencies) if agencies else None
    logger.info(f"dest: {dest}, agencies: {agencies_list}, varlist:{varlist}")
    effective_failures_file = failures_file if failures_file is not None else Path(logdir) / "populate_repo_failures.csv"
    populate_main(dest, agencies_list, varlist=varlist, partial_update=partial, failures_file=effective_failures_file)


if __name__ == "__main__":
    populate_main_cli()
