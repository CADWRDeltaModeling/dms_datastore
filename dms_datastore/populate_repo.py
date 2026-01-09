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
SAFEGUARD = False
import glob
from dms_datastore.logging_config import logger
import os
import shutil
import re
import traceback
import click
import concurrent.futures
import pandas as pd
from dms_datastore.process_station_variable import (
    process_station_list,
    stationfile_or_stations,
    read_station_subloc,
    merge_station_subloc,
)

#if not SAFEGUARD:
#    from schimpy.station import *
from dms_datastore import dstore_config
from dms_datastore.filename import interpret_fname, meta_to_filename
from dms_datastore.read_ts import read_ts
from dms_datastore.download_nwis import nwis_download, parse_start_year
from dms_datastore.download_noaa import noaa_download
from dms_datastore.download_cdec import cdec_download
from dms_datastore.download_ncro2 import ncro_download, mapping_df
#    download_ncro_por,
#    download_ncro_inventory,
#    station_dbase,
#)
from dms_datastore.download_des import des_download


__all__ = [
    "revise_filename_syears",
    "revise_filename_syear_eyear",
    "populate_repo",
    "populate_ncro_realtime",
    "populate_ncro_repo",
    "rationalize_time_partitions",
]

# number of data to read in search of start date or multivariate
NSAMPLE_DATA = 200

downloaders = {
    "dwr_des": des_download,
    "noaa": noaa_download,
    "usgs": nwis_download,
    "usbr": cdec_download,
    "dwr":  cdec_download,
    "cdec": cdec_download,
    "ncro": ncro_download,
}

def _quarantine_file(fname,quarantine_dir = "quarantine"):
    if not os.path.exists(quarantine_dir):
        os.makedirs("quarantine")
    shutil.copy(fname,"quarantine")


def revise_filename_syears(pat, force=True, outfile="rename.txt"):
    """Revise start year of files matching pat to the first year of valid data

    Parameters
    ----------
    pat : str
        Pattern to match, may include wildcards (uses glob)
    force : True
        Force renaming
    outfile : str
        Name of file to log failures

    """
    if SAFEGUARD:
        raise NotImplementedError("populate repo functions not ready to use")
    filelist = glob.glob(pat)

    renames = []
    for fname in filelist:
        direct, pat = os.path.split(fname)
        head, ext = os.path.splitext(pat)
        parts = head.split("_")
        oldstart, oldend = parts[-2:]
        ts = read_ts(fname, nrows=200, force_regular=False)
        if ts.first_valid_index() is None:
            raise ValueError(f"Issue obtaining start time from file: {fname}")
            logger.info(f"Bad: {fname}")
        else:
            newstart = str(ts.first_valid_index().year)
            newname = fname.replace(oldstart, newstart)

            if fname != newname:
                logger.info(f"Renaming {fname} to {newname}")
                renames.append((fname, newname))
                try:
                    if force:
                        os.replace(fname, newname)
                    else:
                        os.rename(fname, newname)
                except:
                    logger.info(
                        "Rename failed because of permission or overwriting issue."
                    )
                    logger.info(
                        "This can be harmless if the downloader handles clipping of the years in file names"
                    )
                    logger.info("Dumping list of renames so far to rename.txt")
                    _write_renames(fname, "rename.txt")
                    raise
    _write_renames(renames, outfile)


def revise_filename_syear_eyear(pat, force=True, outfile="rename.txt"):
    """Revise both the start year and end year of files matching pat to years of valid data

    Parameters
    ----------
    pat : str
        Pattern to match, may include wildcards (uses glob)
    force : True
        Force renaming
    outfile : str
        Name of file to log failures

    """
    return
    if SAFEGUARD:
        raise NotImplementedError("populate repo functions not ready to use")
    logger.info(f"Beginning revise_filename_syear_eyear for pattern: {pat}")

    filelist = glob.glob(pat)
    bad = []
    renames = []
    for fname in filelist:
        direct, pat = os.path.split(fname)
        head, ext = os.path.splitext(pat)
        parts = head.split("_")
        oldstart, oldend = parts[-2:]
        ts = None
        try:
            ts = read_ts(fname, force_regular=False)
        except:
            file_size = os.path.getsize(fname)
            if file_size < 25000:
                os.remove(fname)
                bad.append(fname + " (small,deleted)")
                logger.info(f"Small file {fname} caused read exception. Deleted during rename")
            else:
                quarantine_file(fname,"quarantine")
                bad.append(fname + " (not small, not deleted)")
                logger.info(f"non-small file {fname} caused read exception. Not deleted during rename")
            continue
        if ts is None:
            logger.info(f"File {fname} produced None during read")
            bad.append(fname + " returned None for time series")
            os.remove(fname)
        elif ts.first_valid_index() is None:
            if ts.isnull().all(axis=None):
                logger.info(f"All values are bad. Deleting file {fname}")
                bad.append(fname + " (all bad, deleting)")
                os.remove(fname)
            else:
                raise ValueError(f"Issue obtaining start time from file: {fname}")
        elif not hasattr(ts.first_valid_index(),"year"):
            logger.info(f"Index in file {fname} not a time stamp: {ts.first_valid_index()}")
            bad.append(fname + " (first index not a time stamp)")
            os.remove(fname)
        else:
            newstart = str(ts.first_valid_index().year)
            newend = oldend if oldend == "9999" else str(ts.last_valid_index().year)
            new_time_block = newstart + "_" + newend
            old_time_block = oldstart + "_" + oldend
            newname = fname.replace(old_time_block, new_time_block)
            
            if fname == newname:
                logger.debug(f"Not renaming {fname}")                   
            else:
                logger.info(f"Renaming {fname} to {newname}")                
                renames.append((fname, newname))
                try:
                    if force:
                        os.replace(fname, newname)
                    else:
                        os.rename(fname, newname)
                except:
                    logger.info(
                        "Rename failed because of permission or overwriting issue. The force argment may be set to False. Dumping list of renames so far to rename.txt"
                    )
                    _write_renames(rename, "rename.txt")
                    logger.info("Bad file info below:")
                    logger.info(str(bad))
                    raise

                
    _write_renames(renames, outfile)
    if len(bad) > 0:
        logger.info("Bad files:")
        for b in bad:
            logger.info(b)
    logger.info(f"Renaming complete for pattern: {pat}")


def populate_repo(
    agency, param, dest, start, end, overwrite=False, ignore_existing=None
):
    """Populate repository for the given agency/source and parameter

    Parameters
    ----------
    agency : str
        Agency to populate
    param : str
        Parameter to populate. Should be a variable on the variables.csv table
    dest : str
        Location to put files
    start : int
        year to start
    end : int
        year to end or 9999 to go to now
    overwrite : bool
        passed to downloading script
    ignore_existing : list of existing files to ignore

    Returns
    -------

    """
    if SAFEGUARD:
        raise NotImplementedError("populate repo functions not ready to use")

    # todo: This may limit usefulness for things like atmospheric
    slookup = dstore_config.config_file("station_dbase")
    if "ncro" in agency:
        vlookup = mapping_df
        agency = "ncro"   # todo: this could be cleaned up throught library
    else:
        vlookup = dstore_config.config_file("variable_mappings")


    subloclookup = dstore_config.config_file("sublocations")
    df = pd.read_csv(slookup, sep=",", comment="#", header=0, dtype={"agency_id": str})
    filter_agency = "dwr_ncro" if agency == "ncro" else agency
    df = df.loc[df.agency.str.lower() == filter_agency, :]
    df["agency_id"] = df["agency_id"].str.replace("'", "", regex=True)

    dfsub = read_station_subloc(subloclookup)
    df = merge_station_subloc(df, dfsub, default_z=-0.5)
    

    # This will be used to try upper and lower regardless of whether they are listed
    maximize_subloc = False

    df = df.reset_index()
    if ignore_existing is not None:
        df = df[~df["id"].isin(ignore_existing)]

    dest_dir = dest
    source = "cdec" if agency in ["dwr", "usbr"] else agency
    agency_id_col = "cdec_id" if source == "cdec" else "agency_id"

    df = df[["id", "subloc"]]
    stationlist = process_station_list(
        df,
        param=param,
        param_lookup=vlookup,
        station_lookup=slookup,
        agency_id_col=agency_id_col,
        source=source,
    )
  
    if maximize_subloc:
        stationlist["subloc"] = "default"
        if param not in ["flow", "elev"]:
            sl1 = stationlist.copy()
            sl1["subloc"] = "upper"
            sl2 = stationlist.copy()
            sl2["subloc"] = "lower"
            stationlist = pd.concat([stationlist, sl1, sl2], axis=0)

    downloaders[agency](stationlist, dest_dir, start, end, param, overwrite)


def _write_renames(renames, outfile):
    """Logger to write rename failures"""
    writedf = pd.DataFrame.from_records(renames, columns=["from", "to"])
    writedf.to_csv(outfile, sep=",", header=True)


def existing_stations(pat):
    if SAFEGUARD:
        raise NotImplementedError("populate repo functions not ready to use")

    allfiles = glob.glob(pat)
    existing = set()
    for f in allfiles:
        direct, fname = os.path.split(f)
        parts = fname.split("_")
        station_id = parts[1]
        existing.add(station_id)
    return existing


def list_ncro_stations(dest):
    if SAFEGUARD:
        raise NotImplementedError("populate repo functions not ready to use")

    allfiles = glob.glob(os.path.join(dest, "ncro_*.csv"))

    def station_param(x):
        parts = os.path.split(x)[1].split("_")
        try:
            return (parts[1], parts[3], "cdec", parts[2])
        except:
            logger.info(x)
            raise ValueError(f"Unable to parse station and parameter from name {x}")

    stationlist = [station_param(x) for x in allfiles]
    df = pd.DataFrame(
        data=stationlist, columns=["id", "param", "agency", "agency_id_from_file"]
    )
    return df


def populate_repo2(df, dest, start, overwrite=False, ignore_existing=None):
    if SAFEGUARD:
        raise NotImplementedError("populate repo functions not ready to use")

    """ Currently used by ncro realtime """
    slookup = dstore_config.config_file("station_dbase")
    vlookup = dstore_config.config_file("variable_mappings")
    df["station_id"] = df["id"].str.replace("'", "")
    df["subloc"] = "default"

    if ignore_existing is not None:
        df = df[~df["id"].isin(ignore_existing)]

    source = "cdec"
    agency_id_col = "agency_id_from_file"
    stationlist = process_station_list(
        df,
        param_lookup=vlookup,
        station_lookup=slookup,
        agency_id_col=agency_id_col,
        source=source,
    )
    end = None
    downloaders["cdec"](stationlist, dest, start, end, overwrite)


def populate(dest, all_agencies=None, varlist=None, partial_update=False):
    """Driver script that populates agencies in all_agencies with destination dest"""
    logger.info(f"dest: {dest} agencies: {all_agencies}")
    doneagency = []
    if SAFEGUARD:
        raise NotImplementedError("populate repo functions not ready to use")

    purge = False
    ignore_existing = None  # []
    current = pd.Timestamp.now()
    if all_agencies is None:
        all_agencies = ["usgs", "dwr_des", "dwr_ncro", "usbr", "noaa", "dwr"]

    if not isinstance(all_agencies, list):
        all_agencies = [all_agencies]

    for agency in all_agencies:
        if agency == "noaa":
            if varlist is None or len(varlist) == 0:
                # "predictions" was removed because it can be done very 
                # occasionally, when tidal epochs/fits are revised
                varlist = ["elev"]  # handled in next section
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

        # DES/DISE data from web services comes in by instrument, which can be phased in and out
        # in an overlapping way over time. Some of the early instruments have a one hour time interval
        # which introduces some complications mixing them in with faster collection later. It also causes
        # time blocking to be really weird because the neat 20 year blocks we are hoping for get truncated
        # as the new instruments come in and out of existence.
        # These things happen in the mid 2000s (often 2007 ish).
        # At the moment, I (Eli) tried to avoid this complication by consolidating the pre-2020 history.
        # It looks like big files, and this is possible, but many will be truncated because of limited
        # instrument lifetimes ... so 1980-2019 will come out as 1984-2007 or something like that.
        if agency == "dwr_des":

            for var in varlist:
                logger.info(
                    f"Calling populate_repo with agency {agency} variable: {var}"
                )
                if not partial_update:
                    # Pulls in data in two 20 year blocks, which helps with query length limits
                    # Pulls in data in two 20 year blocks, which helps with query length limits
                    populate_repo(
                        agency,
                        var,
                        dest,
                        pd.Timestamp(1980, 1, 1),
                        pd.Timestamp(1999, 12, 31, 23, 59),
                        ignore_existing=ignore_existing,
                    )
                    populate_repo(
                        agency,
                        var,
                        dest,
                        pd.Timestamp(2000, 1, 1),
                        pd.Timestamp(2019, 12, 31, 23, 59),
                        ignore_existing=ignore_existing,
                    )
                populate_repo(
                    agency, var, dest, pd.Timestamp(2020, 1, 1), None, overwrite=True
                )
                ext = "rdb" if agency == "usgs" else ".csv"
                revise_filename_syear_eyear(
                    os.path.join(dest, f"{agency}*_{var}_*.{ext}")
                )
                logger.info(f"Done with agency {agency} variable: {var}")

        else:
            for var in varlist:
                if not partial_update:
                    logger.info(
                        f"Calling populate_repo (1) with agency {agency} variable: {var}  start: 1980-01-01"
                    )
                    populate_repo(
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
                    populate_repo(
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
                end_download = pd.Timestamp(2039,12,31,23,59) if ((agency == "noaa") and (var == "predictions")) else None

                populate_repo(
                    agency, var, dest, pd.Timestamp(2020, 1, 1), end_download, overwrite=True
                )
                ext = "rdb" if agency == "usgs" else ".csv"
                revise_filename_syear_eyear(
                    os.path.join(dest, f"{agency}*_{var}_*.{ext}")
                )
                logger.info(f"Done with agency {agency} variable: {var}")
        logger.info(f"Done with agency {agency} for all variables")
        doneagency.append(agency)
    logger.info("Completed population for these agencies: ")
    for agent in doneagency:
        logger.info(agent)


def purge(dest):
    if SAFEGUARD:
        raise NotImplementedError("populate repo functions not ready to use")

    if purge:
        for pat in ["*.csv", "*.rdb"]:
            allfiles = glob.glob(os.path.join(dest, pat))
            for fname in allfiles:
                os.remove(fname)


def populate_ncro_realtime(dest, realtime_start=pd.Timestamp(2021, 1, 1)):
    if SAFEGUARD:
        raise NotImplementedError("populate repo functions not ready to use")

    # NCRO QAQC
    # dest = "//cnrastore-bdo/Modeling_Data/continuous_station_repo/raw/incoming/dwr_ncro"
    # ncro_download_por(dest)

    # NCRO recent from CDEC
    end = None
    ncrodf = list_ncro_stations(dest)
    populate_repo2(ncrodf, dest, realtime_start, overwrite=True)


def rationalize_time_partitions(pat):
    if SAFEGUARD:
        raise NotImplementedError("populate repo functions not ready to use")

    allpaths = glob.glob(pat)
    repodir = os.path.split(allpaths[0])[0]
    allfiles = [os.path.split(x)[1] for x in allpaths]
    allmeta = []
    already_checked = set()
    for fname in allfiles:
        fname_meta = interpret_fname(fname)
        allmeta.append(fname_meta)
    for meta in allmeta:
        if meta["filename"] in already_checked:
            continue
        near_misses = []
        for meta2 in allmeta:
            if meta == meta2:
                continue
            same_series = (
                (meta["agency"] == meta2["agency"])
                and (meta["param"] == meta2["param"])
                and (
                    meta["station_id"] == meta2["station_id"]
                    and meta["subloc"] == meta2["subloc"]
                )
            )
            if same_series:
                already_checked.add(meta2["filename"])
                near_misses.append(meta2)

        already_checked.add(meta["filename"])
        if len(near_misses) > 0:
            near_misses.append(meta)
            # logger.info(f"Main series: {meta['filename']}")
            superseded = []
            for i, meta in enumerate(near_misses):
                # logger.info(meta)
                issuperseded = False

                superseding = []
                for meta2 in near_misses:
                    if meta == meta2:
                        continue
                    superseded_thisfile = (
                        meta2["syear"] <= meta["syear"]
                        and meta2["eyear"] >= meta["eyear"]
                    )
                    issuperseded |= superseded_thisfile
                    if superseded_thisfile:
                        superseding.append(
                            meta2
                        )  # this file is a superset of the one being checked
                if issuperseded:
                    fnamesuper = meta["filename"]
                    logger.info(f"superseded: {fnamesuper} superseded by:")
                    for sf in superseding:
                        info_fname = sf['filename']
                        logger.info(f"  {info_fname}")
                    os.remove(os.path.join(repodir, fnamesuper))
                    superseded.append(fnamesuper)

        else:
            logger.info(f"Main series: {meta['filename']} had no similar file names")
    logger.info("Superseded files:")
    for sup in superseded:
        logger.info(sup)


def populate_ncro_repo(dest, variables):
    download_ncro_por(dest, variables)  # period of record for NCRO QA QC'd
    populate_ncro_realtime(dest)  # Recent NCRO


def ncro_only(dest):
    populate_ncro_repo(dest, variables)
    revise_filename_syear_eyear(os.path.join(dest, f"ncro_*.csv"))
    revise_filename_syear_eyear(os.path.join(dest, f"cdec_*.csv"))


def populate_main(dest, agencies=None, varlist=None, partial_update=False):
    if SAFEGUARD:
        raise NotImplementedError("populate repo functions not ready to use")

    do_purge = False
    if not os.path.exists(dest):
        os.mkdir(dest)
        logger.info(f"Directory {dest} created")
    else:
        if do_purge:
            purge(dest)

    failures = []
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
            #if (agency not in ["dwr_ncro", "ncro"])
        }
        #if do_ncro:
        #    future_to_agency[executor.submit(populate_ncro_repo, dest,varlist)] = "ncro"

    for future in concurrent.futures.as_completed(future_to_agency):
        agency = future_to_agency[future]
        try:
            data = future.result()
        except Exception as exc:
            failures.append(agency)
            trace = traceback.format_exc()
            logger.info(f"{agency} generated an exception: {exc} with trace:\n{trace}")
        # This requires that CDEC already be done, though it coudl be split by variable 
        # with some work
        if "ncro" in agency:
            populate_ncro_realtime(dest)

    # A fixup mostly for DES, addresses overlapping years of  same variable
    if do_des:
        rationalize_time_partitions(os.path.join(dest, "des*"))

    if do_ncro:
        revise_filename_syear_eyear(os.path.join(dest, f"ncro_*.csv"))
    revise_filename_syear_eyear(os.path.join(dest, f"cdec_*.csv"))
    logger.info("These agency queries failed")


def populate_debug_ncro_rename(dest, agencies=None, varlist=None):
    do_purge = False
    if not os.path.exists(dest):
        os.mkdir(dest)
        logger.info(f"Directory {dest} created")
    else:
        if do_purge:
            purge(dest)

    failures = []
    if agencies is None or len(agencies) == 0:
        all_agencies = ["usgs", "dwr_des", "usbr", "noaa", "dwr_ncro", "dwr"]
    else:
        all_agencies = agencies
    do_ncro = ("ncro" in all_agencies) or ("dwr_ncro" in all_agencies)
    do_des = ("des" in all_agencies) or ("dwr_des" in all_agencies)
    if do_ncro:
        revise_filename_syear_eyear(os.path.join(dest, f"ncro_*.csv"))
    revise_filename_syear_eyear(os.path.join(dest, f"cdec_*.csv"))
    logger.info("These agency queries failed")


@click.command()
@click.option(
    '--dest',
    required=True,
    help='Directory where files will be stored.',
)
@click.option(
    '--agencies',
    multiple=True,
    default=None,
    help='Agencies to download. If none, a default list is used',
)
@click.option(
    '--variables',
    multiple=True,
    default=None,
    help='Variables to download. If none, a default list is used',
)
@click.option(
    '--partial',
    is_flag=True,
    default=False,
    help='Partial update assuming existing files and only updating from 2020 onwards',
)
def populate_main_cli(dest, agencies, variables, partial):
    """Populate repository with data from various agencies."""
    if SAFEGUARD:
        return

    varlist = list(variables) if variables else None
    agencies_list = list(agencies) if agencies else None
    logger.info(f"dest: {dest}, agencies: {agencies_list}, varlist:{varlist}")
    populate_main(dest, agencies_list, varlist=varlist, partial_update=partial)


if __name__ == "__main__":
    populate_main_cli()


# Additional: make sure we have woodbridge, yby,
