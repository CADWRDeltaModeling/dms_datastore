"""
Raw download and staging utilities for external data providers.

This module orchestrates acquisition of raw time series files from multiple
providers and writes them into a staging/raw directory using a raw naming
convention. It does not apply repository merge or provider-resolution policy;
those steps occur later in formatting and repo update workflows.

Overview
--------
The module supports two closely related workflows.

1. General population of raw data
   - ``populate_main()`` is the top-level orchestration entry point.
   - ``populate()`` loops over agencies and variables.
   - ``populate_repo()`` prepares a station request for one agency/parameter
     pair and dispatches to the appropriate downloader.

2. NCRO realtime supplementation
   - ``list_ncro_stations()`` scans existing NCRO files in a destination
     directory and extracts the logical station/parameter pairs already present.
   - ``supplement_ncro_with_cdec()`` uses that list to request corresponding CDEC data,
     intended to supplement NCRO's non-realtime coverage with a realtime feed.
   - ``populate_ncro_realtime()`` is a convenience wrapper around this path.

Raw naming
----------
Files handled by this module use a raw/downloader naming profile rather than
repo-configured naming semantics. The profile is used only for parsing and
renaming downloader outputs:

- ``{agency}_{station_id@subloc}_{agency_id}_{param}_{syear}_{eyear}.csv``
- ``{agency}_{station_id@subloc}_{agency_id}_{param}_{year}.csv``

This naming is represented by ``RAW_NAMING`` and is intentionally separate from
repo templates used by formatted/screened/processed repositories.

Download sequence
-----------------
A typical end-to-end raw population sequence is:

1. Build a station request from configured registries and variable mappings.
2. Download raw files with the provider-specific downloader.
3. Revise filename year fields to match the actual years present in the data.
4. For DES, rationalize overlapping or inconsistent time partitions.
5. For NCRO, optionally supplement with corresponding CDEC realtime files.

The NCRO supplementation step depends on the existing NCRO files already present
in the destination directory, because those files are used to determine which
station/parameter combinations should be mirrored from CDEC.

Station request preparation
---------------------------
Before calling a downloader, the module prepares a station-request DataFrame
using helper functions such as:

- ``normalize_station_request()``
- ``attach_agency_id()``
- ``attach_src_var_id()``

The resulting request typically contains station identity, sublocation,
parameter, agency/provider-specific identifier, and source-variable code needed
by the target downloader.

Functions
---------
populate_main(dest, agencies=None, varlist=None, partial_update=False)
    Main orchestration entry point. Runs downloads, handles failures, triggers
    NCRO realtime supplementation when applicable, and performs selected
    post-download cleanup steps.

populate(dest, all_agencies=None, varlist=None, partial_update=False)
    Agency/variable loop used by ``populate_main()``.

populate_repo(agency, param, dest, start, end, overwrite=False, ...)
    Prepare a request for a single agency and parameter and invoke the matching
    downloader.

list_ncro_stations(dest)
    Inspect existing ``ncro_*.csv`` files in a destination directory and return
    a DataFrame describing the station/parameter combinations present.

supplement_ncro_with_cdec(df, dest, start, overwrite=False, ...)
    Request CDEC data corresponding to the station/parameter combinations
    identified from NCRO files.

populate_ncro_realtime(dest, realtime_start=...)
    Convenience wrapper that derives the NCRO station list from ``dest`` and
    passes it to ``supplement_ncro_with_cdec()``.

revise_filename_syears(...)
revise_filename_syear_eyear(...)
    Adjust year fields in raw filenames to reflect the actual valid-data span.

Notes
-----
This module operates at the raw-file staging layer. It is expected that later
steps such as reformatting, screening, and repo reconciliation will impose the
stricter repository semantics defined elsewhere in the package.

Because this module sits between external downloaders and downstream repo logic,
identifier handling is especially important. In particular, the NCRO-to-CDEC
supplementation path is sensitive to how station identifiers and provider-
specific IDs are prepared before download.
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

    if "ncro" in agency:
        vlookup = mapping_df
        agency = "ncro"
    else:
        vlookup = dstore_config.config_file("variable_mappings")

    # Use repo-aware registry access instead of reading station_dbase CSV directly.
    slookup = dstore_config.repo_registry("formatted").copy()

    filter_agency = "dwr_ncro" if agency == "ncro" else agency
    slookup = slookup.loc[slookup.agency.str.lower() == filter_agency, :]
    name_lookup = (
        slookup.loc[:, ["station_id", "name"]]
        .drop_duplicates(subset=["station_id"])
    )


    dfsub = read_station_subloc(dstore_config.config_file("sublocations"))
    slookup = merge_station_subloc(slookup, dfsub, default_z=-0.5)

    if ignore_existing is not None:
        slookup = slookup[~slookup["station_id"].isin(ignore_existing)]

    dest_dir = dest
    source = "cdec" if agency in ["dwr", "usbr"] else agency
    agency_id_col = "agency_id"
    src_site_id_col = "cdec_id" if source == "cdec" else None

    # Preserve only the station display name, and only outside the standard
    # request-building pipeline.
    slookup = slookup.reset_index()
    df_req = slookup.loc[:, ["station_id", "subloc"]]

    stationlist = normalize_station_request(
        stationframe=df_req,
        param=param,
        default_subloc="default",
    )
    stationlist = attach_agency_id(
        stationlist,
        repo_name="formatted",
        agency_id_col=agency_id_col,
        src_site_id_col=src_site_id_col,
        on_missing="drop" if src_site_id_col is not None else "raise",
    )
    stationlist = attach_src_var_id(stationlist, vlookup, source=source)

    stationlist = stationlist.merge(
        name_lookup,
        on="station_id",
        how="left",
        validate="many_to_one",
    )

    if stationlist["name"].isna().any():
        missing = stationlist.loc[stationlist["name"].isna(), "station_id"].tolist()
        raise ValueError(f"Missing station name for station_id(s): {missing}")

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

    df = pd.DataFrame(
        data=stationlist, columns=["station_id", "param", "agency", "agency_id_from_file"]
    )
    df = df.drop_duplicates(subset=["station_id", "param"])
    return df


def supplement_ncro_with_cdec(df, dest, start, overwrite=False, ignore_existing=None):
    """Currently used by ncro realtime."""
    vlookup = dstore_config.config_file("variable_mappings")
    df["station_id"] = df["station_id"].str.replace("'", "")
    df["subloc"] = "default"

    if ignore_existing is not None:
        df = df[~df["station_id"].isin(ignore_existing)]

    source = "cdec"
    agency_id_col = "agency_id"
    src_site_id_col = "cdec_id"

    stationlist = normalize_station_request(
        stationframe=df,
        default_subloc="default",
    )

    stationlist = attach_agency_id(
        stationlist,
        repo_name="formatted",
        agency_id_col=agency_id_col,
        src_site_id_col=src_site_id_col,
        on_missing="drop",
    )

    if stationlist.empty:
        logger.warning(
            "No NCRO stations remain for CDEC supplementation after dropping rows "
            "with missing %s",
            agency_id_col,
        )
        return

    stationlist = attach_src_var_id(stationlist, vlookup, source=source)
    end = None
    logger.debug("NCRO CDEC supplementation request:\n%s", stationlist)
    downloaders["cdec"](stationlist, dest, start, end, overwrite)


def populate(dest, all_agencies=None, varlist=None, partial_update=False):
    logger.info(f"dest: {dest} agencies: {all_agencies}")
    doneagency = []

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
                revise_filename_syear_eyear(os.path.join(dest, f"{agency}*_{var}_*.{ext}"))
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
                end_download = (
                    pd.Timestamp(2039, 12, 31, 23, 59)
                    if ((agency == "noaa") and (var == "predictions"))
                    else None
                )

                populate_repo(
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



def populate_ncro_realtime(dest, realtime_start=pd.Timestamp(2021, 1, 1)):
    end = None
    ncrodf = list_ncro_stations(dest)
    supplement_ncro_with_cdec(ncrodf, dest, realtime_start, overwrite=True)



def populate_ncro_repo(dest, variables):
    download_ncro_por(dest, variables)
    populate_ncro_realtime(dest)


def ncro_only(dest):
    populate_ncro_repo(dest, variables)
    revise_filename_syear_eyear(os.path.join(dest, f"ncro_*.csv"))
    revise_filename_syear_eyear(os.path.join(dest, f"cdec_*.csv"))


def populate_main(dest, agencies=None, varlist=None, partial_update=False):
    do_purge = False
    if not os.path.exists(dest):
        raise ValueError(f"Destination directory {os.path.abspath(dest)} does not exist. Please create it before running populate.")
    else:
        if do_purge:
            purge(dest)

    failures = []
    if agencies is None or len(agencies) == 0:
        all_agencies = ["usgs", "dwr_des", "usbr", "noaa", "dwr_ncro", "dwr"]
    else:
        all_agencies = agencies
    
    # Normalize agency names: convert "ncro" to "dwr_ncro" and "des" to "dwr_des"
    all_agencies = ["dwr_ncro" if ag == "ncro" else "dwr_des" if ag == "des" else ag for ag in all_agencies]
    
    do_ncro = "dwr_ncro" in all_agencies
    do_des = "dwr_des" in all_agencies

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        future_to_agency = {
            executor.submit(populate, dest, agency, varlist, partial_update): agency
            for agency in all_agencies
        }

    for future in concurrent.futures.as_completed(future_to_agency):
        agency = future_to_agency[future]
        try:
            future.result()
        except Exception as exc:
            failures.append(agency)
            trace = traceback.format_exc()
            logger.info(f"{agency} generated an exception: {exc} with trace:\n{trace}")
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
    # Normalize agency names: convert "ncro" to "dwr_ncro" and "des" to "dwr_des"
    all_agencies = ["dwr_ncro" if ag == "ncro" else "dwr_des" if ag == "des" else ag for ag in all_agencies]
    do_ncro = "dwr_ncro" in all_agencies
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
@click.help_option("-h", "--help")
def populate_main_cli(dest, agencies, variables, partial, logdir="logs", debug=False, quiet=False):
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
    populate_main(dest, agencies_list, varlist=varlist, partial_update=partial)


if __name__ == "__main__":
    populate_main_cli()
