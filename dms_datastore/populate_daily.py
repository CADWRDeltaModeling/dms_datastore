#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Populate daily data from a manifest.

This command downloads daily data (currently from CDEC and USGS)
into a staging/raw directory. It does not perform reformatting.

Workflow:
    populate_daily -> raw staging -> dropbox_data -> daily/formatted
"""

import os
import click
import pandas as pd
import logging
from pathlib import Path

from dms_datastore.download_nwis import nwis_download
from dms_datastore.download_cdec import cdec_download
from dms_datastore.process_station_variable import (
    normalize_station_request,
    attach_agency_id,
    attach_src_var_id,
)
from dms_datastore import dstore_config
from dms_datastore.logging_config import configure_logging, resolve_loglevel
from dms_datastore.populate_repo import revise_filename_syear_eyear

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = ["station_id", "param", "provider"]
SUPPORTED_PROVIDERS = {"cdec", "usgs"}


# ---------------------------------------------------------------------
# Manifest handling
# ---------------------------------------------------------------------
def read_daily_manifest(manifest):
    """
    Read and validate manifest.

    Parameters
    ----------
    manifest : str
        Path to manifest CSV

    Returns
    -------
    DataFrame
    """
    df = pd.read_csv(manifest, comment="#")

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Manifest missing required columns: {missing}")

    # enforce strict typing
    df = df[REQUIRED_COLUMNS].copy()
    df["station_id"] = df["station_id"].astype(str).str.strip()
    df["param"] = df["param"].astype(str).str.strip()
    df["provider"] = df["provider"].astype(str).str.strip().str.lower()

    # provider validation
    bad = df.loc[~df["provider"].isin(SUPPORTED_PROVIDERS)]
    if not bad.empty:
        raise ValueError(f"Unsupported providers in manifest:\n{bad}")

    # duplicate detection (strict)
    dup = df.duplicated()
    if dup.any():
        raise ValueError(f"Duplicate rows found in manifest:\n{df[dup]}")

    return df


# ---------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------
def _dispatch_provider(df, provider, dest, start):
    """
    Dispatch a provider group to the appropriate downloader.

    Parameters
    ----------
    df : DataFrame
        Subset of manifest for one provider
    provider : str
        'cdec' or 'usgs'
    dest : str
        Destination directory
    start : Timestamp
        Start date
    """
    vlookup = dstore_config.config_file("variable_mappings")

    # normalize station request
    stationlist = normalize_station_request(
        stationframe=df,
        default_subloc="default",
    )

    # attach agency_id from registry
    agency_id_col = "cdec_id" if provider == "cdec" else "agency_id"
    stationlist = attach_agency_id(
        stationlist,
        repo_name="formatted",
        agency_id_col=agency_id_col,
    )

    # attach src_var_id
    stationlist = attach_src_var_id(stationlist, vlookup, source=provider)

    end = None  # daily always goes to "now"

    if provider == "cdec":
        logger.info(f"Dispatching {len(df)} rows to CDEC daily downloader (freq='D')")
        cdec_download(
            stationlist,
            dest,
            start,
            end,
            overwrite=False,
            freq="D",
        )

    elif provider == "usgs":
        logger.info(f"Dispatching {len(df)} rows to USGS daily downloader (--daily)")
        nwis_download(
            stationlist,
            dest,
            start,
            end,
            param=None,
            overwrite=False,
            daily=True,
        )


# ---------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------
def populate_daily(manifest, dest, start):
    """
    Populate daily raw data from manifest.

    Parameters
    ----------
    manifest : str
        Path to manifest CSV
    dest : str
        Destination directory
    start : Timestamp
        Start date
    """
    if not os.path.exists(dest):
        raise ValueError(f"Destination directory does not exist: {dest}")

    df = read_daily_manifest(manifest)

    logger.info(f"Loaded manifest with {len(df)} rows")

    # group by provider
    for provider, group in df.groupby("provider"):
        _dispatch_provider(group, provider, dest, start)

    # clip filenames to actual data extent
    logger.info("Revising filenames based on actual data extent")
    revise_filename_syear_eyear(os.path.join(dest, "*.csv"))

    logger.info("populate_daily complete")


# ---------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------
@click.command()
@click.argument("manifest")
@click.option("--dest", required=True, help="Directory where files will be stored.")
@click.option("--start", default="1980-01-01", help="Start date (YYYY-MM-DD)")
@click.option("--logdir", type=click.Path(path_type=Path), default="logs")
@click.option("--debug", is_flag=True)
@click.option("--quiet", is_flag=True)
@click.help_option("-h", "--help")
def populate_daily_cli(manifest, dest, start, logdir, debug, quiet):
    """Populate daily raw data from manifest."""

    level, console = resolve_loglevel(debug=debug, quiet=quiet)

    configure_logging(
        package_name="dms_datastore",
        level=level,
        console=console,
        logdir=logdir,
        logfile_prefix="populate_daily",
    )

    start_ts = pd.to_datetime(start)

    populate_daily(manifest, dest, start_ts)


if __name__ == "__main__":
    populate_daily_cli()