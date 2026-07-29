#!/usr/bin/env python
"""Correct and clean CCF (Clifton Court Forebay) upstream/downstream water levels.

Reads the formatted upstream (``up``) and downstream (``down``) CCF
elevation products from the ``proprietary_formatted`` repo (station
``clc``), applies a datum/instrument offset correction, masks
out-of-range values, and interpolates short gaps.

The output is written as DMS-format CSVs via :func:`write_ts_csv`.
"""
import logging
from pathlib import Path

import click
import pandas as pd

from dms_datastore.read_multi import read_ts_repo
from dms_datastore.write_ts import write_ts_csv
from dms_datastore.logging_config import configure_logging, resolve_loglevel

logger = logging.getLogger(__name__)

MIN_DATE = pd.Timestamp(2020, 1, 1)


def ccf_waterlevel_correction(formatted_ccf_ele_up, formatted_ccf_ele_down):
    """Apply datum and instrument offset corrections to CCF water level data.

    Parameters
    ----------
    formatted_ccf_ele_up : pandas.DataFrame or pandas.Series
        Formatted upstream (``up``) CCF elevation series, indexed by
        datetime, prior to datum/instrument correction.
    formatted_ccf_ele_down : pandas.DataFrame or pandas.Series
        Formatted downstream (``down``) CCF elevation series, indexed by
        datetime, prior to datum/instrument correction.

    Returns
    -------
    tuple of (pandas.DataFrame or pandas.Series, pandas.DataFrame or pandas.Series)
        A 2-tuple ``(corrected_ccf_ele_up, corrected_ccf_ele_down)`` of the
        datum/instrument-corrected upstream and downstream elevation
        series, indexed by datetime.
    """
    corrected_ccf_ele_up = formatted_ccf_ele_up + 2.57 - 0.27
    corrcted_ccf_ele_down = formatted_ccf_ele_down + 2.57 + 0.22

    return corrected_ccf_ele_up, corrcted_ccf_ele_down


def remove_outliers(ccf_ele_up, ccf_ele_down):
    """Mask out-of-range values and interpolate short gaps in CCF elevation data.

    Values outside the plausible range are masked (``up``: -0.5 to 8.5 ft;
    ``down``: 0 to 4.5 ft) and short gaps (up to 5 steps) are closed by
    interpolation.

    Parameters
    ----------
    ccf_ele_up : pandas.DataFrame or pandas.Series
        Upstream (``up``) CCF elevation series, indexed by datetime, after
        datum/instrument correction.
    ccf_ele_down : pandas.DataFrame or pandas.Series
        Downstream (``down``) CCF elevation series, indexed by datetime,
        after datum/instrument correction.

    Returns
    -------
    tuple of (pandas.DataFrame or pandas.Series, pandas.DataFrame or pandas.Series)
        A 2-tuple ``(ccf_ele_up, ccf_ele_down)`` of the outlier-masked,
        gap-filled upstream and downstream elevation series, indexed by
        datetime (index named ``"datetime"``).
    """

    ccf_ele_up = ccf_ele_up.mask((ccf_ele_up > 8.5) | (ccf_ele_up < -0.5))
    ccf_ele_down = ccf_ele_down.mask((ccf_ele_down > 4.5) | (ccf_ele_down < 0))
    

    ccf_ele_up = ccf_ele_up.interpolate(limit=5)
    ccf_ele_down = ccf_ele_down.interpolate(limit=5)
    
    ccf_ele_up.index.name = "datetime"
    ccf_ele_down.index.name = "datetime"

    return ccf_ele_up, ccf_ele_down


def process_ccfb_waterlevel(start=None, end=None):
    """Read, correct, and clean the CCF upstream/downstream elevation products.

    Parameters
    ----------
    start : pandas.Timestamp or None, optional
        Optional inclusive start time for the data read.
    end : pandas.Timestamp or None, optional
        Optional inclusive end time for the data read.

    Returns
    -------
    tuple
        ``(ccf_ele_up, ccf_ele_down, metadata_up, metadata_down)`` where
        the first two elements are the corrected, outlier-masked,
        gap-filled upstream and downstream elevation series, and the
        latter two are the metadata dicts carried forward from the
        respective formatted source files.
    """
    meta_up, formatted_ccf_ele_up = read_ts_repo(
        "clc", "elev", subloc="up", repo="proprietary_formatted",
        start=start, end=end, meta=True,
    )
    meta_down, formatted_ccf_ele_down = read_ts_repo(
        "clc", "elev", subloc="down", repo="proprietary_formatted",
        start=start, end=end, meta=True,
    )

    corrected_ccf_ele_up, corrected_ccf_ele_down = ccf_waterlevel_correction(
        formatted_ccf_ele_up, formatted_ccf_ele_down
    )
    ccf_ele_up, ccf_ele_down = remove_outliers(corrected_ccf_ele_up, corrected_ccf_ele_down)

    metadata_up = dict(meta_up[0]) if meta_up else {}
    metadata_down = dict(meta_down[0]) if meta_down else {}
    return ccf_ele_up, ccf_ele_down, metadata_up, metadata_down


@click.command("process_ccfb_waterlevel")
@click.option("--up-outfile", type=click.Path(path_type=Path),
              default="ccf_ele_up.csv", show_default=True,
              help="Output CSV path for the processed upstream CCF elevation product.")
@click.option("--down-outfile", type=click.Path(path_type=Path),
              default="ccf_ele_down.csv", show_default=True,
              help="Output CSV path for the processed downstream CCF elevation product.")
@click.option("--start", type=str, default="2020-01-01", show_default=True,
              help="Inclusive start time.")
@click.option("--end", type=str, default="2026-01-01", show_default=True,
              help="Inclusive end time.")
@click.option("--logdir", type=click.Path(path_type=Path), default="logs",
              help="Directory for log files.")
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.option("--quiet", is_flag=True, help="Suppress console output.")
@click.help_option("-h", "--help")
def process_ccfb_waterlevel_cli(up_outfile, down_outfile, start, end, logdir, debug, quiet):
    """Correct and clean CCF upstream/downstream water levels and write processed CSVs.

    Reads the formatted upstream/downstream elevation products from the
    ``proprietary_formatted`` repo, applies datum and instrument offset
    corrections, masks outliers, and writes ``datetime,value`` DMS-format
    CSVs (feet).
    """
    level, console = resolve_loglevel(debug=debug, quiet=quiet)
    configure_logging(
        package_name="dms_datastore",
        level=level,
        console=console,
        logdir=logdir,
        logfile_prefix="process_ccfb_waterlevel",
    )
    sdate = pd.Timestamp(start)
    edate = pd.Timestamp(end)
    if sdate < MIN_DATE:
        raise ValueError(f"Start date is before the minimum allowed date {MIN_DATE.date()}.")

    ccf_ele_up, ccf_ele_down, metadata_up, metadata_down = process_ccfb_waterlevel(
        start=sdate, end=edate
    )
    logger.info("Processed %d rows for ccf_ele_up; last time %s",
                len(ccf_ele_up), ccf_ele_up.last_valid_index())
    logger.info("Processed %d rows for ccf_ele_down; last time %s",
                len(ccf_ele_down), ccf_ele_down.last_valid_index())
    write_ts_csv(ccf_ele_up, str(up_outfile), metadata=metadata_up)
    write_ts_csv(ccf_ele_down, str(down_outfile), metadata=metadata_down)
    logger.info("Wrote %s", up_outfile)
    logger.info("Wrote %s", down_outfile)


if __name__ == "__main__":
    process_ccfb_waterlevel_cli()