import logging
import os
import os.path as osp
from pathlib import Path

import click
import pandas as pd

import matplotlib.pyplot as plt
from vtools import *
from dms_datastore.read_ts import *
from vtools.functions.unit_conversions import ec_psu_25c, CFS2CMS, CMS2CFS
from vtools.functions.error_detect import *
import yaml
from dms_datastore.read_multi import read_ts_repo
from dms_datastore.write_ts import write_ts_csv
from dms_datastore.logging_config import configure_logging, resolve_loglevel

logger = logging.getLogger(__name__)

lisbon_elev_top = 11.5
lisbon_flow_top = 4000.0


def process_yolo_cache_slough():
    """Compute the southern (Cache Slough minus Miner) estimate of Yolo Bypass flow.

    Cache Slough flow at Ryer Island (station ``rye``) is interpolated,
    low-pass filtered (``cosine_lanczos``, 40-hour cutoff), and cubically
    interpolated across remaining gaps. Because the Ryer Island gauge
    measures flow below the confluence with Miner Slough, flow at Miner
    (station ``mir``, similarly interpolated and filtered) is subtracted to
    yield the flow heading south out of the Yolo Bypass/Cache Complex. The
    result is further smoothed with a 4-day low-pass filter.

    This does not account for change in storage in the Cache Complex or
    for the difference between Toe Drain and Bypass flow, so it should be
    treated as an approximate southern estimate of total bypass flow.

    Notes
    -----
    Uses the module-level ``sdate`` and ``edate`` variables (set in the
    ``__main__`` block) to bound the data read from the repository.

    Returns
    -------
    pandas.DataFrame or pandas.Series
        The 4-day low-pass-filtered southern estimate of Yolo Bypass flow,
        indexed by datetime.
    """

    # RYE station is the newer station
    cache_ryer = read_ts_repo(station_id="rye", variable="flow", start=sdate, end=edate)
    cache_ryer = cache_ryer.interpolate(limit=60)
    cache_ryer = cosine_lanczos(cache_ryer, hours(40))
    cache_ryer.columns = ["value"]

    cache_interp = cache_ryer.interpolate(method="cubic")
    # The flow at RYE/RYE is measured below Miner,
    # so we have to subtract Miner to get the flow
    # out of Yolo to the south
    miner = read_ts_repo(station_id="mir", variable="flow", start=sdate, end=edate)
    miner = miner.interpolate(limit=60)
    miner = cosine_lanczos(miner, hours(40))
    yolo_south = cache_interp.sub(miner.squeeze(), axis=0)
    yolo_south_4d = cosine_lanczos(yolo_south, days(4))

    return yolo_south_4d


def est_yolo_woodland_sacweir(sdate, edate):
    """Create the northern (first-priority) estimate of total Yolo Bypass flow.

    Computed as the sum of flow at Woodland (station ``yby``, linearly
    interpolated) and flow over the Sacramento Weir. Sacramento Weir flow
    is currently hardwired to zero (placeholder pending a real data
    source) and is smoothed with ``rhistinterp`` before being reindexed
    and reconciled to the Woodland flow index.

    This does not account for the Bypass vs. Toe Drain difference.

    If this sum is low (i.e. Sac Weir flow is zero and Woodland flow is
    low), it may indicate that more flow is carried by the Toe Drain, in
    which case this estimate MAY be usable as an estimate of Toe Drain
    flow.

    If this sum is high (Woodland high, possibly with Sac Weir flow), then
    this becomes an estimate of the sum of the two Yolo flows (bypass and
    toe), and the ultimate estimate will probably be obtained by
    subtracting a Toe Drain estimate.

    Parameters
    ----------
    sdate : pandas.Timestamp
        Start of the period over which Woodland and Sacramento Weir flow
        are read and combined.
    edate : pandas.Timestamp
        End of the period over which Woodland and Sacramento Weir flow are
        read and combined.

    Returns
    -------
    pandas.DataFrame
        Sum of Woodland flow and Sacramento Weir flow, indexed by
        datetime, with column ``value``.
    """
    interval = minutes(15)

    woodland_flow = read_ts_repo(
        station_id="yby", variable="flow", start=sdate, end=edate
    )
    woodland_flow = woodland_flow.interpolate(method="linear", limit=1200)
    woodland_flow.columns = ["value"]

    sac_weir_flow = pd.DataFrame(
        0, index=woodland_flow.index, columns=["value"]
    ).to_period()  # todo: temporarily use 0 for sac weir
    # sac_weir_flow = read_ts(osp.join(input_dir,config['yolo']['data_sources']['sac_weir_flow']), start=sdate, end=edate).interpolate().to_period()
    sac_weir_flow = (
        rhistinterp(sac_weir_flow + 100.0, interval, lowbound=0.0, p=12.0) - 100.0
    )
    sac_weir_flow.columns = ["value"]

    sac_weir_flow = sac_weir_flow.reindex(woodland_flow.index)
    sac_weir_flow = sac_weir_flow.fillna(0.0)
    return woodland_flow + sac_weir_flow


def get_lisbon(sdate, edate):
    """Read and lightly gap-fill flow and elevation at the Lisbon station.

    Flow (station ``lis``, variable ``flow``) is interpolated with a
    20-step limit, clipped to ``sdate``:``edate``, resampled to 15-minute
    intervals, and interpolated again with a 3-step limit. Elevation
    (station ``lis``, variable ``elev``) is interpolated with a 50-step
    limit; this generous limit is acceptable because large gaps mainly
    occur when Lisbon Weir is tidal, which does not affect the later test
    of whether elevation exceeds ``lisbon_elev_top``.

    Parameters
    ----------
    sdate : pandas.Timestamp
        Start of the period over which Lisbon flow and elevation are read.
    edate : pandas.Timestamp
        End of the period over which Lisbon flow and elevation are read.

    Returns
    -------
    tuple of (pandas.DataFrame, pandas.DataFrame)
        A 2-tuple ``(lisbon_flow, lisbon_elev)`` of the gap-filled Lisbon
        flow and elevation series, indexed by datetime.
    """
    # use read_ts_repo
    lisbon_flow = read_ts_repo(
        station_id="lis", variable="flow", start=sdate, end=edate
    )

    lisbon_flow = lisbon_flow.interpolate(limit=20)[sdate:edate]  # todo: hardwired
    lisbon_flow = lisbon_flow.resample("15min").interpolate(limit=3)

    # The filling limit on lisbon_elev is far beyond what would be reasonble for
    # a tidal quantity for accuracy, but the times when it is too much are times when
    # lisbon weir is tidal and that will not affect the test of whether it is > 11.6

    lisbon_elev = read_ts_repo(
        station_id="lis", variable="elev", start=sdate, end=edate
    )
    lisbon_elev = lisbon_elev.interpolate(limit=50)

    return (lisbon_flow, lisbon_elev)


def fill_lisbon_flow(lisbon_flow_unfilled, sdate, edate):
    """Fill gaps in Lisbon flow using a simple offset relationship with Toe Drain flow.

    Missing values in ``lisbon_flow_unfilled`` are replaced with flow at
    the Toe Drain station (``lbtoe``) offset by -200 (a simple assumed
    relationship), and any remaining gaps are closed with interpolation
    (limit of 20 steps). A warning is printed if missing values remain
    afterward.

    Parameters
    ----------
    lisbon_flow_unfilled : pandas.DataFrame or pandas.Series
        Lisbon flow series, indexed by datetime, potentially containing
        gaps (``NaN`` values) to be filled.
    sdate : pandas.Timestamp
        Start of the period over which the Toe Drain (``lbtoe``) flow used
        for filling is read.
    edate : pandas.Timestamp
        End of the period over which the Toe Drain (``lbtoe``) flow used
        for filling is read.

    Returns
    -------
    pandas.DataFrame or pandas.Series
        The Lisbon flow series with gaps filled using Toe Drain flow and
        interpolation, in the same shape/index as ``lisbon_flow_unfilled``.
    """
    lbtoe_flow = read_ts_repo(
        station_id="lbtoe", variable="flow", start=sdate, end=edate
    )

    lisbon_flow_unfilled[lisbon_flow_unfilled.isnull()] = (
        lbtoe_flow[lisbon_flow_unfilled.isnull()] - 200
    )  # todo: simple relationship
    lisbon_flow_filled = lisbon_flow_unfilled.interpolate(limit=20)
    if np.sum(lisbon_flow_filled.isna().values) > 0:
        print(
            "Warning: there are still {} missing values in the Lisbon flow data after calling function fill_lisbon_flow.".format(
                np.sum(lisbon_flow_filled.isna().values)
            )
        )

    return lisbon_flow_filled


def fill_yolototal(yolo_total_raw):
    """Fill gaps in the total Yolo Bypass flow estimate.

    Missing values in ``yolo_total_raw`` (the northern Woodland + Sac Weir
    estimate) are first replaced with the southern Cache Slough-Miner
    estimate from :func:`process_yolo_cache_slough`, reindexed to match.
    Any remaining gaps are closed by linear interpolation.

    Parameters
    ----------
    yolo_total_raw : pandas.DataFrame or pandas.Series
        Northern (Woodland + Sac Weir) estimate of total Yolo Bypass flow,
        indexed by datetime, potentially containing gaps.

    Returns
    -------
    pandas.DataFrame or pandas.Series
        The total Yolo Bypass flow estimate with gaps filled using the
        southern estimate and interpolation, in the same shape/index as
        ``yolo_total_raw``.
    """
    yolo_total_filled = yolo_total_raw.copy()
    yolo_total_optional = process_yolo_cache_slough().reindex(yolo_total_raw.index)
    yolo_total_filled[yolo_total_raw.isnull()] = yolo_total_optional[
        yolo_total_raw.isnull()
    ]
    yolo_total_filled = yolo_total_filled.interpolate()
    return yolo_total_filled


def adjust_yoloflow(yolo_flow_raw, low_flow):
    """Clip effective Yolo Bypass flow to remove negative values.

    Prints the count of values below -1.0 cfs in ``yolo_flow_raw`` before
    adjustment. Values at positions flagged by ``low_flow`` are forced to
    zero, and any remaining negative values are also set to zero.

    Parameters
    ----------
    yolo_flow_raw : pandas.DataFrame or pandas.Series
        Raw effective Yolo Bypass flow (e.g. total flow minus effective
        Toe Drain flow), which may contain small negative values due to
        the subtraction.
    low_flow : pandas.Series of bool
        Boolean mask, aligned with ``yolo_flow_raw``, indicating
        low-flow conditions (total flow at or below the bypass threshold)
        where Yolo Bypass flow should be forced to zero.

    Returns
    -------
    pandas.DataFrame or pandas.Series
        The adjusted Yolo Bypass flow with negative and low-flow values
        set to zero.
    """
    print(
        "Number of negative Yolo flow value before adjustment: {}".format(
            (yolo_flow_raw < -1.0).sum()
        )
    )
    yolo_flow = yolo_flow_raw.copy()
    yolo_flow[low_flow] = 0.0
    yolo_flow[yolo_flow < 0.0] = 0.0
    return yolo_flow


def process_yolo_effective_flow(toe_raw, lisbon_elev, sdate, edate):
    """Derive effective Toe Drain and Yolo Bypass flows from Lisbon flow/elevation.

    Determines whether the Yolo Bypass is hydraulically active (Lisbon
    elevation above ``lisbon_elev_top`` or Toe Drain flow above
    ``lisbon_flow_top``) and, when inactive, interpolates Toe Drain flow
    without a gap-size limit. It then computes a total Yolo Bypass flow
    estimate (via :func:`est_yolo_woodland_sacweir` and
    :func:`fill_yolototal`), clips effective Toe Drain flow to 4000 cfs,
    and apportions any total flow above 4000 cfs 5% to the Toe Drain and
    95% to the Bypass. Effective Yolo Bypass flow is then computed as the
    total minus effective Toe Drain flow and adjusted to remove negative
    values via :func:`adjust_yoloflow`. Diagnostic counts of negative
    values and remaining gaps are printed.

    Parameters
    ----------
    toe_raw : pandas.DataFrame
        Raw (gap-filled) Toe Drain/Lisbon flow series, indexed by
        datetime. Renamed internally to a single ``value`` column.
    lisbon_elev : pandas.DataFrame
        Lisbon elevation series, indexed by datetime, used to determine
        whether the Yolo Bypass is hydraulically active. Renamed
        internally to a single ``value`` column.
    sdate : pandas.Timestamp
        Start of the period used when computing the total Yolo Bypass
        flow estimate.
    edate : pandas.Timestamp
        End of the period used when computing the total Yolo Bypass flow
        estimate.

    Returns
    -------
    tuple of (pandas.Series, pandas.Series)
        A 2-tuple ``(toe_eff, yolo_eff)`` of the effective Toe Drain flow
        and effective Yolo Bypass flow series, indexed by datetime.
    """
    lisbon_elev.columns = ["value"]
    toe_raw.columns = ["value"]

    # full_yolo is the "final" data frame that will hold the effective toe drain and yolo flows
    # as well as some intermediate quantities
    yolo_data_all = toe_raw.copy()
    yolo_data_all.columns = ["toe"]

    is_yolo_active = (lisbon_elev > lisbon_elev_top) | (toe_raw > lisbon_flow_top)
    is_yolo_active = is_yolo_active.reindex(yolo_data_all.index)
    is_yolo_active.ffill(inplace=True)
    yolo_data_all["is_yolo_active"] = is_yolo_active

    # interpolate Toe drain without a limit in gap size and apply only in areas where
    # is_yolo_active is False.
    print("Interpolate Toe Drain without limit where is_yolo_active is not active")
    toeinterp = yolo_data_all.toe.interpolate()
    yolo_data_all.loc[~yolo_data_all.is_yolo_active, "toe"] = toeinterp.where(
        ~yolo_data_all.is_yolo_active
    )

    # Now add an estimate of yolo total flow, preferred estimate is Woodland + Sac Weir
    # Fill estimated gaps with the southern estimate based on Cache Slough - Miner
    yolo_total_raw = est_yolo_woodland_sacweir(sdate, edate).reindex(
        yolo_data_all.index
    )
    yolo_total_filled = fill_yolototal(yolo_total_raw)
    yolo_data_all["yolo_total"] = yolo_total_filled

    # This adjustment keeps the full_yolo interpretation correct, but values
    # that meet these criteria are not used in later computations
    yolo_data_all.loc[~yolo_data_all.is_yolo_active, "yolo_total"] = yolo_data_all.toe[
        ~yolo_data_all.is_yolo_active
    ]

    # adjust effective toe drain flow
    toe_eff = yolo_data_all.toe.clip(upper=4000.0)
    toe_eff[is_yolo_active.value & toe_eff.isnull()] = 4000.0
    toe_eff[~is_yolo_active.value & toe_eff.isnull()] = yolo_data_all.yolo_total[
        ~is_yolo_active.value & toe_eff.isnull()
    ]
    # Use yolo_total flow to determine whether high flow occurs or not
    full_low = yolo_data_all.yolo_total <= 4000.0
    full_high = ~full_low  # recipricol for convenience
    # adjust toe_eff when yolo_total is high
    toe_eff.mask(
        full_high, toe_eff + 0.05 * (yolo_data_all.yolo_total - toe_eff), inplace=True
    )
    yolo_data_all["toe_eff"] = toe_eff
    # compute effective yolo flow
    yolo_eff_raw = yolo_data_all.yolo_total - yolo_data_all.toe_eff
    yolo_eff = adjust_yoloflow(yolo_eff_raw, full_low)
    yolo_data_all["yolo_eff"] = yolo_eff
    print(
        "Number of negative Yolo flow value after adjustment: {}".format(
            (yolo_eff < -1.0).sum()
        )
    )
    print(
        "Number of gaps in Yolo flow data: {}".format(
            yolo_data_all["yolo_eff"].isnull().sum()
        )
    )
    print(
        "Number of gaps in Toe flow data: {}".format(
            yolo_data_all["toe_eff"].isnull().sum()
        )
    )
    return (yolo_data_all["toe_eff"], yolo_data_all["yolo_eff"])


def process_yolo(start, end):
    """Compute effective Toe Drain and Yolo Bypass flow estimates.

    Parameters
    ----------
    start : pandas.Timestamp
        Start of the period over which Lisbon flow/elevation and the Yolo
        Bypass total flow estimate are read and combined.
    end : pandas.Timestamp
        End of the period over which Lisbon flow/elevation and the Yolo
        Bypass total flow estimate are read and combined.

    Returns
    -------
    tuple of (pandas.Series, pandas.Series)
        A 2-tuple ``(toe_final, yolo_final)`` of the effective Toe Drain
        flow and effective Yolo Bypass flow series, indexed by datetime.
    """
    lisbon_flow_raw, lisbon_elev = get_lisbon(start, end)
    lisbon_flow = fill_lisbon_flow(lisbon_flow_raw, start, end)
    yolo_toe_raw = lisbon_flow.copy()

    toe_final, yolo_final = process_yolo_effective_flow(
        yolo_toe_raw, lisbon_elev, start, end
    )
    if toe_final.isnull().any() or yolo_final.isnull().any():
        raise ValueError(
            "There are missing values in the final Toe Drain or Yolo flow data."
        )
    return toe_final, yolo_final


@click.command("process_yolo")
@click.option("--yolo-outfile", type=click.Path(path_type=Path),
              default="yolo_flow.csv", show_default=True,
              help="Output CSV path for the processed Yolo Bypass flow product.")
@click.option("--ytoe-outfile", type=click.Path(path_type=Path),
              default="ytoe_flow.csv", show_default=True,
              help="Output CSV path for the processed effective Toe Drain flow product.")
@click.option("--start", type=str, default="2020-01-01", show_default=True,
              help="Inclusive start time.")
@click.option("--end", type=str, default="2026-01-01", show_default=True,
              help="Inclusive end time.")
@click.option("--logdir", type=click.Path(path_type=Path), default="logs",
              help="Directory for log files.")
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.option("--quiet", is_flag=True, help="Suppress console output.")
@click.help_option("-h", "--help")
def process_yolo_cli(yolo_outfile, ytoe_outfile, start, end, logdir, debug, quiet):
    """Compute effective Toe Drain and Yolo Bypass flow and write processed CSVs.

    Reads Lisbon flow/elevation and related stations from the repo, derives
    effective Toe Drain and Yolo Bypass flow, and writes
    ``datetime,value`` DMS-format CSVs for each.
    """
    level, console = resolve_loglevel(debug=debug, quiet=quiet)
    configure_logging(
        package_name="dms_datastore",
        level=level,
        console=console,
        logdir=logdir,
        logfile_prefix="process_yolo",
    )
    sdate = pd.Timestamp(start)
    edate = pd.Timestamp(end)
    toe_final, yolo_final = process_yolo(sdate, edate)
    logger.info("Processing for yolo flow complete.")
    write_ts_csv(toe_final, str(ytoe_outfile))
    write_ts_csv(yolo_final, str(yolo_outfile))
    logger.info("Wrote %s", ytoe_outfile)
    logger.info("Wrote %s", yolo_outfile)


if __name__ == "__main__":
    process_yolo_cli()
