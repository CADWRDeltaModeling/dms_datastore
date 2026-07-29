import logging
import os.path as osp
from pathlib import Path

import click
import pandas as pd

import matplotlib.pyplot as plt
from vtools import *
from vtools import ts_merge
from dms_datastore.read_ts import *
from vtools.functions.unit_conversions import ec_psu_25c,CFS2CMS,CMS2CFS
from vtools.functions.error_detect import *
import yaml
from dms_datastore.read_multi import read_ts_repo
from dms_datastore.write_ts import write_ts_csv
from dms_datastore.logging_config import configure_logging, resolve_loglevel
from vtools.functions.neighbor_fill import (
    fill_from_neighbor,
    dfm_pack_params, save_dfm_params, load_dfm_params,
)
from vtools.functions.lag_cross_correlation import calculate_lag

logger = logging.getLogger(__name__)

def process_vernalis_flow(fit_start, fit_end, fill_start, fill_end):
    """Fill gaps in San Joaquin River flow at Vernalis (station ``vns``).

    A Dynamic Factor Model (``dfm_trimbur_rw``) is fit between USGS flow at
    Vernalis and NCRO flow at Mossdale (station ``msd``, lagged and low-pass
    filtered) over the ``fit_start``-``fit_end`` window. The fitted model
    parameters are saved to disk and then reloaded and applied to fill gaps
    in Vernalis flow over the longer ``fill_start``-``fill_end`` window. The
    filled flow series is merged with the observed data and written to
    ``sjr_flow.csv``.

    Parameters
    ----------
    fit_start : pandas.Timestamp
        Start of the period used to fit the Dynamic Factor Model and
        estimate the lag between Vernalis and Mossdale flow.
    fit_end : pandas.Timestamp
        End of the period used to fit the Dynamic Factor Model and
        estimate the lag between Vernalis and Mossdale flow.
    fill_start : pandas.Timestamp
        Start of the period over which gaps in Vernalis flow are filled
        using the fitted model parameters.
    fill_end : pandas.Timestamp
        End of the period over which gaps in Vernalis flow are filled
        using the fitted model parameters.

    Returns
    -------
    pandas.Series
        The gap-filled Vernalis flow series, indexed by datetime.

    Raises
    ------
    ValueError
        If missing values remain in the filled Vernalis flow series after
        merging the USGS and NCRO data sources.
    """
    # get filling parameters using data from 2020-2025
    vnl_usgs = read_ts_repo(station_id='vns', variable='flow',start=fit_start,end=fit_end)
    vnl_usgs = vnl_usgs.interpolate(limit=4)
    msd_ncro = read_ts_repo(station_id='msd', variable='flow',start=fit_start,end=fit_end)
    msd_ncro = msd_ncro.interpolate(limit=20)
    msd_ncro = cosine_lanczos(msd_ncro, hours(40))[fit_start:fit_end]
    lag_steps = int(calculate_lag(msd_ncro.squeeze(), vnl_usgs.squeeze(), max_lag='14h', res='15min')/minutes(15))
    msd_ncro = msd_ncro.shift(lag_steps)
    #First fill vnl gaps with residual interpolation for gaps when msd data present
    print("Filling gaps in Vernalis flow using Dynamic Factor Model...")
    dfm_trimbur_rw = fill_from_neighbor(vnl_usgs.squeeze(), msd_ncro.squeeze(), method='dfm_trimbur_rw')
    blob = dfm_pack_params(dfm_trimbur_rw["model_info"])
    save_dfm_params(blob, "dfm_trimbur_rw_vns_msd.yaml")

    # load saved parameters and fill data from 2005 - 2025
    blob2 = load_dfm_params("dfm_trimbur_rw_vns_msd.yaml")
    vnl_usgs_all = read_ts_repo(station_id='vns', variable='flow',start=fill_start,end=fill_end)
    vnl_usgs_all = vnl_usgs_all.interpolate(limit=4)
    msd_ncro_all = read_ts_repo(station_id='msd', variable='flow',start=fill_start,end=fill_end)
    msd_ncro_all = msd_ncro_all.interpolate(limit=20)
    msd_ncro_all = cosine_lanczos(msd_ncro_all, hours(40))[fill_start:fill_end]
    msd_ncro_all = msd_ncro_all.shift(lag_steps)
    res_reuse = fill_from_neighbor(vnl_usgs_all.squeeze(), msd_ncro_all.squeeze(), method='dfm_trimbur_rw', params=blob2)

    filled_vnl = res_reuse['yhat']
    filled_vnl = ts_merge([vnl_usgs_all.squeeze(), filled_vnl.squeeze()],names='value')

    if filled_vnl.isnull().sum().sum() > 0:
        raise ValueError("Warning: There are {} missing values in the Vernalis flow after merging USGS and " \
                        "NCRO data sources.".format(filled_vnl.isnull().sum().sum()))
    logger.info("No missing values in the final filled Vernalis flow data.")
    filled_vnl.index.name = 'datetime'
    filled_vnl.name = 'value'
    return filled_vnl

def process_vernalis_ec(start, end):
    """Fill gaps in San Joaquin River electrical conductivity (EC) near Vernalis.

    USGS EC at station ``sjr`` and CDEC EC at station ``ver`` are each
    masked to the plausible range (25-1425 µS/cm) and interpolated to close
    short gaps. The lag between the two series is estimated and applied to
    the CDEC series, after which a Dynamic Factor Model
    (``dfm_trimbur_rw``) is fit to fill remaining gaps in the ``sjr`` EC
    series using the ``ver`` series as a neighbor. The filled series is
    merged with the observed data and written to ``sjr_ec.csv``.

    Parameters
    ----------
    start : pandas.Timestamp
        Start of the period over which EC data are read, filled, and
        written.
    end : pandas.Timestamp
        End of the period over which EC data are read, filled, and
        written.

    Returns
    -------
    pandas.Series
        The gap-filled San Joaquin River EC series, indexed by datetime.

    Raises
    ------
    ValueError
        If missing values remain in the filled EC series after merging the
        USGS and CDEC data sources.
    """
    sjr_ec = read_ts_repo(station_id='sjr', variable='ec', start=start, end=end)
    sjr_ec = sjr_ec.mask((sjr_ec < 25.0) | (sjr_ec > 1425.0))
    sjr_ec = sjr_ec.interpolate(limit=200)

    ts_cdec = read_ts_repo(station_id='ver', variable='ec', start=start, end=end)
    ts_cdec = ts_cdec.mask((ts_cdec < 25.0) | (ts_cdec > 1425.0))
    ts_cdec = ts_cdec.interpolate(limit=150)

    lag_steps = int(calculate_lag(ts_cdec.squeeze(), sjr_ec.squeeze(), max_lag='14h', res='15min')/minutes(15))
    ts_cdec = ts_cdec.shift(lag_steps)
    print("Filling gaps in Vernalis ec using Dynamic Factor Model...")

    dfm_trimbur_rw = fill_from_neighbor(sjr_ec.squeeze(), ts_cdec.squeeze(), method='dfm_trimbur_rw')
    filled_sjr = dfm_trimbur_rw['yhat']

    filled_sjr = ts_merge([sjr_ec.squeeze(), filled_sjr.squeeze()],names='value')

    if filled_sjr.isnull().sum().sum() > 0:
        raise ValueError("Warning: There are {} missing values in the San Joaquin River EC after merging USGS and " \
                        "CDEC data sources.".format(filled_sjr.isnull().sum().sum()))
    logger.info("No missing values in the final merged San Joaquin River EC data.")
    filled_sjr.index.name = 'datetime'
    filled_sjr.name = 'value'
    return filled_sjr




def process_sjl_ele(fit_start, fit_end, fill_start, fill_end):
    """Fill gaps in San Joaquin River stage/elevation near Lathrop (station ``sjl``).

    NCRO elevation at Lathrop (``sjl``) and at Burns Cutoff/Turner Cut
    (station ``bdt``) are interpolated and low-pass filtered
    (``cosine_lanczos``, 40-hour cutoff) over the ``fit_start``-``fit_end``
    window. The lag between the two series is estimated and applied to the
    ``bdt`` series, and a Dynamic Factor Model (``dfm_trimbur_rw``) is fit
    to relate them. The fitted parameters are saved to disk, then reloaded
    and applied to fill gaps in ``sjl`` elevation over the longer
    ``fill_start``-``fill_end`` window. The filled series is merged with
    the observed data and written to ``sjr_lathrop_elevation.csv``.

    Parameters
    ----------
    fit_start : pandas.Timestamp
        Start of the period used to fit the Dynamic Factor Model and
        estimate the lag between Lathrop and Burns Cutoff/Turner Cut
        elevation.
    fit_end : pandas.Timestamp
        End of the period used to fit the Dynamic Factor Model and
        estimate the lag between Lathrop and Burns Cutoff/Turner Cut
        elevation.
    fill_start : pandas.Timestamp
        Start of the period over which gaps in Lathrop elevation are
        filled using the fitted model parameters.
    fill_end : pandas.Timestamp
        End of the period over which gaps in Lathrop elevation are filled
        using the fitted model parameters.

    Returns
    -------
    pandas.Series
        The gap-filled SJR Lathrop elevation series, indexed by datetime.

    Raises
    ------
    ValueError
        If missing values remain in the filled elevation series after
        merging the USGS and NCRO data sources.
    """
    sjl_ncro = read_ts_repo(station_id='sjl', variable='elev', start=fit_start, end=fit_end)
    sjl_ncro = sjl_ncro.interpolate(limit=4)
    sjl_ncro = cosine_lanczos(sjl_ncro, hours(40))[fit_start:fit_end]

    bdt_ncro = read_ts_repo(station_id='bdt', variable='elev', start=fit_start, end=fit_end)
    bdt_ncro = bdt_ncro.interpolate(limit=4)
    bdt_ncro = cosine_lanczos(bdt_ncro, hours(40))[fit_start:fit_end]
    lag_steps = int(calculate_lag(bdt_ncro.squeeze(), sjl_ncro.squeeze(), max_lag='14h', res='15min')/minutes(15))
    bdt_ncro = bdt_ncro.shift(lag_steps)

    print("Filling gaps in SJR Lathrop elevation using Dynamic Factor Model...")
    dfm_trimbur_rw = fill_from_neighbor(sjl_ncro.squeeze(), bdt_ncro.squeeze(), method='dfm_trimbur_rw')
    blob = dfm_pack_params(dfm_trimbur_rw["model_info"])
    save_dfm_params(blob, "dfm_trimbur_rw_sjl_bdt.yaml")

    # load saved parameters and fill data from 2005 - 2025
    blob2 = load_dfm_params("dfm_trimbur_rw_sjl_bdt.yaml")
    sjl_usgs_all = read_ts_repo(station_id='sjl', variable='elev',start=fill_start,end=fill_end)
    sjl_usgs_all = sjl_usgs_all.interpolate(limit=4)
    bdt_ncro_all = read_ts_repo(station_id='bdt', variable='elev',start=fill_start,end=fill_end)
    bdt_ncro_all = bdt_ncro_all.interpolate(limit=4)
    bdt_ncro_all = cosine_lanczos(bdt_ncro_all, hours(40))[fill_start:fill_end]
    bdt_ncro_all = bdt_ncro_all.shift(lag_steps)
    res_reuse = fill_from_neighbor(sjl_usgs_all.squeeze(), bdt_ncro_all.squeeze(), method='dfm_trimbur_rw', params=blob2)

    filled_sjl = res_reuse['yhat']
    filled_sjl = ts_merge([sjl_usgs_all.squeeze(), filled_sjl.squeeze()],names='value')

    if filled_sjl.isnull().sum().sum() > 0:
        raise ValueError("Warning: There are {} missing values in the San Joaquin Lathrop elevation after merging USGS and " \
                        "NCRO data sources.".format(filled_sjl.isnull().sum().sum()))
    logger.info("No missing values in the final filled SJR Lathrop elevation data.")
    filled_sjl.index.name = 'datetime'
    filled_sjl.name = 'value'
    return filled_sjl





@click.command("process_vns_sjl")
@click.option("--flow-outfile", type=click.Path(path_type=Path),
              default="sjr_flow.csv", show_default=True,
              help="Output CSV path for the gap-filled Vernalis flow product.")
@click.option("--ec-outfile", type=click.Path(path_type=Path),
              default="sjr_ec.csv", show_default=True,
              help="Output CSV path for the gap-filled San Joaquin River EC product.")
@click.option("--elev-outfile", type=click.Path(path_type=Path),
              default="sjr_lathrop_elevation.csv", show_default=True,
              help="Output CSV path for the gap-filled SJR Lathrop elevation product.")
@click.option("--flow-fit-start", type=str, default="2020-01-01", show_default=True,
              help="Start of the window used to fit the Vernalis flow filling model.")
@click.option("--flow-fit-end", type=str, default="2025-09-01", show_default=True,
              help="End of the window used to fit the Vernalis flow filling model.")
@click.option("--flow-fill-start", type=str, default="2005-01-01", show_default=True,
              help="Start of the window over which Vernalis flow is filled.")
@click.option("--flow-fill-end", type=str, default="2025-09-01", show_default=True,
              help="End of the window over which Vernalis flow is filled.")
@click.option("--ec-start", type=str, default="2020-01-01", show_default=True,
              help="Start of the window over which Vernalis EC is filled.")
@click.option("--ec-end", type=str, default="2025-09-01", show_default=True,
              help="End of the window over which Vernalis EC is filled.")
@click.option("--elev-fit-start", type=str, default="2002-01-01", show_default=True,
              help="Start of the window used to fit the SJR Lathrop elevation filling model.")
@click.option("--elev-fit-end", type=str, default="2025-09-01", show_default=True,
              help="End of the window used to fit the SJR Lathrop elevation filling model.")
@click.option("--elev-fill-start", type=str, default="2002-01-01", show_default=True,
              help="Start of the window over which SJR Lathrop elevation is filled.")
@click.option("--elev-fill-end", type=str, default="2025-09-01", show_default=True,
              help="End of the window over which SJR Lathrop elevation is filled.")
@click.option("--skip-flow", is_flag=True, help="Skip the Vernalis flow gap-filling step.")
@click.option("--skip-ec", is_flag=True, help="Skip the Vernalis EC gap-filling step.")
@click.option("--skip-elev", is_flag=True, help="Skip the SJR Lathrop elevation gap-filling step.")
@click.option("--logdir", type=click.Path(path_type=Path), default="logs",
              help="Directory for log files.")
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.option("--quiet", is_flag=True, help="Suppress console output.")
@click.help_option("-h", "--help")
def process_vns_sjl_cli(flow_outfile, ec_outfile, elev_outfile,
                         flow_fit_start, flow_fit_end, flow_fill_start, flow_fill_end,
                         ec_start, ec_end,
                         elev_fit_start, elev_fit_end, elev_fill_start, elev_fill_end,
                         skip_flow, skip_ec, skip_elev,
                         logdir, debug, quiet):
    """Gap-fill Vernalis flow/EC and SJR Lathrop elevation and write processed CSVs.

    Each product is fit and filled using a Dynamic Factor Model against a
    neighboring station, merged with observed data, and written as a
    ``datetime,value`` DMS-format CSV.
    """
    level, console = resolve_loglevel(debug=debug, quiet=quiet)
    configure_logging(
        package_name="dms_datastore",
        level=level,
        console=console,
        logdir=logdir,
        logfile_prefix="process_vns_sjl",
    )
    if not skip_flow:
        filled_vnl = process_vernalis_flow(
            pd.Timestamp(flow_fit_start), pd.Timestamp(flow_fit_end),
            pd.Timestamp(flow_fill_start), pd.Timestamp(flow_fill_end),
        )
        write_ts_csv(filled_vnl, str(flow_outfile))
        logger.info("Wrote %s", flow_outfile)

    if not skip_ec:
        filled_sjr = process_vernalis_ec(pd.Timestamp(ec_start), pd.Timestamp(ec_end))
        write_ts_csv(filled_sjr, str(ec_outfile))
        logger.info("Wrote %s", ec_outfile)

    if not skip_elev:
        filled_sjl = process_sjl_ele(
            pd.Timestamp(elev_fit_start), pd.Timestamp(elev_fit_end),
            pd.Timestamp(elev_fill_start), pd.Timestamp(elev_fill_end),
        )
        write_ts_csv(filled_sjl, str(elev_outfile))
        logger.info("Wrote %s", elev_outfile)


if __name__ == "__main__":
    process_vns_sjl_cli()
    