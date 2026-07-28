import os.path as osp
import pandas as pd

import matplotlib.pyplot as plt
from vtools import *
from vtools import ts_merge
from dms_datastore.read_ts import *
from vtools.functions.unit_conversions import ec_psu_25c,CFS2CMS,CMS2CFS
from vtools.functions.error_detect import *
import yaml
from dms_datastore.read_multi import read_ts_repo
from vtools.functions.neighbor_fill import (
    fill_from_neighbor,
    dfm_pack_params, save_dfm_params, load_dfm_params,
)
from vtools.functions.lag_cross_correlation import calculate_lag

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
    None
        The gap-filled Vernalis flow series is written to ``sjr_flow.csv``
        in the current working directory.

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
    else:
        print("No missing values in the final filled Vernalis flow data. Writting processed data to sjr_flow.csv")
        filled_vnl.index.name = 'Date/time'
        filled_vnl.name = 'Flow (cfs)'
        filled_vnl.to_csv('sjr_flow.csv')

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
    None
        The gap-filled EC series is written to ``sjr_ec.csv`` in the
        current working directory.

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
    else:
        print("No missing values in the final merged San Joaquin River EC data. Writting processed data to sjr_ec.csv")
        filled_sjr.index.name = 'Date/time'
        filled_sjr.name = 'EC (µS/cm)'
        filled_sjr.to_csv('sjr_ec.csv')




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
    None
        The gap-filled Lathrop elevation series is written to
        ``sjr_lathrop_elevation.csv`` in the current working directory.

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
    else:
        print("No missing values in the final filled SJR Lathrop elevation data. Writting processed data to sjr_lathrop_elevation.csv")
        filled_sjl.index.name = 'Date/time'
        filled_sjl.name = 'Elevation (ft)'
        filled_sjl.to_csv('sjr_lathrop_elevation.csv')





if __name__ == '__main__':
    sdate1 = pd.to_datetime('2020-01-01')
    edate1 = pd.to_datetime('2025-09-01')
    sdate2 = pd.to_datetime('2005-01-01')
    sdate3 = pd.to_datetime('2002-01-01')
    process_vernalis_flow(sdate1, edate1, sdate2, edate1)
    process_vernalis_ec(sdate1, edate1)
    process_sjl_ele(sdate3, edate1, sdate3, edate1)
    