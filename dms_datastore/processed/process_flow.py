import logging
import os.path as osp
from pathlib import Path

import click
import pandas as pd

import matplotlib.pyplot as plt
from vtools import *
from dms_datastore.read_ts import *
from vtools.functions.unit_conversions import ec_psu_25c,CFS2CMS,CMS2CFS
from vtools.functions.error_detect import *
import yaml
from dms_datastore.read_multi import read_ts_repo
from dms_datastore.logging_config import configure_logging, resolve_loglevel
from vtools.functions.neighbor_fill import fill_from_neighbor
from vtools.functions.lag_cross_correlation import calculate_lag

logger = logging.getLogger(__name__)


def process_mokelumne_flow(sdate, edate, outdir):
    """Process Mokelumne River flow at Woodbridge (station ``wbr``).

    Daily flow from USGS/EBMUD (merged via provider priority) is
    gap-filled (interpolation limit 4), converted to a period index, and
    disaggregated to 15-minute values using ``rhistinterp`` on a
    constant-shifted series, then clipped to remove negative values. The
    result is written to ``moke_flow.csv``.

    Parameters
    ----------
    sdate : pandas.Timestamp
        Start of the period over which Mokelumne flow is read and
        processed.
    edate : pandas.Timestamp
        End of the period over which Mokelumne flow is read and
        processed.
    outdir : str
        Directory to which ``moke_flow.csv`` is written.

    Returns
    -------
    None
        Writes the processed flow series to ``moke_flow.csv`` in
        ``outdir``.

    Raises
    ------
    ValueError
        If missing values remain in the Mokelumne flow after merging
        USGS and EBMUD data sources.
    """
    moke = read_ts_repo(station_id='wbr', variable='flow',start=sdate,end=edate, repo='daily_formatted', provider_priority=['usgs','ebmud'])
    moke = moke.interpolate(limit=4).to_period(freq='D')
    moke = rhistinterp(moke+4, minutes(15), lowbound=0.0, p=1, maxiter=8) - 4.0
    moke = moke.clip(lower=0.0)

    if moke.isnull().sum().sum() > 0:
        raise ValueError("There are {} missing values in the Mokelumne flow after merging USGS and EBMUD data sources.".format(moke.isnull().sum().sum()))
    else:
        filename = 'moke_flow.csv'
        moke.index.name = 'Date/time'
        moke.name = 'Flow (cfs)'
        moke.to_csv(osp.join(outdir, filename))
        logger.info("Wrote %s", osp.join(outdir, filename))



def process_consumnes_flow(sdate, edate, outdir):
    """Process Consumnes River flow at Michigan Bar (station ``mhb``).

    USGS flow is read and gap-filled by direct interpolation. If the
    processing window fully covers 2023-08-19 through 2023-08-29, that
    span is replaced with its own mean value (preserving a legacy
    manual correction). The result is written to
    ``consumnes_flow.csv``.

    Parameters
    ----------
    sdate : pandas.Timestamp
        Start of the period over which Consumnes flow is read and
        processed.
    edate : pandas.Timestamp
        End of the period over which Consumnes flow is read and
        processed.
    outdir : str
        Directory to which ``consumnes_flow.csv`` is written.

    Returns
    -------
    None
        Writes the processed flow series to ``consumnes_flow.csv`` in
        ``outdir``.

    Raises
    ------
    ValueError
        If missing values remain in the Consumnes flow at MHB after
        interpolation.
    """
    mhb_usgs = read_ts_repo(station_id='mhb', variable='flow',start=sdate,end=edate)
    mhb_usgs = mhb_usgs.interpolate()
    if sdate < pd.to_datetime('2023-08-19') and edate > pd.to_datetime('2023-08-29'):
        replace_val = mhb_usgs.loc["2023-08-19":"2023-08-29"]['value'].mean()
        mhb_usgs.loc["2023-08-19":"2023-08-29"] = replace_val #preserve the modification from the archived script
    if mhb_usgs.isnull().sum().sum() > 0:
        raise ValueError("There are {} missing values in the Consumnes flow at MHB after interpolation.".format(mhb_usgs.isnull().sum().sum()))
    else:
        filename = 'consumnes_flow.csv'
        mhb_usgs.index.name = 'Date/time'
        mhb_usgs.name = 'Flow (cfs)'
        mhb_usgs.to_csv(osp.join(outdir, filename))
        logger.info("Wrote %s", osp.join(outdir, filename))



def process_american_sac_flow(sdate, edate, outdir):
    """Process American River flow and derive Sacramento River flow at I and K Streets.

    USGS flow at Freeport (station ``fpt``) is gap-filled
    (interpolation limit 20), tidally filtered (``cosine_lanczos``,
    40-hour cutoff), and further gap-filled by interpolation; USGS flow
    at Fair Oaks (station ``afo``) is gap-filled by interpolation and
    restricted to the window widened by the 3-hour Sacramento lag.
    Sacramento flow at I Street is computed as Freeport flow minus
    American River flow, then shifted -3 hours to align with I Street.
    Freeport flow itself is treated as the K Street product. The three
    series are written to ``sac_i_flow.csv``, ``sac_k_flow.csv``, and
    ``american_flow.csv`` respectively.

    Parameters
    ----------
    sdate : pandas.Timestamp
        Start of the period to which the final products are clipped
        (data are read over a wider buffered window).
    edate : pandas.Timestamp
        End of the period to which the final products are clipped
        (data are read over a wider buffered window).
    outdir : str
        Directory to which ``sac_i_flow.csv``, ``sac_k_flow.csv``, and
        ``american_flow.csv`` are written.

    Returns
    -------
    None
        Writes the three processed flow series to CSV files in
        ``outdir``.

    Raises
    ------
    ValueError
        If American River flow exceeds Freeport flow at any time, or if
        missing values remain in any of the three final series after
        processing.
    """
    buffer = days(5)
    lag_sac = hours(3)
    freeport_usgs = read_ts_repo(station_id='fpt', variable='flow',start=sdate-buffer,end=edate+buffer)
    freeport_usgs = freeport_usgs.interpolate(limit=20)
    freeport_usgs = cosine_lanczos(freeport_usgs, hours(40))
    freeport_usgs = freeport_usgs.interpolate() #Place holder: filling gaps directly with interpolation

    american_usgs = read_ts_repo(station_id='afo', variable='flow',start=sdate-buffer,end=edate+buffer)
    american_usgs = american_usgs.interpolate() #Place holder: filling gaps directly with interpolation
    american_usgs = american_usgs[(sdate-lag_sac):(edate+lag_sac)]

    sac_i = freeport_usgs.sub(american_usgs.squeeze(), axis=0)
    if (sac_i.lt(0.)).values.any(axis=0):
        raise ValueError("American flow can't be greater than Freeport")

    sac_i = sac_i.shift(-3,hours(1),)[sdate:edate]
    freeport_usgs = freeport_usgs[sdate:edate]

    if sac_i.isnull().sum().sum() > 0:
        raise ValueError("There are {} missing values in the Sacramento flow at I street after processing.".format(sac_i.isnull().sum().sum()))
    else:
        sac_i_filename = 'sac_i_flow.csv'
        sac_i.index.name = 'Date/time'
        sac_i.name = 'Flow (cfs)'
        sac_i.to_csv(osp.join(outdir, sac_i_filename))
        logger.info("Wrote %s", osp.join(outdir, sac_i_filename))
    if freeport_usgs.isnull().sum().sum() > 0:
        raise ValueError("There are {} missing values in the Sacramento flow at K street after processing.".format(freeport_usgs.isnull().sum().sum()))
    else:
        freeport_usgs_filename = 'sac_k_flow.csv'
        freeport_usgs.index.name = 'Date/time'
        freeport_usgs.name = 'Flow (cfs)'
        freeport_usgs.to_csv(osp.join(outdir, freeport_usgs_filename))
        logger.info("Wrote %s", osp.join(outdir, freeport_usgs_filename))
    if american_usgs.isnull().sum().sum() > 0:
        raise ValueError("There are {} missing values in the American flow after processing.".format(american_usgs.isnull().sum().sum()))
    else:
        american_usgs_filename = 'american_flow.csv'
        american_usgs.index.name = 'Date/time'
        american_usgs.name = 'Flow (cfs)'
        american_usgs.to_csv(osp.join(outdir, american_usgs_filename))
        logger.info("Wrote %s", osp.join(outdir, american_usgs_filename))



def process_calaveras_flow(sdate, edate, outdir):
    """Process Calaveras River flow at New Hogan Lake (station ``nhg``).

    Daily flow is resampled to 15-minute intervals and gap-filled by
    interpolation (limit 20). Following a legacy assumption, summer
    flows below 500 cfs are treated as not reaching the Delta and set
    to zero; remaining gaps are filled with zero and then
    forward-filled (limit 7). The result is written to
    ``calaveras_flow.csv``.

    Parameters
    ----------
    sdate : pandas.Timestamp
        Start of the period over which Calaveras flow is read and
        processed.
    edate : pandas.Timestamp
        End of the period over which Calaveras flow is read and
        processed.
    outdir : str
        Directory to which ``calaveras_flow.csv`` is written.

    Returns
    -------
    None
        Writes the processed flow series to ``calaveras_flow.csv`` in
        ``outdir``.

    Raises
    ------
    ValueError
        If missing values remain in the Calaveras flow after
        processing.
    """
    interval = minutes(15)
    calaveras = read_ts_repo(station_id='nhg', variable='flow',start=sdate,end=edate, repo='daily_formatted')
    calaveras = calaveras.resample(interval).interpolate(limit=20)
    # Carrty the assumption that summer flows below 500cfs do not reach the Delta from legacy script
    calaveras[calaveras.lt(500.)] = 0.
    calaveras = calaveras.fillna(0.)
    calaveras = calaveras.ffill(limit=7)

    if calaveras.isnull().sum().sum() > 0:
        raise ValueError("There are {} missing values in the Calaveras flow after processing.".format(calaveras.isnull().sum().sum()))
    else:
        filename = 'calaveras_flow.csv'
        calaveras.index.name = 'Date/time'
        calaveras.name = 'Flow (cfs)'
        calaveras.to_csv(osp.join(outdir, filename))
        logger.info("Wrote %s", osp.join(outdir, filename))





def process_northbay_flow(sdate, edate, outdir):
    """Process North Bay Aqueduct diversion flow at Barker Slough (station ``bks``).

    Sub-daily CDEC flow is collapsed to a daily mean, converted to a
    period index, and gap-filled by interpolation (limit 2), then
    disaggregated to 15-minute values using ``rhistinterp`` on a
    constant-shifted series. The result is written to
    ``northbay_flow.csv``.

    Parameters
    ----------
    sdate : pandas.Timestamp
        Start of the period over which North Bay diversion flow is
        read and processed.
    edate : pandas.Timestamp
        End of the period over which North Bay diversion flow is read
        and processed.
    outdir : str
        Directory to which ``northbay_flow.csv`` is written.

    Returns
    -------
    None
        Writes the processed flow series to ``northbay_flow.csv`` in
        ``outdir``.

    Raises
    ------
    ValueError
        If missing values remain in the North Bay diversion flow after
        processing.
    """
    interval = minutes(15)
    bks_cdec = read_ts_repo(station_id='bks', variable='flow', start=sdate, end=edate, repo='daily_formatted')
    bks_cdec = bks_cdec.resample('D').mean()   # collapse hourly readings to one row/day before to_period
    bks_cdec = bks_cdec.to_period(freq='D')
    bks_cdec = bks_cdec.interpolate(limit=2)
    bks_cdec.columns = ['value']
    bks_cdec.index.name = 'datetime'

    bks_cdec = rhistinterp(bks_cdec+10, interval,lowbound=0.0,p=20.) - 10

    if bks_cdec.isnull().sum().sum() > 0:
        raise ValueError("There are {} missing values in the North Bay diversion flow after processing.".format(bks_cdec.isnull().sum().sum()))
    else:
        filename = 'northbay_flow.csv'
        bks_cdec.index.name = 'Date/time'
        bks_cdec.name = 'Flow (cfs)'
        bks_cdec.to_csv(osp.join(outdir, filename))
        logger.info("Wrote %s", osp.join(outdir, filename))


def process_ccwd_flow(sdate, edate, outdir):
    """Process CCWD diversion flow at Rock Slough, Old River, and Victoria Canal.

    Daily flow at Rock Slough (station ``inb``), Old River (station
    ``idb``), and Victoria Canal (station ``ccw``) is each gap-filled
    and disaggregated to 15-minute values via the nested
    :func:`ccwd_intake` helper, using a different interpolation gap
    limit for each station. The three series are written to
    ``ccc_flow.csv``, ``cccoldr_flow.csv``, and ``ccw_flow.csv``
    respectively.

    Parameters
    ----------
    sdate : pandas.Timestamp
        Start of the period over which CCWD diversion flow is read and
        processed.
    edate : pandas.Timestamp
        End of the period over which CCWD diversion flow is read and
        processed.
    outdir : str
        Directory to which ``ccc_flow.csv``, ``cccoldr_flow.csv``, and
        ``ccw_flow.csv`` are written.

    Returns
    -------
    None
        Writes the three processed flow series to CSV files in
        ``outdir``.

    Raises
    ------
    ValueError
        If missing values remain in any of the three diversion flow
        series after processing.
    """

    def ccwd_intake(data, interp_limit):
        """Interpolate and disaggregate a daily CCWD diversion flow series to 15-minute values.

        Parameters
        ----------
        data : pandas.DataFrame or pandas.Series
            Daily diversion flow series, indexed by datetime,
            potentially containing gaps.
        interp_limit : int
            Maximum number of consecutive missing daily values to fill
            by interpolation before disaggregation.

        Returns
        -------
        pandas.DataFrame or pandas.Series
            The series interpolated, converted to a period index, and
            disaggregated to 15-minute values via ``rhistinterp``,
            clipped to remove negative values.
        """
        data = data.interpolate(limit=interp_limit)
        data = data.to_period()
        data = rhistinterp(data+5, minutes(15), lowbound=0.0, p=6) - 5
        data = data.clip(lower=0.0)
        return data


    ccc_rock = read_ts_repo(station_id='inb', variable='flow', start=sdate, end=edate, repo='daily_formatted')
    ccc_old = read_ts_repo(station_id='idb', variable='flow', start=sdate, end=edate, repo='daily_formatted')
    ccc_victoria = read_ts_repo(station_id='ccw', variable='flow', start=sdate, end=edate, repo='daily_formatted')

    ccc_rock = ccwd_intake(ccc_rock, interp_limit=5)
    ccc_old = ccwd_intake(ccc_old, interp_limit=2)
    ccc_victoria = ccwd_intake(ccc_victoria, interp_limit=2)

    if ccc_rock.isnull().sum().sum() > 0:
        raise ValueError("There are {} missing values in the CCWD Rock Slough diversion flow after processing.".format(ccc_rock.isnull().sum().sum()))
    else:
        filename = 'ccc_flow.csv'
        ccc_rock.index.name = 'Date/time'
        ccc_rock.name = 'Flow (cfs)'
        ccc_rock.to_csv(osp.join(outdir, filename))
        logger.info("Wrote %s", osp.join(outdir, filename))

    if ccc_old.isnull().sum().sum() > 0:
        raise ValueError("There are {} missing values in the CCWD Old River diversion flow after processing.".format(ccc_old.isnull().sum().sum()))
    else:
        filename = 'cccoldr_flow.csv'
        ccc_old.index.name = 'Date/time'
        ccc_old.name = 'Flow (cfs)'
        ccc_old.to_csv(osp.join(outdir, filename))
        logger.info("Wrote %s", osp.join(outdir, filename))

    if ccc_victoria.isnull().sum().sum() > 0:
        raise ValueError("There are {} missing values in the CCWD Victoria Canal diversion flow after processing.".format(ccc_victoria.isnull().sum().sum()))
    else:
        filename = 'ccw_flow.csv'
        ccc_victoria.index.name = 'Date/time'
        ccc_victoria.name = 'Flow (cfs)'
        ccc_victoria.to_csv(osp.join(outdir, filename))
        logger.info("Wrote %s", osp.join(outdir, filename))


def process_cvp_flow(sdate, edate, outdir):
    """Process CVP export flow at Tracy Pumping Plant (station ``trp``).

    Daily flow is gap-filled by interpolation, converted to a daily
    period index, and disaggregated to 15-minute values using
    ``rhistinterp`` on a constant-shifted series, then clipped to
    remove negative values and restricted to ``sdate``:``edate``. The
    result is written to ``cvp_flow.csv``.

    Parameters
    ----------
    sdate : pandas.Timestamp
        Start of the period to which the processed CVP flow is
        clipped.
    edate : pandas.Timestamp
        End of the period to which the processed CVP flow is clipped.
    outdir : str
        Directory to which ``cvp_flow.csv`` is written.

    Returns
    -------
    None
        Writes the processed flow series to ``cvp_flow.csv`` in
        ``outdir``.

    Raises
    ------
    ValueError
        If missing values remain in the CVP flow after processing.
    """

    cvp = read_ts_repo(station_id='trp', variable='flow', start=sdate, end=edate, repo='daily_formatted')
    cvp = cvp.interpolate()
    cvp = cvp.asfreq('D').to_period()
    cvp = rhistinterp(cvp+10, minutes(15), lowbound=0.0, p=6, maxiter=10) - 20
    cvp = cvp.clip(lower=0.0)[sdate:edate]

    if cvp.isnull().sum().sum() > 0:
        raise ValueError("There are {} missing values in the CVP flow after processing.".format(cvp.isnull().sum().sum()))
    else:
        filename = 'cvp_flow.csv'
        cvp.index.name = 'Date/time'
        cvp.name = 'Flow (cfs)'
        cvp.to_csv(osp.join(outdir, filename))
        logger.info("Wrote %s", osp.join(outdir, filename))


def process_swp_flow(sdate, edate, outdir):
    """Process SWP export flow at Harvey O. Banks Pumping Plant (station ``hro``).

    Daily flow is gap-filled by direct interpolation. The result is
    written to ``swp_flow.csv``.

    Parameters
    ----------
    sdate : pandas.Timestamp
        Start of the period over which SWP flow is read and processed.
    edate : pandas.Timestamp
        End of the period over which SWP flow is read and processed.
    outdir : str
        Directory to which ``swp_flow.csv`` is written.

    Returns
    -------
    None
        Writes the processed flow series to ``swp_flow.csv`` in
        ``outdir``.

    Raises
    ------
    ValueError
        If missing values remain in the SWP flow after processing.
    """

    swp = read_ts_repo(station_id='hro', variable='flow', start=sdate, end=edate, repo='daily_formatted')
    swp = swp.interpolate()

    if swp.isnull().sum().sum() > 0:
        raise ValueError("There are {} missing values in the SWP flow after processing.".format(swp.isnull().sum().sum()))
    else:
        filename = 'swp_flow.csv'
        swp.index.name = 'Date/time'
        swp.name = 'Flow (cfs)'
        swp.to_csv(osp.join(outdir, filename))
        logger.info("Wrote %s", osp.join(outdir, filename))


def process_napa_flow(sdate, edate, outdir):
    """Process Napa River flow near Napa (station ``napr``).

    Flow is resampled to 15-minute intervals and gap-filled by
    interpolation (limit 10); any remaining gaps are filled with zero.
    The result is written to ``napa_flow.csv``.

    Parameters
    ----------
    sdate : pandas.Timestamp
        Start of the period over which Napa River flow is read and
        processed.
    edate : pandas.Timestamp
        End of the period over which Napa River flow is read and
        processed.
    outdir : str
        Directory to which ``napa_flow.csv`` is written.

    Returns
    -------
    None
        Writes the processed flow series to ``napa_flow.csv`` in
        ``outdir``.

    Raises
    ------
    ValueError
        If missing values remain in the Napa River flow after
        processing.
    """
    interval = minutes(15)
    napa = read_ts_repo(station_id='napr', variable='flow', start=sdate, end=edate)
    napa = napa.resample(interval).interpolate(limit=10)
    napa = napa.fillna(0.0)

    if napa.isnull().sum().sum() > 0:
        raise ValueError("There are {} missing values in the Napa River flow after processing.".format(napa.isnull().sum().sum()))
    else:
        filename = 'napa_flow.csv'
        napa.index.name = 'Date/time'
        napa.name = 'Flow (cfs)'
        napa.to_csv(osp.join(outdir, filename))
        logger.info("Wrote %s", osp.join(outdir, filename))


def process_coyote_flow(sdate, edate, outdir):
    """Process Coyote Creek flow above Highway 237 (station ``coycr``).

    Flow is resampled to 15-minute intervals and gap-filled by
    interpolation (limit 50); any remaining gaps are filled with zero.
    The result is written to ``coyote_flow.csv``.

    Parameters
    ----------
    sdate : pandas.Timestamp
        Start of the period over which Coyote Creek flow is read and
        processed.
    edate : pandas.Timestamp
        End of the period over which Coyote Creek flow is read and
        processed.
    outdir : str
        Directory to which ``coyote_flow.csv`` is written.

    Returns
    -------
    None
        Writes the processed flow series to ``coyote_flow.csv`` in
        ``outdir``.

    Raises
    ------
    ValueError
        If missing values remain in the Coyote Creek flow after
        processing.
    """
    interval = minutes(15)
    coyote = read_ts_repo(station_id='coycr', variable='flow', start=sdate, end=edate)
    coyote = coyote.resample(interval).interpolate(limit=50)
    coyote = coyote.fillna(0.0)

    if coyote.isnull().sum().sum() > 0:
        raise ValueError("There are {} missing values in the Coyote Creek flow after processing.".format(coyote.isnull().sum().sum()))
    else:
        filename = 'coyote_flow.csv'
        coyote.index.name = 'Date/time'
        coyote.name = 'Flow (cfs)'
        coyote.to_csv(osp.join(outdir, filename))
        logger.info("Wrote %s", osp.join(outdir, filename))



@click.command("process_flow")
@click.option("--start", type=str, default="2020-01-01 00:00:00", show_default=True,
              help="Start of the processing window.")
@click.option("--end", type=str, default="2026-01-01 00:00:00", show_default=True,
              help="End of the processing window.")
@click.option("--outdir", type=click.Path(path_type=Path),
              default=r"\\cnrastore-bdo\Modeling_Data\repo_processing_scratch",
              show_default=True, help="Directory to write output CSVs to.")
@click.option("--skip-mokelumne", is_flag=True, help="Skip the Mokelumne flow step.")
@click.option("--skip-consumnes", is_flag=True, help="Skip the Consumnes flow step.")
@click.option("--skip-american-sac", is_flag=True, help="Skip the American/Sacramento flow step.")
@click.option("--skip-ccwd", is_flag=True, help="Skip the CCWD diversions step.")
@click.option("--skip-calaveras", is_flag=True, help="Skip the Calaveras flow step.")
@click.option("--skip-cvp", is_flag=True, help="Skip the CVP flow step.")
@click.option("--skip-swp", is_flag=True, help="Skip the SWP flow step.")
@click.option("--skip-napa", is_flag=True, help="Skip the Napa River flow step.")
@click.option("--skip-coyote", is_flag=True, help="Skip the Coyote Creek flow step.")
@click.option("--skip-northbay", is_flag=True, help="Skip the North Bay diversion flow step.")
@click.option("--logdir", type=click.Path(path_type=Path), default="logs",
              help="Directory for log files.")
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.option("--quiet", is_flag=True, help="Suppress console output.")
@click.help_option("-h", "--help")
def process_flow_cli(start, end, outdir,
                          skip_mokelumne, skip_consumnes, skip_american_sac,
                          skip_ccwd, skip_calaveras, skip_swp, skip_northbay, skip_cvp, skip_napa, skip_coyote,
                          logdir, debug, quiet):
    """Run one or all of the processed-flow products, writing CSVs to --outdir."""
    level, console = resolve_loglevel(debug=debug, quiet=quiet)
    configure_logging(
        package_name="dms_datastore",
        level=level,
        console=console,
        logdir=logdir,
        logfile_prefix="process_flow",
    )
    sdate = pd.to_datetime(start)
    edate = pd.to_datetime(end)
    outdir = str(outdir)

    if not skip_swp:
        process_swp_flow(sdate, edate, outdir)
    if not skip_northbay:
        process_northbay_flow(sdate, edate, outdir)
    if not skip_mokelumne:
        process_mokelumne_flow(sdate, edate, outdir)
    if not skip_consumnes:
        process_consumnes_flow(sdate, edate, outdir)
    if not skip_american_sac:
        process_american_sac_flow(sdate, edate, outdir)
    if not skip_ccwd:
        process_ccwd_flow(sdate, edate, outdir)
    if not skip_cvp:
        process_cvp_flow(sdate, edate, outdir)
    if not skip_calaveras:
        process_calaveras_flow(sdate, edate, outdir)

    if not skip_napa:
        process_napa_flow(sdate, edate, outdir)
    if not skip_coyote:
        process_coyote_flow(sdate, edate, outdir)


if __name__ == '__main__':
    process_flow_cli()