import logging
from pathlib import Path

import click
import pandas as pd
import numpy as np
import statsmodels.api as sm
import matplotlib.pyplot as plt

from dms_datastore.read_multi import read_ts_repo
from dms_datastore.write_ts import *
from dms_datastore.logging_config import configure_logging, resolve_loglevel
from vtools.data.gap import describe_null
from vtools import cosine_lanczos
from vtools.functions.blend import ts_blend

logger = logging.getLogger(__name__)


def fit_and_impute(dependent: pd.Series,
                   independent: pd.Series,
                   fit_start=None,
                   fit_end=None):
    """
    Fit dependent ~ independent via OLS on timestamps where both are non-missing,
    then predict at all timestamps and fill NaNs in the original dependent series
    with those predictions.

    Parameters
    ----------
    dependent : pd.Series
        The series to be imputed (e.g., subtidal component of Pt Reyes).
    independent : pd.Series
        The predictor series (e.g., subtidal component of Monterey).
    fit_start : datetime-like or None
        Start of calibration window for OLS training. If None, use all available.
    fit_end : datetime-like or None
        End of calibration window for OLS training. If None, use all available.

    Returns
    -------
    model : statsmodels.regression.linear_model.RegressionResultsWrapper
        The fitted OLS model.
    preds : pd.Series
        Predicted values at every timestamp.
    imputed : pd.Series
        Original dependent series with NaNs filled by preds.
    """
    y, x = dependent.align(independent, join='outer')
    df = pd.DataFrame({'y': y, 'x': x})
    df_valid = df.dropna(subset=['y','x'])
    if fit_start is not None:
        df_valid = df_valid.loc[fit_start:]
    if fit_end is not None:
        df_valid = df_valid.loc[:fit_end]
    model = sm.OLS(df_valid['y'], sm.add_constant(df_valid['x'])).fit()
    raw = model.predict(sm.add_constant(df['x']))
    if hasattr(raw, 'ndim') and raw.ndim > 1:
        raw = raw.iloc[:, 0] if isinstance(raw, pd.DataFrame) else raw.flatten()
    preds = pd.Series(raw, index=df.index, name=f"{dependent.name or 'y'}_pred")
    imputed = dependent.reindex(df.index).fillna(preds)
    return model, preds, imputed


def process_elev_data(plot: bool=False,
                      start=None,
                      end=None,
                      fit_start=None,
                      fit_end=None,
                      gap_warn_limit=40) -> tuple:
    """
    Process elevation data for SF, Pt Reyes, and Monterey by loading,
    decomposing tidal/subtidal, imputing gaps across stations, and recombining.

    This includes:
    - Loading harmonic predictions and extracting tidal components.
    - Loading raw elevation data and linear-interpolating small gaps.
    - Low-pass filtering (40h) to isolate subtidal signals.
    - Imputing subtidal gaps: PtReyes<->Monterey and SF<-PtReyes.
    - Blending at gap edges over 1 day transitions.
    - Recombining subtidal+tidal and filling raw-data gaps.

    Parameters
    ----------
    plot : bool, optional
        If True, generate diagnostic plots. Default False.
    start : datetime-like or None
        Start timestamp for read_ts_repo. Uses full history if None.
    end : datetime-like or None
        End timestamp for read_ts_repo. Uses full history if None.
    fit_start : datetime-like or None
        Start of OLS calibration window. If None, use all available data.
    fit_end : datetime-like or None
        End of OLS calibration window. If None, use all available data.

    Returns
    -------
    result : pd.DataFrame
        DataFrame indexed on the common time grid, columns:
        'sf', 'ptr', 'mtr' = final filled series.
    models : dict
        Dict with keys 'ptr_mtr', 'mtr_ptr', 'sf_ptr' mapping to fitted OLS models.
    """
    # 1) Harmonic predictions -> tidal components
    logger.info("Loading harmonic predictions and extracting tidal comps...")
    sf_ha = read_ts_repo("sffpx","predictions", start=start, end=end)
    sf_ha.columns = ['sf']
    sf_sub = cosine_lanczos(sf_ha,'40h')['sf']
    sf_tidal = sf_ha['sf'] - sf_sub

    ptr_ha = read_ts_repo("pryc1","predictions", start=start, end=end).rename(columns={'value':'ptr'})
    ptr_ha.columns = ['ptr']
    ptr_sub = cosine_lanczos(ptr_ha,'40h')['ptr']
    ptr_tidal = ptr_ha['ptr'] - ptr_sub

    mtr_ha = read_ts_repo("mtyc1","predictions", start=start, end=end).rename(columns={'value':'mtr'})
    mtr_ha.columns = ['mtr']
    mtr_sub = cosine_lanczos(mtr_ha,'40h')['mtr']
    mtr_tidal = mtr_ha['mtr'] - mtr_sub

    # 2) Raw observations -> interpolate small gaps
    logger.info("Loading raw elev and interpolating small gaps...")
    sf_raw = read_ts_repo("sffpx","elev", start=start, end=end).interpolate(limit=4, method='linear').rename(columns={'value':'sf'})
    sf_raw.loc[pd.Timestamp(2024,1,1): pd.Timestamp(2024,8,1), :] = np.nan
    ptr_raw = read_ts_repo("pryc1","elev", start=start, end=end).interpolate(limit=4, method='linear').rename(columns={'value':'ptr'})
    mtr_raw = read_ts_repo("mtyc1","elev", start=start, end=end).interpolate(limit=4, method='linear').rename(columns={'value':'mtr'})

    # 3) Subtidal from raw
    logger.info("Filtering subtidal (40h Lanczos)...")
    sf_low = cosine_lanczos(sf_raw,'40h')['sf']
    ptr_low = cosine_lanczos(ptr_raw,'40h')['ptr']
    mtr_low = cosine_lanczos(mtr_raw,'40h')['mtr']

    # 4) Impute subtidal across stations
    logger.info("Imputing subtidal PtReyes<->Monterey...")
    mdl_ptr, ptr_pred, ptr_imp = fit_and_impute(ptr_low, mtr_low, fit_start=fit_start, fit_end=fit_end)
    ptr_imp_blend = ts_blend([ptr_low, ptr_imp], blend_length='1D')

    mdl_mtr, mtr_pred, mtr_imp = fit_and_impute(mtr_low, ptr_low, fit_start=fit_start, fit_end=fit_end)
    mtr_imp_blend = ts_blend([mtr_low, mtr_imp], blend_length='1D')

    logger.info("Imputing SF subtidal based on PtReyes...")
    mdl_sf, sf_pred, sf_imp = fit_and_impute(sf_low, ptr_low, fit_start=fit_start, fit_end=fit_end)
    sf_imp_blend = ts_blend([sf_low, sf_imp], blend_length='1D')

    # 5) Recombine subtidal + tidal
    logger.info("Interpolating remaining subtidal gaps before adding tide...")
    for name, series in [('SF', sf_imp_blend), ('PtReyes', ptr_imp_blend), ('Monterey', mtr_imp_blend)]:
        n_missing = series.isna().sum()
        if n_missing >= gap_warn_limit:
            logger.warning(f"{n_missing} missing subtidal values remain in {name} after imputation and blending.")
            describe_null(series, name)
    ptr_imp_blend = ptr_imp_blend.interpolate(method='linear')
    mtr_imp_blend = mtr_imp_blend.interpolate(method='linear')
    sf_imp_blend  = sf_imp_blend.interpolate(method='linear')

    ptr_comb = ptr_imp_blend + ptr_tidal.reindex(ptr_imp_blend.index)
    mtr_comb = mtr_imp_blend + mtr_tidal.reindex(mtr_imp_blend.index)
    sf_comb  = sf_imp_blend  + sf_tidal.reindex(sf_imp_blend.index)

    # 6) Fill raw gaps
    logger.info("Filling remaining raw-data gaps...")
    ptr_filled = ptr_raw['ptr'].fillna(ptr_comb)
    mtr_filled = mtr_raw['mtr'].fillna(mtr_comb)
    sf_filled  = sf_raw['sf'].fillna(sf_comb)

    # 7) Assemble and optionally plot
    result = pd.DataFrame({
        'sf': sf_filled,
        'ptr': ptr_filled,
        'mtr': mtr_filled
    })
    if plot:
        logger.info("Plotting diagnostics...")
        fig, ax = plt.subplots(3, 1, figsize=(12, 8), sharex=True)
        ax[0].plot(sf_raw.index, sf_raw['sf'], label='SF raw', alpha=0.6)
        ax[0].plot(sf_comb.index, sf_comb, '--', label='SF sub+tidal')
        ax[0].plot(sf_filled.index, sf_filled, label='SF final')
        ax[0].legend()
        ax[1].plot(ptr_raw.index, ptr_raw['ptr'], label='PtReyes raw', alpha=0.6)
        ax[1].plot(ptr_comb.index, ptr_comb, '--', label='PtReyes sub+tidal')
        ax[1].plot(ptr_filled.index, ptr_filled, label='PtReyes final')
        ax[1].legend()
        ax[2].plot(mtr_raw.index, mtr_raw['mtr'], label='Monterey raw', alpha=0.6)
        ax[2].plot(mtr_comb.index, mtr_comb, '--', label='Monterey sub+tidal')
        ax[2].plot(mtr_filled.index, mtr_filled, label='Monterey final')
        ax[2].legend()
        plt.tight_layout()
        plt.show()
    models = {'ptr_mtr': mdl_ptr, 'mtr_ptr': mdl_mtr, 'sf_ptr': mdl_sf}
    return result, models


# Fit window defaults span ~1 lunar nodal cycle (18.6 yr) ending 2022-10-01.
# Changing --fit-start/--fit-end will alter regression coefficients and filled values.
@click.command("process_elev")
@click.option('--start', type=str, default='1999-01-01',
              help='Data start date.')
@click.option('--end', type=str, default=None,
              help='Data end date (default: today).')
@click.option('--fit-start', type=str, default='2004-03-01',
              help='OLS calibration window start (~1 nodal cycle before fit-end).')
@click.option('--fit-end', type=str, default='2022-10-01',
              help='OLS calibration window end.')
@click.option('--plot', is_flag=True, default=False,
              help='Generate diagnostic plots.')
@click.option('--no-save', is_flag=True, default=False,
              help='Skip writing output CSVs.')
@click.option('--logdir', type=click.Path(path_type=Path), default="logs",
              help='Directory for log files.')
@click.option('--debug', is_flag=True, help='Enable debug logging.')
@click.option('--quiet', is_flag=True, help='Suppress console output.')
@click.help_option("-h", "--help")
def process_elev_cli(start, end, fit_start, fit_end, plot, no_save, logdir, debug, quiet):
    """Process and gap-fill ocean elevation data for SF Bay hindcast.

    Fit window defaults reflect the calibration epoch 2004-03 to 2022-10
    (~1 lunar nodal cycle, 18.6 yr). Changing --fit-start/--fit-end will
    alter regression coefficients and therefore filled values.
    """
    level, console = resolve_loglevel(debug=debug, quiet=quiet)
    configure_logging(
        package_name="dms_datastore",
        level=level,
        console=console,
        logdir=logdir,
        logfile_prefix="process_elev",
    )
    end_ts = pd.Timestamp(end) if end else pd.Timestamp.now()
    ts, models = process_elev_data(
        plot=plot,
        start=pd.Timestamp(start),
        end=end_ts,
        fit_start=pd.Timestamp(fit_start),
        fit_end=pd.Timestamp(fit_end)
    )

    if not no_save:
        fit_period = f"{fit_start} to {fit_end} (~1 lunar nodal cycle, 18.6 yr)"
        explanation = "filled data from NOAA tidal station"

        def _model_meta(model, predictor_name):
            params = model.params
            return {
                "ols_intercept": f"{params.iloc[0]:.6f}",
                "ols_slope": f"{params.iloc[1]:.6f}",
                "ols_predictor": predictor_name,
                "fit_period": fit_period,
            }

        base_meta = {"agency": "dms", "units": "m", "datum": "NAVD88", "comment": explanation}

        write_ts_csv(ts.ptr, "dms_ptreyes_pryc1_elev_1999_2026.csv",
                     metadata={**base_meta, "agency_station_name": "Point Reyes",
                               **_model_meta(models['ptr_mtr'], 'mtyc1_subtidal')})
        write_ts_csv(ts.mtr, "dms_monterey_mtyc1_elev_1999_2026.csv",
                     metadata={**base_meta, "agency_station_name": "Monterey",
                               **_model_meta(models['mtr_ptr'], 'pryc1_subtidal')})
        write_ts_csv(ts.sf, "dms_sf_sffpx_elev_1999_2026.csv",
                     metadata={**base_meta, "agency_station_name": "San Francisco",
                               **_model_meta(models['sf_ptr'], 'pryc1_subtidal')})


if __name__ == '__main__':
    process_elev_cli()

