import logging
import os.path as osp
import re
from functools import lru_cache
from pathlib import Path

import click
import pandas as pd

from vtools import ts_merge
from vtools.functions.error_detect import med_outliers
from dms_datastore.read_multi import read_ts_repo
from dms_datastore.logging_config import configure_logging, resolve_loglevel

logger = logging.getLogger(__name__)

_BUFFER = pd.Timedelta(days=5)
_INTERVAL = "15min"
_TEMP_RANGE = (1.0, 40.0)  # degC; values outside are treated as bad data

# Cross-fill order carried over from the legacy script: (destination, source).
_CROSS_FILL = [
    ("sjr", "yolo_toedrain"),
    ("sjr", "east"),
    ("yolo_toedrain", "east"),
    ("yolo_toedrain", "sjr"),
    ("east", "sac"),
    ("sac", "east"),
    ("east", "sjr"),
    ("sac", "sjr"),
    ("american", "east"),
    ("american", "sac"),
]


def _read_temp(station_id, provider, sdate, edate):
    """Read a water temperature series for one station/provider as a Series, padded by ``_BUFFER``."""
    df = read_ts_repo(
        station_id=station_id,
        variable="temp",
        start=sdate - _BUFFER,
        end=edate + _BUFFER,
        provider_priority=[provider],
    )
    return df["value"]


def _to_interval(ts, limit):
    """Resample ``ts`` to 15 minutes and interpolate gaps of at most ``limit`` steps."""
    return ts.resample(_INTERVAL).interpolate(method="index", limit=limit)


def _fillin_columns(df, dest, src):
    """Fill missing values in column ``dest`` from column ``src``."""
    df[dest] = df[dest].fillna(df[src])


def _build_sac_temp(sdate, edate):
    """Merge NCRO Knights Landing, USGS Wilkins Slough, and USGS Freeport temperature."""
    knights = _to_interval(_read_temp("klup", "ncro", sdate, edate), 12)
    wilkins = _to_interval(_read_temp("wlk", "usgs", sdate, edate), 12)
    freeport = _to_interval(_read_temp("fpt", "usgs", sdate, edate), 8)
    return ts_merge([knights, wilkins, freeport])


def _build_sjr_temp(sdate, edate):
    """Merge USGS Vernalis and USBR Vernalis temperature."""
    vns = _to_interval(_read_temp("vns", "usgs", sdate, edate), 8)
    ver = _to_interval(_read_temp("ver", "usbr", sdate, edate), 8)
    return ts_merge([vns, ver])


def _build_east_temp(sdate, edate):
    """Merge NCRO Mokelumne (mkn) and USBR North Fork Mokelumne (nmr) temperature."""
    mkn = _read_temp("mkn", "ncro", sdate, edate)
    mkn = med_outliers(mkn, scale=0.2, range=(2, 35))
    mkn = _to_interval(mkn, 20)
    nmr = _read_temp("nmr", "usbr", sdate, edate)
    nmr = med_outliers(nmr, scale=2.0, range=(2, 35))
    nmr = _to_interval(nmr, 20)
    return ts_merge([mkn, nmr])


def _build_american_temp(sdate, edate):
    """Merge USGS Watt Avenue and Fair Oaks temperature, then fill remaining gaps."""
    watt = _to_interval(_read_temp("awb", "usgs", sdate, edate), 20)
    fairoak = _to_interval(_read_temp("afo", "usgs", sdate, edate), 20)
    # Unlimited fill is a placeholder inherited from the legacy script.
    return ts_merge([watt, fairoak]).interpolate()


def _build_yolo_toedrain_temp(sdate, edate):
    """Read NCRO Lisbon temperature with outliers removed."""
    lisbon = _read_temp("lis", "ncro", sdate, edate)
    lisbon = med_outliers(lisbon, scale=2.0, range=(5, 35))
    return _to_interval(lisbon, 8)


@lru_cache(maxsize=2)
def _build_temp_block(sdate, edate):
    """Build the cross-filled water temperature table for all boundary locations.

    Returns
    -------
    pandas.DataFrame
        15-minute temperature with columns ``sac``, ``sjr``, ``east``,
        ``american``, ``yolo_toedrain``, ``coyote``, ``napa``,
        ``calaveras``, ``consumnes``, and ``yolo``, clipped to
        ``sdate``:``edate``.
    """
    block = pd.concat(
        {
            "sjr": _build_sjr_temp(sdate, edate),
            "east": _build_east_temp(sdate, edate),
            "american": _build_american_temp(sdate, edate),
            "sac": _build_sac_temp(sdate, edate),
            "yolo_toedrain": _build_yolo_toedrain_temp(sdate, edate),
        },
        axis=1,
    )
    block = block.mask((block < _TEMP_RANGE[0]) | (block > _TEMP_RANGE[1]))
    block = block.resample(_INTERVAL).asfreq()

    # East is used for filling but then replaced by American, which is colder.
    for dest, src in _CROSS_FILL:
        _fillin_columns(block, dest, src)
    block["east"] = block["american"]
    block["coyote"] = block["east"]
    block["napa"] = block["east"]
    block["calaveras"] = block["east"]
    block["consumnes"] = block["east"]
    block["yolo"] = block["yolo_toedrain"]
    return block.loc[sdate:edate]


def _write_temp(ts, outdir, filename, label):
    """Check ``ts`` for gaps and write it to ``outdir/filename``."""
    ts = ts.copy()
    nmissing = int(ts.isnull().sum())
    if nmissing > 0:
        raise ValueError(
            "There are {} missing values in the {} water temperature after processing.".format(
                nmissing, label
            )
        )
    ts.index.name = "Date/time"
    ts.name = "Temperature (degC)"
    ts.to_csv(osp.join(outdir, filename), float_format="%.1f")
    logger.info("Wrote %s", osp.join(outdir, filename))


def process_sac_temp(sdate, edate, outdir):
    """Process Sacramento River water temperature boundary condition.

    NCRO Knights Landing (``klup``, interpolation limit 12), USGS Wilkins
    Slough (``wlk``, limit 12), and USGS Freeport (``fpt``, limit 8) are
    merged with that priority. Values outside 1-40 degC are masked, and
    remaining gaps are filled from the east and San Joaquin series. The
    result is written to ``sac_temp.csv``.

    Parameters
    ----------
    sdate : pandas.Timestamp
        Start of the period to which the final product is clipped.
    edate : pandas.Timestamp
        End of the period to which the final product is clipped.
    outdir : str
        Directory to which ``sac_temp.csv`` is written.

    Returns
    -------
    None
        Writes the processed temperature series to ``sac_temp.csv`` in
        ``outdir``.

    Raises
    ------
    ValueError
        If missing values remain in the Sacramento temperature after
        processing.
    """
    _write_temp(_build_temp_block(sdate, edate)["sac"], outdir, "sac_temp.csv", "Sacramento")


def process_sjr_temp(sdate, edate, outdir):
    """Process San Joaquin River water temperature boundary condition.

    USGS Vernalis (``vns``) and USBR Vernalis (``ver``, hourly upsampled
    to 15 minutes) are interpolated (limit 8) and merged with USGS taking
    priority. Remaining gaps are filled from the Yolo Toe Drain and east
    series. The result is written to ``sjr_temp.csv``.

    Parameters
    ----------
    sdate : pandas.Timestamp
        Start of the period to which the final product is clipped.
    edate : pandas.Timestamp
        End of the period to which the final product is clipped.
    outdir : str
        Directory to which ``sjr_temp.csv`` is written.

    Returns
    -------
    None
        Writes the processed temperature series to ``sjr_temp.csv`` in
        ``outdir``.

    Raises
    ------
    ValueError
        If missing values remain in the San Joaquin temperature after
        processing.
    """
    _write_temp(_build_temp_block(sdate, edate)["sjr"], outdir, "sjr_temp.csv", "San Joaquin")


def process_american_temp(sdate, edate, outdir):
    """Process American River water temperature boundary condition.

    USGS Watt Avenue (``awb``) and Fair Oaks (``afo``) are interpolated
    (limit 20), merged, and gap-filled by interpolation. Any remaining
    gaps are filled from the east and Sacramento series. The result is
    written to ``american_temp.csv``.

    Parameters
    ----------
    sdate : pandas.Timestamp
        Start of the period to which the final product is clipped.
    edate : pandas.Timestamp
        End of the period to which the final product is clipped.
    outdir : str
        Directory to which ``american_temp.csv`` is written.

    Returns
    -------
    None
        Writes the processed temperature series to ``american_temp.csv``
        in ``outdir``.

    Raises
    ------
    ValueError
        If missing values remain in the American temperature after
        processing.
    """
    _write_temp(_build_temp_block(sdate, edate)["american"], outdir, "american_temp.csv", "American")


def process_east_temp(sdate, edate, outdir):
    """Process eastside stream water temperature boundary condition.

    NCRO Mokelumne (``mkn``) and USBR North Fork Mokelumne (``nmr``) are
    cleaned with ``med_outliers``, interpolated (limit 20), and merged
    for gap filling, but the product itself is set equal to American
    River temperature because eastside streams are colder. The result is
    written to ``east_temp.csv``.

    Parameters
    ----------
    sdate : pandas.Timestamp
        Start of the period to which the final product is clipped.
    edate : pandas.Timestamp
        End of the period to which the final product is clipped.
    outdir : str
        Directory to which ``east_temp.csv`` is written.

    Returns
    -------
    None
        Writes the processed temperature series to ``east_temp.csv`` in
        ``outdir``.

    Raises
    ------
    ValueError
        If missing values remain in the east temperature after
        processing.
    """
    _write_temp(_build_temp_block(sdate, edate)["east"], outdir, "east_temp.csv", "East")


def process_coyote_temp(sdate, edate, outdir):
    """Process Coyote Creek water temperature, assumed equal to the east series.

    Parameters
    ----------
    sdate : pandas.Timestamp
        Start of the period to which the final product is clipped.
    edate : pandas.Timestamp
        End of the period to which the final product is clipped.
    outdir : str
        Directory to which ``coyote_temp.csv`` is written.

    Returns
    -------
    None
        Writes the processed temperature series to ``coyote_temp.csv`` in
        ``outdir``.

    Raises
    ------
    ValueError
        If missing values remain in the Coyote Creek temperature.
    """
    _write_temp(_build_temp_block(sdate, edate)["coyote"], outdir, "coyote_temp.csv", "Coyote Creek")


def process_napa_temp(sdate, edate, outdir):
    """Process Napa River water temperature, assumed equal to the east series.

    Parameters
    ----------
    sdate : pandas.Timestamp
        Start of the period to which the final product is clipped.
    edate : pandas.Timestamp
        End of the period to which the final product is clipped.
    outdir : str
        Directory to which ``napa_temp.csv`` is written.

    Returns
    -------
    None
        Writes the processed temperature series to ``napa_temp.csv`` in
        ``outdir``.

    Raises
    ------
    ValueError
        If missing values remain in the Napa River temperature.
    """
    _write_temp(_build_temp_block(sdate, edate)["napa"], outdir, "napa_temp.csv", "Napa")


def process_calaveras_temp(sdate, edate, outdir):
    """Process Calaveras River water temperature, assumed equal to the east series.

    Parameters
    ----------
    sdate : pandas.Timestamp
        Start of the period to which the final product is clipped.
    edate : pandas.Timestamp
        End of the period to which the final product is clipped.
    outdir : str
        Directory to which ``calaveras_temp.csv`` is written.

    Returns
    -------
    None
        Writes the processed temperature series to ``calaveras_temp.csv``
        in ``outdir``.

    Raises
    ------
    ValueError
        If missing values remain in the Calaveras temperature.
    """
    _write_temp(_build_temp_block(sdate, edate)["calaveras"], outdir, "calaveras_temp.csv", "Calaveras")


def process_consumnes_temp(sdate, edate, outdir):
    """Process Consumnes River water temperature, assumed equal to the east series.

    Parameters
    ----------
    sdate : pandas.Timestamp
        Start of the period to which the final product is clipped.
    edate : pandas.Timestamp
        End of the period to which the final product is clipped.
    outdir : str
        Directory to which ``consumnes_temp.csv`` is written.

    Returns
    -------
    None
        Writes the processed temperature series to ``consumnes_temp.csv``
        in ``outdir``.

    Raises
    ------
    ValueError
        If missing values remain in the Consumnes temperature.
    """
    _write_temp(_build_temp_block(sdate, edate)["consumnes"], outdir, "consumnes_temp.csv", "Consumnes")


def process_yolo_toedrain_temp(sdate, edate, outdir):
    """Process Yolo Toe Drain water temperature boundary condition.

    NCRO Lisbon (``lis``) is cleaned with ``med_outliers`` (range 5-35
    degC) and interpolated (limit 8). Remaining gaps are filled from the
    east and San Joaquin series. The result is written to
    ``yolo_toedrain_temp.csv``.

    Parameters
    ----------
    sdate : pandas.Timestamp
        Start of the period to which the final product is clipped.
    edate : pandas.Timestamp
        End of the period to which the final product is clipped.
    outdir : str
        Directory to which ``yolo_toedrain_temp.csv`` is written.

    Returns
    -------
    None
        Writes the processed temperature series to
        ``yolo_toedrain_temp.csv`` in ``outdir``.

    Raises
    ------
    ValueError
        If missing values remain in the Yolo Toe Drain temperature after
        processing.
    """
    _write_temp(
        _build_temp_block(sdate, edate)["yolo_toedrain"],
        outdir,
        "yolo_toedrain_temp.csv",
        "Yolo Toe Drain",
    )


def process_yolo_temp(sdate, edate, outdir):
    """Process Yolo Bypass water temperature, assumed equal to the Toe Drain series.

    Parameters
    ----------
    sdate : pandas.Timestamp
        Start of the period to which the final product is clipped.
    edate : pandas.Timestamp
        End of the period to which the final product is clipped.
    outdir : str
        Directory to which ``yolo_temp.csv`` is written.

    Returns
    -------
    None
        Writes the processed temperature series to ``yolo_temp.csv`` in
        ``outdir``.

    Raises
    ------
    ValueError
        If missing values remain in the Yolo temperature.
    """
    _write_temp(_build_temp_block(sdate, edate)["yolo"], outdir, "yolo_temp.csv", "Yolo")


# Ordered registry of processed-temperature products: (step name, worker function).
# Each worker has the signature ``func(sdate, edate, outdir)`` and writes one product CSV.
_TW_STEPS = [
    ("sac", process_sac_temp),
    ("sjr", process_sjr_temp),
    ("american", process_american_temp),
    ("east", process_east_temp),
    ("coyote", process_coyote_temp),
    ("napa", process_napa_temp),
    ("calaveras", process_calaveras_temp),
    ("consumnes", process_consumnes_temp),
    ("yolo_toedrain", process_yolo_toedrain_temp),
    ("yolo", process_yolo_temp),
]

TW_STEP_NAMES = [name for name, _ in _TW_STEPS]


def _split_multi(values):
    """Flatten a tuple of CLI option values, splitting each on commas/whitespace."""
    out = []
    for v in values or ():
        out.extend(part for part in re.split(r"[,\s]+", str(v).strip()) if part)
    return out


def select_tw_steps(include=None, exclude=None):
    """Resolve which water temperature steps to run.

    Parameters
    ----------
    include : sequence of str, optional
        Run only these steps. Mutually exclusive with ``exclude``.
    exclude : sequence of str, optional
        Run every step except these.

    Returns
    -------
    list of (str, callable)
        The ordered ``(name, worker)`` pairs to run.

    Raises
    ------
    ValueError
        If both ``include`` and ``exclude`` are given, or if any name is not a
        recognized step.
    """
    include = _split_multi(include)
    exclude = _split_multi(exclude)
    if include and exclude:
        raise ValueError("Use only one of --include or --exclude, not both.")

    unknown = [s for s in include + exclude if s not in TW_STEP_NAMES]
    if unknown:
        raise ValueError(
            f"Unknown temperature step(s): {unknown}. Valid steps: {TW_STEP_NAMES}."
        )

    if include:
        keep = set(include)
        return [(n, f) for n, f in _TW_STEPS if n in keep]
    if exclude:
        drop = set(exclude)
        return [(n, f) for n, f in _TW_STEPS if n not in drop]
    return list(_TW_STEPS)


def process_tw(start, end, outdir, include=None, exclude=None):
    """Run the selected processed-temperature products, writing one CSV per step.

    Parameters
    ----------
    start, end : str or pandas.Timestamp
        Inclusive bounds of the processing window. ``end`` defaults to
        today when ``None``.
    outdir : str or pathlib.Path
        Directory to which the product CSVs are written.
    include : sequence of str, optional
        Run only these steps (starts from nothing).
    exclude : sequence of str, optional
        Run all steps except these (starts from everything).
    """
    sdate = pd.to_datetime(start)
    edate = pd.Timestamp.now().normalize() if end is None else pd.to_datetime(end)
    outdir = str(outdir)
    for name, func in select_tw_steps(include=include, exclude=exclude):
        logger.info("Processing temperature step: %s", name)
        func(sdate, edate, outdir)


@click.command("process_tw")
@click.option("--start", type=str, default="2020-01-01 00:00:00", show_default=True,
              help="Start of the processing window.")
@click.option("--end", type=str, default=None,
              help="End of the processing window. Defaults to today.")
@click.option("--outdir", type=click.Path(path_type=Path),
              default=r"\\cnrastore-bdo\Modeling_Data\repo_processing_scratch",
              show_default=True, help="Directory to write output CSVs to.")
@click.option("--include", multiple=True,
              help="Run ONLY these steps (starts from nothing). Repeatable or "
                   "comma-/space-separated. Steps: " + ", ".join(TW_STEP_NAMES) + ".")
@click.option("--exclude", multiple=True,
              help="Run EVERYTHING except these steps (starts from everything). "
                   "Repeatable or comma-/space-separated. Mutually exclusive with --include.")
@click.option("--logdir", type=click.Path(path_type=Path), default="logs",
              help="Directory for log files.")
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.option("--quiet", is_flag=True, help="Suppress console output.")
@click.help_option("-h", "--help")
def process_tw_cli(start, end, outdir, include, exclude, logdir, debug, quiet):
    """Run one or all processed water temperature products, writing CSVs to --outdir.

    By default every step runs. Use --include to run only specific steps
    (selection starts from nothing) or --exclude to run everything except
    certain steps. The two options are mutually exclusive. Available steps:
    sac, sjr, american, east, coyote, napa, calaveras, consumnes, yolo_toedrain,
    yolo.
    """
    level, console = resolve_loglevel(debug=debug, quiet=quiet)
    configure_logging(
        package_name="dms_datastore",
        level=level,
        console=console,
        logdir=logdir,
        logfile_prefix="process_tw",
    )
    process_tw(start, end, outdir, include=include, exclude=exclude)


if __name__ == '__main__':
    process_tw_cli()
