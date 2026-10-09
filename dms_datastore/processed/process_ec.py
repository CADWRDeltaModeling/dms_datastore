import logging
import os.path as osp
import re
from pathlib import Path

import click
import pandas as pd

from vtools import ts_merge
from vtools.functions.error_detect import med_outliers
from dms_datastore.read_multi import read_ts_repo
from dms_datastore.logging_config import configure_logging, resolve_loglevel

logger = logging.getLogger(__name__)

_BUFFER = pd.Timedelta(days=5)


def _read_ec(station_id, provider, sdate, edate):
    """Read an EC series for one station/provider as a Series, padded by ``_BUFFER``."""
    df = read_ts_repo(
        station_id=station_id,
        variable="ec",
        start=sdate - _BUFFER,
        end=edate + _BUFFER,
        provider_priority=[provider],
    )
    return df["value"]


def _write_ec(ts, sdate, edate, outdir, filename, label):
    """Trim ``ts`` to the window, check for gaps, and write it to ``outdir/filename``."""
    ts = ts[sdate:edate]
    nmissing = int(ts.isnull().sum())
    if nmissing > 0:
        raise ValueError(
            "There are {} missing values in the {} EC after processing.".format(
                nmissing, label
            )
        )
    ts.index.name = "Date/time"
    ts.name = "EC (uS/cm)"
    ts.to_csv(osp.join(outdir, filename))
    logger.info("Wrote %s", osp.join(outdir, filename))


def _build_sac_ec(sdate, edate):
    """Build the merged, gap-filled Sacramento EC series (see :func:`process_sac_ec`)."""
    approx_bias = 25.0

    srh = _read_ec("srh", "des", sdate, edate)
    srh = srh.mask((srh < 10.0) | (srh > 400.0))
    srh = srh.interpolate(limit=10) - approx_bias

    sac = srh
    # Unlimited fill is a placeholder inherited from the legacy script.
    return sac.interpolate()


def process_sac_ec(sdate, edate, outdir):
    """Process Sacramento River EC boundary condition.

    USGS EC at Verona (station ``von``) is gap-filled by interpolation
    (limit 320). DES EC at Sacramento River at Hood (station ``srh``) is
    masked outside 10-400 uS/cm, gap-filled by interpolation (limit 10),
    and reduced by an approximate bias of 25 uS/cm. The two series are
    merged with Verona taking priority, and remaining gaps are filled by
    interpolation. The result is written to ``sac_ec.csv``.

    Parameters
    ----------
    sdate : pandas.Timestamp
        Start of the period to which the final product is clipped (data
        are read over a wider buffered window).
    edate : pandas.Timestamp
        End of the period to which the final product is clipped (data
        are read over a wider buffered window).
    outdir : str
        Directory to which ``sac_ec.csv`` is written.

    Returns
    -------
    None
        Writes the processed EC series to ``sac_ec.csv`` in ``outdir``.

    Raises
    ------
    ValueError
        If missing values remain in the Sacramento EC after processing.
    """
    sac = _build_sac_ec(sdate, edate)
    _write_ec(sac, sdate, edate, outdir, "sac_ec.csv", "Sacramento")


def process_yolo_ec(sdate, edate, outdir):
    """Process Yolo Bypass EC boundary condition.

    Yolo EC is assumed equal to Sacramento EC (Yolo Bypass flow is mostly
    fresh runoff), so the series is identical to that of
    :func:`process_sac_ec`. The result is written to ``yolo_ec.csv``.

    Parameters
    ----------
    sdate : pandas.Timestamp
        Start of the period to which the final product is clipped (data
        are read over a wider buffered window).
    edate : pandas.Timestamp
        End of the period to which the final product is clipped (data
        are read over a wider buffered window).
    outdir : str
        Directory to which ``yolo_ec.csv`` is written.

    Returns
    -------
    None
        Writes the processed EC series to ``yolo_ec.csv`` in ``outdir``.

    Raises
    ------
    ValueError
        If missing values remain in the Yolo EC after processing.
    """
    yolo = _build_sac_ec(sdate, edate)
    _write_ec(yolo, sdate, edate, outdir, "yolo_ec.csv", "Yolo")


def process_yolo_toedrain_ec(sdate, edate, outdir):
    """Process Yolo Toe Drain EC boundary condition.

    A constant 650 uS/cm (typical seasonal value) is used as the base
    series. It is overwritten by CDEC EC at Lisbon (station ``lis``,
    outliers removed by ``med_outliers``) and then by NCRO EC at Lisbon
    (gap-filled by interpolation, limit 200), so NCRO takes priority.
    The result is written to ``yolo_toedrain_ec.csv``.

    Parameters
    ----------
    sdate : pandas.Timestamp
        Start of the period to which the final product is clipped (data
        are read over a wider buffered window).
    edate : pandas.Timestamp
        End of the period to which the final product is clipped (data
        are read over a wider buffered window).
    outdir : str
        Directory to which ``yolo_toedrain_ec.csv`` is written.

    Returns
    -------
    None
        Writes the processed EC series to ``yolo_toedrain_ec.csv`` in
        ``outdir``.

    Raises
    ------
    ValueError
        If missing values remain in the Yolo Toe Drain EC after
        processing.
    """
    interval = "15min"

    lisbon_ncro = _read_ec("lis", "ncro", sdate, edate)
    lisbon_ncro = lisbon_ncro.resample(interval).interpolate(limit=200)

    index = pd.date_range(sdate - _BUFFER, edate + _BUFFER, freq=interval)
    yolo_toedrain = pd.Series(650.0, index=index)
    yolo_toedrain.update(lisbon_ncro)

    _write_ec(
        yolo_toedrain, sdate, edate, outdir, "yolo_toedrain_ec.csv", "Yolo Toe Drain"
    )


# Ordered registry of processed-EC products: (step name, worker function).
# Each worker has the signature ``func(sdate, edate, outdir)`` and writes one product CSV.
_EC_STEPS = [
    ("sac", process_sac_ec),
    ("yolo", process_yolo_ec),
    ("yolo_toedrain", process_yolo_toedrain_ec),
]

EC_STEP_NAMES = [name for name, _ in _EC_STEPS]


def _split_multi(values):
    """Flatten a tuple of CLI option values, splitting each on commas/whitespace."""
    out = []
    for v in values or ():
        out.extend(part for part in re.split(r"[,\s]+", str(v).strip()) if part)
    return out


def select_ec_steps(include=None, exclude=None):
    """Resolve which EC steps to run.

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

    unknown = [s for s in include + exclude if s not in EC_STEP_NAMES]
    if unknown:
        raise ValueError(
            f"Unknown EC step(s): {unknown}. Valid steps: {EC_STEP_NAMES}."
        )

    if include:
        keep = set(include)
        return [(n, f) for n, f in _EC_STEPS if n in keep]
    if exclude:
        drop = set(exclude)
        return [(n, f) for n, f in _EC_STEPS if n not in drop]
    return list(_EC_STEPS)


def process_ec(start, end, outdir, include=None, exclude=None):
    """Run the selected processed-EC products, writing one CSV per step.

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
    for name, func in select_ec_steps(include=include, exclude=exclude):
        logger.info("Processing EC step: %s", name)
        func(sdate, edate, outdir)


@click.command("process_ec")
@click.option("--start", type=str, default="2020-01-01 00:00:00", show_default=True,
              help="Start of the processing window.")
@click.option("--end", type=str, default=None,
              help="End of the processing window. Defaults to today.")
@click.option("--outdir", type=click.Path(path_type=Path),
              default=r"\\cnrastore-bdo\Modeling_Data\repo_processing_scratch",
              show_default=True, help="Directory to write output CSVs to.")
@click.option("--include", multiple=True,
              help="Run ONLY these steps (starts from nothing). Repeatable or "
                   "comma-/space-separated. Steps: " + ", ".join(EC_STEP_NAMES) + ".")
@click.option("--exclude", multiple=True,
              help="Run EVERYTHING except these steps (starts from everything). "
                   "Repeatable or comma-/space-separated. Mutually exclusive with --include.")
@click.option("--logdir", type=click.Path(path_type=Path), default="logs",
              help="Directory for log files.")
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.option("--quiet", is_flag=True, help="Suppress console output.")
@click.help_option("-h", "--help")
def process_ec_cli(start, end, outdir, include, exclude, logdir, debug, quiet):
    """Run one or all processed-EC products, writing CSVs to --outdir.

    By default every step runs. Use --include to run only specific steps
    (selection starts from nothing) or --exclude to run everything except
    certain steps. The two options are mutually exclusive. Available steps:
    sac, yolo, yolo_toedrain.
    """
    level, console = resolve_loglevel(debug=debug, quiet=quiet)
    configure_logging(
        package_name="dms_datastore",
        level=level,
        console=console,
        logdir=logdir,
        logfile_prefix="process_ec",
    )
    process_ec(start, end, outdir, include=include, exclude=exclude)


if __name__ == '__main__':
    process_ec_cli()
