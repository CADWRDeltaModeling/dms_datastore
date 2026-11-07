#!/usr/bin/env python
"""Banks Pumping Plant (SWP) flow from the SAP unit status change log.

Input is the consolidated change log written by :mod:`merge_banks_exports`
(``sap_banks_2006_9999_qaqc.csv``).  It lists pumping unit status changes in
Pacific local clock time (no DST correction), one row per unit change, with
``Exclude=X`` marking superseded rows.  Each unit that is in service is assumed to
pump at its rated capacity, so a status change adds or removes that capacity.  The
logged ``CFS`` value is only used to warn when the running total disagrees.

Steps:

1. read the log and drop ``Exclude=X`` rows,
2. build the flow change events (stable ordering by time and unit, so rows that tie
   keep their file order),
3. put the events on a regular grid (local clock time),
4. convert to PST with :func:`vtools.dst_st` and refill the regular PST grid,
5. smooth with a gaussian filter,
6. write ``dms_hro_flow.csv`` in cfs (DMS format, ``timezone: PST`` in the header).

BBID, CVP and the other exports are handled elsewhere.
"""
import logging
from pathlib import Path

import click
import numpy as np
import pandas as pd
from scipy.ndimage import gaussian_filter1d
from vtools import dst_st

from dms_datastore.logging_config import configure_logging, resolve_loglevel
from dms_datastore.read_ts import read_ts
from dms_datastore.write_ts import write_ts_csv

logger = logging.getLogger(__name__)

SRR = 0
DFD = 2
RATINGS = {"srr": SRR, "dfd": DFD}

# Rated capacity (cfs) by unit number 1..11 for each rating version; slot 0 is unused.
CAPACITY = [
    [0] + [375] * 2 + [1130] * 9,
    [0] + [375] * 2 + [1067] * 6 + [1130] * 3,
    [0] + [375] * 2 + [1129] * 5 + [1101] * 4,
]

# 1 = pumping, 0 = not pumping
OPS_STATUS = {
    "T/IS": 1,
    "T/S": 0,
    "I/S": 1,
    "I/P": 1,
    "S/D": 0,
    "SED": 0,
    "SO/S": 0,
    "FO/S": 0,
    "FOG": 0,
    "FOP": 0,
    "SFP": 0,
}

CFS_MISMATCH_TOL = 200.0
DEFAULT_INFILE = "W:/clifton_court/banks_pumping/sap_banks_2006_9999_qaqc.csv"
DEFAULT_OUTFILE = "dms_hro_flow.csv"


def read_banks_log(path):
    """Read the consolidated change log as text, skipping leading ``#`` lines.

    Returns a DataFrame in file order with a ``Datetime`` column (naive local time)
    and an integer ``UnitNo`` column.
    """
    path = Path(path)
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        nskip = 0
        for line in f:
            if not line.startswith("#"):
                break
            nskip += 1
    df = pd.read_csv(path, dtype=str, keep_default_na=False, skiprows=nskip, encoding="utf-8-sig")
    required = ["Time", "Unit", "Status", "Date", "CFS", "Exclude"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{path}: missing columns {missing}")

    time = df["Time"].str.strip().str.zfill(4)
    bad_time = ~time.str.fullmatch(r"\d{4}")
    if bad_time.any():
        raise ValueError(f"{path}: bad Time values {time[bad_time].unique()[:10]}")
    date = pd.to_datetime(df["Date"].str.strip(), format="%m/%d/%Y", errors="coerce")
    if date.isna().any():
        raise ValueError(f"{path}: bad Date values {df.loc[date.isna(), 'Date'].unique()[:10]}")
    minutes = time.str.slice(0, 2).astype(int) * 60 + time.str.slice(2, 4).astype(int)
    df["Datetime"] = date + pd.to_timedelta(minutes, unit="min")

    unitno = pd.to_numeric(df["Unit"].str.extract(r"(\d+)\s*$")[0], errors="coerce")
    if unitno.isna().any() or not unitno.between(1, 11).all():
        raise ValueError(f"{path}: bad Unit values {df.loc[~unitno.between(1, 11), 'Unit'].unique()[:10]}")
    df["UnitNo"] = unitno.astype(int)
    return df


def flow_change(rating, unit, op, unit_status):
    """Change in flow (cfs) when ``unit`` goes to the state implied by ``op``.

    ``unit_status`` is updated in place.  Test starts (``T...``) are not changes.
    """
    if op.startswith("T"):
        return 0.0
    new_status = OPS_STATUS[op]
    if new_status == unit_status[unit]:
        return 0.0
    unit_status[unit] = new_status
    cap = CAPACITY[rating][unit]
    return float(cap if new_status == 1 else -cap)


def parse_banks(df, rating=DFD):
    """Convert the change log into flow change events.

    Parameters
    ----------
    df : DataFrame
        Output of :func:`read_banks_log`, in file order.
    rating : int
        Capacity table, ``SRR`` or ``DFD``.

    Returns
    -------
    events : Series
        Flow (cfs) in effect from each change time, naive local time.
    last_time : Timestamp
        Time of the last non-excluded row.
    """
    excl = df["Exclude"].str.strip().str.lower()
    bad = ~excl.isin(["", "x"])
    if bad.any():
        raise ValueError(f"Bad value in Exclude column: {df.loc[bad, 'Exclude'].unique()[:10]}")
    df = df[excl == ""]
    if df.empty:
        raise ValueError("No usable rows in the change log")

    cfs = df["CFS"].str.lower().str.replace("cfs", "", regex=False).str.strip().str.lstrip("0")
    logged = pd.to_numeric(cfs.replace("", np.nan), errors="coerce")
    ops = df["Status"].str.split(",").str[0].str.strip()

    ev = pd.DataFrame(
        {"t": df["Datetime"], "unit": df["UnitNo"], "op": ops, "logged": logged}
    ).sort_values(["t", "unit"], kind="stable")

    unknown = ~ev["op"].isin(OPS_STATUS)
    if unknown.any():
        first = ev[unknown].iloc[0]
        raise ValueError(f"Op unknown: {first.t} unit={first.unit} op={first.op!r}")

    unit_status = [0] * 12
    t0 = ev["t"].iloc[0]
    flow_old = 0.0
    times, flows = [], []
    mismatches = 0
    for t, unit, op, q in ev.itertuples(index=False):
        dq = flow_change(rating, unit, op, unit_status)
        if t == t0:
            dq = max(dq, 0.0)  # initial out of service marker is status, not a change
        flow = flow_old + dq
        if not np.isnan(q) and abs(flow - q) > CFS_MISMATCH_TOL:
            mismatches += 1
            logger.debug("%s unit %d: calculated %.0f vs logged %.0f cfs", t, unit, flow, q)
        if dq != 0.0:
            times.append(t)
            flows.append(flow)
        flow_old = flow
    if mismatches:
        logger.warning(
            "%d rows where the calculated flow differs from the logged CFS by more than %.0f cfs "
            "(details at debug level)",
            mismatches,
            CFS_MISMATCH_TOL,
        )

    events = pd.Series(flows, index=pd.DatetimeIndex(times), name="value")
    # The last of several changes at the same time wins
    events = events[~events.index.duplicated(keep="last")]
    return events, ev["t"].iloc[-1]


def regularize_pst(events, last_time, interval_min=15):
    """Regular PST series from change events given in local clock time.

    The events are held (forward filled) on a regular local-clock grid and converted
    with :func:`vtools.dst_st`.  That drops points in the repeated fall-back hour and
    shifts points in the skipped spring-forward hour, so the PST grid is rebuilt and
    forward filled over those points.
    """
    freq = f"{interval_min}min"
    grid = pd.date_range(events.index[0].ceil(freq), pd.Timestamp(last_time).ceil(freq), freq=freq)
    local = events.reindex(events.index.union(grid)).ffill().reindex(grid)

    pst = dst_st(local)
    pst = pst[~pst.index.duplicated(keep="last")].sort_index()
    pst_grid = pd.date_range(pst.index[0], pst.index[-1], freq=freq)
    exact = pst_grid.isin(pst.index).sum()
    regular = pst.reindex(pst_grid, method="ffill")
    logger.info(
        "DST conversion: %d local points -> %d regular PST points (%d filled in repeated/skipped DST hours)",
        len(local),
        len(regular),
        len(pst_grid) - exact,
    )
    return regular


def process_banks(infile, rating=DFD, interval_min=15, sigma=2.0, start=None, end=None):
    """Banks pumping flow (cfs) on a regular PST grid.

    Parameters
    ----------
    start, end : datetime-like, optional
        Output bounds in PST.  Default is the first change to the last row of the log.
        Times beyond the data are held at the last flow.
    sigma : float
        Gaussian smoothing width in time steps.  Zero or negative disables smoothing.
    """
    df = read_banks_log(infile)
    events, last_time = parse_banks(df, rating)
    logger.info("%d change events from %s to %s", len(events), events.index[0], last_time)

    flow = regularize_pst(events, last_time, interval_min)
    if sigma and sigma > 0:
        flow[:] = gaussian_filter1d(flow.to_numpy(dtype=float), sigma=sigma, mode="nearest", order=0)

    freq = f"{interval_min}min"
    if start is not None or end is not None:
        s = pd.Timestamp(start) if start is not None else flow.index[0]
        e = pd.Timestamp(end) if end is not None else flow.index[-1]
        flow = flow.reindex(pd.date_range(s.ceil(freq), e.floor(freq), freq=freq), method="ffill")
        if flow.isna().any():
            raise ValueError(f"--start {s} is before the first change in the log ({events.index[0]})")
    flow.name = "value"
    flow.index.name = "datetime"
    return flow


def compare_hro_daily(flow, hro_path):
    """Daily means of the computed flow against CDEC HRO daily flow.

    Only complete days of the computed series are compared.  ``diff`` is
    ``processed - hro``.
    """
    hro = read_ts(hro_path)
    if isinstance(hro, pd.DataFrame):
        hro = hro.iloc[:, 0]
    step = flow.index[1] - flow.index[0]
    full_day = int(round(pd.Timedelta(days=1) / step))
    daily = flow.resample("1D").mean()[flow.resample("1D").count() == full_day]
    comp = pd.concat([daily, hro], axis=1, join="inner")
    comp.columns = ["processed", "hro"]
    comp["diff"] = comp["processed"] - comp["hro"]
    comp.index.name = "datetime"
    return comp


@click.command("process_banks")
@click.option("--infile", type=click.Path(exists=True, dir_okay=False, path_type=Path),
              default=DEFAULT_INFILE, show_default=True,
              help="Consolidated change log CSV (output of merge_banks_exports).")
@click.option("--outfile", type=click.Path(dir_okay=False, path_type=Path),
              default=DEFAULT_OUTFILE, show_default=True,
              help="Output CSV (cfs, PST). With --chunk-years, _<year> is added to the name.")
@click.option("--chunk-years", is_flag=True, help="Write one file per year.")
@click.option("--rating", type=click.Choice(list(RATINGS)), default="dfd", show_default=True,
              help="Unit capacity table.")
@click.option("--interval", "interval_min", type=int, default=15, show_default=True,
              help="Output interval in minutes.")
@click.option("--sigma", type=float, default=2.0, show_default=True,
              help="Gaussian smoothing width in time steps (0 disables).")
@click.option("--start", type=str, default=None, help="Optional start time (PST).")
@click.option("--end", type=str, default=None,
              help="Optional end time (PST). Default is the last row of the log.")
@click.option("--compare", "compare_file", type=str, default=None,
              help="CDEC HRO daily flow file or glob to compare against. Writes <outfile>_vs_hro_daily.csv.")
@click.option("--plot", is_flag=True, help="Plot the comparison (needs --compare).")
@click.option("--logdir", type=click.Path(path_type=Path), default="logs", help="Directory for log files.")
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.option("--quiet", is_flag=True, help="Suppress console output.")
@click.help_option("-h", "--help")
def process_banks_cli(infile, outfile, chunk_years, rating, interval_min, sigma, start, end,
                      compare_file, plot, logdir, debug, quiet):
    """Banks Pumping Plant (SWP) flow in cfs from the SAP unit status change log."""
    level, console = resolve_loglevel(debug=debug, quiet=quiet)
    configure_logging(
        package_name="dms_datastore",
        level=level,
        console=console,
        logdir=logdir,
        logfile_prefix="process_banks",
    )
    flow = process_banks(infile, rating=RATINGS[rating], interval_min=interval_min,
                         sigma=sigma, start=start, end=end)
    metadata = {
        "agency": "dms",
        "station_name": "Harvey O. Banks Pumping Plant (SWP)",
        "param": "flow",
        "unit": "ft^3/s",
        "timezone": "PST",
        "source_file": Path(infile).name,
        "rating": rating,
        "comment": "Unit status change log converted to flow with unit capacities, then smoothed",
    }
    write_ts_csv(flow, str(outfile), metadata=metadata, chunk_years=chunk_years, float_format="%.2f")
    logger.info("Wrote %s (%d values, %s to %s)", outfile, len(flow), flow.index[0], flow.index[-1])

    if compare_file:
        comp = compare_hro_daily(flow, compare_file)
        comp_file = Path(outfile).with_name(Path(outfile).stem + "_vs_hro_daily.csv")
        comp.to_csv(comp_file, float_format="%.2f", date_format="%Y-%m-%d")
        logger.info(
            "Compared %d days with HRO daily: mean diff %.1f cfs, mean abs diff %.1f cfs, max abs diff %.1f cfs; wrote %s",
            len(comp), comp["diff"].mean(), comp["diff"].abs().mean(), comp["diff"].abs().max(), comp_file,
        )
        if plot:
            import matplotlib.pyplot as plt

            fig, (ax0, ax1) = plt.subplots(2, sharex=True)
            comp[["processed", "hro"]].plot(drawstyle="steps-post", ax=ax0)
            comp["diff"].plot(ax=ax1, color="orange")
            ax1.set_ylabel("processed - hro (cfs)")
            plt.show()
    elif plot:
        logger.warning("--plot ignored without --compare")


if __name__ == "__main__":
    process_banks_cli()
