"""Read-only coverage plots for overlapping DES instrument files."""

from pathlib import Path

import click
import numpy as np
import pandas as pd

from dms_datastore import read_ts
from dms_datastore.filename import interpret_fname, naming_spec


def collect_coverage(directory, station, param, frequency="1d", include_series=False):
    """Read DES files independently, without merging overlapping instruments.

    Parameters
    ----------
    directory : str or pathlib.Path
        Directory containing raw files; subdirectories are not searched.
    station : str
        Station id, optionally qualified as ``station@subloc``. Without a
        sublocation, all sublocations are included in separate file lanes.
    param : str
        Parameter, optionally qualified with a modifier.
    frequency : str, default="1d"
        Coverage bin size. A bin is covered when any unmasked value is present.
    include_series : bool, default=False
        Retain reader-returned values for overlapping time-series plots.

    Returns
    -------
    list of dict
        Filename metadata, occupied bins, sample counts, and span relationships.
    """
    directory = Path(directory)
    if not directory.is_dir():
        raise ValueError(f"Not a directory: {directory}")
    offset = pd.tseries.frequencies.to_offset(frequency)
    if offset.nanos <= 0:
        raise ValueError("Coverage frequency must be positive and fixed-duration")
    station_id, separator, subloc = station.partition("@")
    parameter, _, modifier = param.partition("@")
    naming = naming_spec(repo="raw")
    records = []
    for path in sorted(directory.glob("des_*.csv")):
        meta = interpret_fname(path.name, naming=naming)
        if meta["station_id"] != station_id or meta["param"] != parameter:
            continue
        if separator and meta.get("subloc") != subloc:
            continue
        if (meta.get("modifier") or "") != modifier:
            continue
        data = read_ts(str(path))
        if isinstance(data, pd.Series):
            valid = data.notna()
        else:
            valid = data.drop(columns=["user_flag"], errors="ignore").notna().any(axis=1)
        timestamps = data.index[valid]
        bins = pd.DatetimeIndex(timestamps.floor(frequency).unique()).sort_values()
        records.append(
            dict(path=path, meta=meta, bins=bins, n_valid=int(valid.sum()),
                 start=timestamps.min(), end=timestamps.max(), superseded_by=[])
        )
        if include_series:
            records[-1]["data"] = data
    if not records:
        raise ValueError(f"No DES files found for {station} {param} in {directory}")
    for record in records:
        meta = record["meta"]
        for other in records:
            candidate = other["meta"]
            same_series = all(
                meta.get(key) == candidate.get(key)
                for key in ("source", "station_id", "subloc", "param")
            )
            contained = (
                candidate["syear"] <= meta["syear"]
                and candidate["eyear"] >= meta["eyear"]
            )
            if other is not record and same_series and contained:
                record["superseded_by"].append(other["path"].name)
    return records


def plot_coverage(records, station, param, frequency="1d"):
    """Plot filename spans and occupied bins in distinct, labeled file lanes.

    Parameters
    ----------
    records : list of dict
        Output of :func:`collect_coverage`.
    station, param : str
        Data-stream labels.
    frequency : str, default="1d"
        Bin size used by ``collect_coverage``.

    Returns
    -------
    matplotlib.figure.Figure
        Coverage figure, not displayed or saved by this function.
    """
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D

    figure, axis = plt.subplots(figsize=(14, max(3.5, 1.5 + 0.55 * len(records))))
    width = pd.Timedelta(frequency) / pd.Timedelta("1d")
    labels = []
    for lane, record in enumerate(records):
        meta = record["meta"]
        start = pd.Timestamp(year=int(meta["syear"]), month=1, day=1)
        end_year = int(meta["eyear"])
        end = (
            pd.Timestamp(year=end_year + 1, month=1, day=1)
            if end_year < 9999 else max(
                (item["end"] for item in records if pd.notna(item["end"])),
                default=start,
            ) + pd.Timedelta(frequency)
        )
        axis.hlines(lane, start, end, color="0.8", linewidth=9, zorder=1)
        bins = record["bins"]
        if len(bins):
            positions = mdates.date2num(bins.to_pydatetime())
            breaks = np.flatnonzero(np.diff(positions) > width * 1.01) + 1
            runs = np.split(positions, breaks)
            axis.broken_barh(
                [(run[0], run[-1] - run[0] + width) for run in runs],
                (lane - 0.18, 0.36), facecolors=[plt.get_cmap("tab20")(lane % 20)],
                zorder=2,
            )
        marker = " [contained/equal span]" if record["superseded_by"] else ""
        labels.append(record["path"].name + marker)
    axis.set_yticks(range(len(records)))
    axis.set_yticklabels(labels, fontsize=8)
    axis.invert_yaxis()
    locator = mdates.AutoDateLocator()
    axis.xaxis.set_major_locator(locator)
    axis.xaxis.set_major_formatter(mdates.ConciseDateFormatter(locator))
    axis.grid(axis="x", alpha=0.25)
    axis.set_title(f"DES coverage: {station} / {param} ({frequency} bins)")
    axis.legend(handles=[
        Line2D([], [], color="0.8", linewidth=7, label="Filename year span"),
        Line2D([], [], color="tab:blue", linewidth=5, label="At least one valid sample per bin"),
    ], loc="upper center", bbox_to_anchor=(0.5, -0.12), ncol=2)
    figure.tight_layout()
    return figure


def plot_series(records, station, param):
    """Plot unaggregated, provider-masked values on shared time/value axes.

    Parameters
    ----------
    records : list of dict
        Output of ``collect_coverage(..., include_series=True)``.
    station, param : str
        Data-stream labels.

    Returns
    -------
    matplotlib.figure.Figure
        Overlapping series figure, not displayed or saved.
    """
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt

    figure, axis = plt.subplots(figsize=(14, 6))
    styles = ("-", "--", "-.", ":")
    for number, record in enumerate(records):
        data = record["data"]
        if isinstance(data, pd.DataFrame):
            data = data.drop(columns=["user_flag"], errors="ignore").squeeze("columns")
        if not isinstance(data, pd.Series):
            raise ValueError(f"Expected one data column in {record['path'].name}")
        axis.plot(
            data.index, data.to_numpy(), label=record["path"].name,
            color=plt.get_cmap("tab20")(number % 20),
            linestyle=styles[number % len(styles)], linewidth=0.9,
            marker=".", markersize=2, alpha=0.75,
        )
    locator = mdates.AutoDateLocator()
    axis.xaxis.set_major_locator(locator)
    axis.xaxis.set_major_formatter(mdates.ConciseDateFormatter(locator))
    axis.set_ylabel(param)
    axis.set_title(f"DES overlapping series: {station} / {param}")
    axis.grid(alpha=0.25)
    axis.legend(loc="upper left", bbox_to_anchor=(1.01, 1), fontsize=8)
    figure.tight_layout()
    return figure


def des_view_overlap(directory, station, param, frequency="1d", series=False):
    """Read a DES stream and return its figure and per-file records.

    Parameters
    ----------
    directory : str or pathlib.Path
        Input directory; no files are modified.
    station, param : str
        Stream identity, with optional ``@subloc`` and ``@modifier`` suffixes.
    frequency : str, default="1d"
        Fixed-duration coverage bin size.
    series : bool, default=False
        Plot overlapping values instead of coverage lanes. Values returned by
        ``read_ts`` are not binned, jittered, or merged across files.

    Returns
    -------
    figure : matplotlib.figure.Figure
        Coverage or overlapping-series plot.
    records : list of dict
        Per-file coverage and filename containment information.
    """
    records = collect_coverage(directory, station, param, frequency, include_series=series)
    if series:
        return plot_series(records, station, param), records
    return plot_coverage(records, station, param, frequency), records


@click.command()
@click.help_option("-h", "--help")
@click.argument("directory", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option("--station", required=True, help="Station id, optionally station@subloc.")
@click.option("--param", required=True, help="Parameter, e.g. ec.")
@click.option("--frequency", default="1d", show_default=True, help="Coverage bin size, e.g. 1h or 1d.")
@click.option("--series", is_flag=True, help="Overlay unaggregated values instead of coverage lanes; frequency only affects coverage.")
@click.option("-o", "--output", type=click.Path(dir_okay=False, path_type=Path), help="Save plot instead of opening a window.")
def des_view_overlap_cli(directory, station, param, frequency, series, output):
    """View overlapping DES instrument coverage without changing raw files.

    Each file has a separate lane. Gray shows filename years; colors show
    occupied data bins. Partial overlaps remain under generic rationalization;
    contained/equal filename spans are supersession candidates. YAML overrides
    are not applied or simulated by this viewer.
    """
    import matplotlib

    if output:
        matplotlib.use("Agg")
    try:
        figure, records = des_view_overlap(directory, station, param, frequency, series=series)
    except (ValueError, OSError) as error:
        raise click.ClickException(str(error)) from error
    for record in records:
        click.echo(f"{record['path'].name}: {record['n_valid']} valid samples; "
                   f"{record['start']} to {record['end']}")
        for name in record["superseded_by"]:
            click.echo(f"  Filename span contained in/equal to: {name}")
    if output:
        figure.savefig(output, dpi=150, bbox_inches="tight")
        click.echo(f"Saved {output}")
    else:
        import matplotlib.pyplot as plt

        plt.show()


if __name__ == "__main__":
    des_view_overlap_cli()