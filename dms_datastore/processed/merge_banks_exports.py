#!/usr/bin/env python
"""Merge SWP Banks pumping plant ``EXPORT_*.csv`` change logs into the consolidated file.

The exports come from SAP as spreadsheets (``EXPORT_yyyymmdd_hhmmss.XLSX``) and are
assumed to have been saved manually as CSV first, so no Excel dependency is needed.
Each export is a change log: the first entries hold a row for every unit and
afterwards only changes are listed.  (Date, Time, Unit) is the sort key but it is
*not* unique (a correction can land in the same minute for the same unit, with the
superseded row flagged ``Exclude=X``).  The sort is stable, so rows that tie keep
their existing order: existing rows are never reordered relative to each other, and
new rows follow the existing rows in a tie.

The consolidated file (``sap_banks_2006_9999_qaqc.csv``) is the source of truth,
including hand-entered ``Exclude`` flags and ``Notes``.  An export row whose
(Date, Time, Unit) already appears in the consolidated file is skipped, whatever its
``Exclude`` value.  New rows are appended and the whole file is stably sorted by
Date, Time, Unit.

All fields are handled as text so values such as ``0000000000`` in ``CFS`` survive.
Times are Pacific local clock time with no DST correction and the output starts with the line
``# Time zone: PDT``.  Readers must skip leading ``#`` lines rather than use
``comment='#'``, because Notes may contain ``#``.
"""
import glob
import logging
import os
from pathlib import Path

import click
import pandas as pd

from dms_datastore.logging_config import configure_logging, resolve_loglevel

logger = logging.getLogger(__name__)

COLUMNS = [
    "Time",
    "Field Div.",
    "Plant",
    "Unit",
    "Status",
    "Date",
    "AVR",
    "PSS",
    "MWs",
    "CFS",
    "Notes",
    "Exclude",
]
KEY = ["Date", "Time", "Unit"]
TZ_COMMENT = "# Time zone: PDT"
MIN_EXPORT_ROWS = 60  # a shorter export is probably truncated


def _read_text_csv(path):
    """Read a CSV as all-text, skipping leading ``#`` comment lines."""
    path = Path(path)
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        nskip = 0
        for line in f:
            if not line.startswith("#"):
                break
            nskip += 1
    return pd.read_csv(
        path,
        dtype=str,
        keep_default_na=False,
        skiprows=nskip,
        encoding="utf-8-sig",
    )


def _normalize(df, source):
    """Select the consolidated columns and normalize Time/Date text.

    The first column is the time; the header of the first column in an export can
    be garbled (e.g. ``If``) so it is taken by position.
    """
    df = df.copy()
    df.columns = ["Time"] + [c.strip() for c in df.columns[1:]]
    missing = [c for c in COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"{source}: missing columns {missing}")
    df = df[COLUMNS]
    df["Time"] = df["Time"].str.strip().str.zfill(4)
    bad_time = ~df["Time"].str.fullmatch(r"\d{4}")
    if bad_time.any():
        raise ValueError(f"{source}: bad Time values {df.loc[bad_time, 'Time'].unique()[:10]}")
    df["Date"] = df["Date"].str.strip()
    df["Unit"] = df["Unit"].str.strip()
    return df.reset_index(drop=True)


def _sort_keys(df, source):
    """Return (datetime, unit number) series used for the stable sort."""
    date = pd.to_datetime(df["Date"], format="%m/%d/%Y", errors="coerce")
    if date.isna().any():
        raise ValueError(f"{source}: bad Date values {df.loc[date.isna(), 'Date'].unique()[:10]}")
    hh = df["Time"].str.slice(0, 2).astype(int)
    mm = df["Time"].str.slice(2, 4).astype(int)
    if ((hh > 24) | (mm > 59) | ((hh == 24) & (mm > 0))).any():
        raise ValueError(f"{source}: Time values out of range")
    dt = date + pd.to_timedelta(hh * 60 + mm, unit="min")
    unit = pd.to_numeric(df["Unit"].str.extract(r"(\d+)\s*$")[0], errors="coerce")
    if unit.isna().any():
        raise ValueError(f"{source}: bad Unit values {df.loc[unit.isna(), 'Unit'].unique()[:10]}")
    return dt, unit


def merge_exports(archive_df, export_dfs):
    """Append export rows not already present, then stably sort.

    Parameters
    ----------
    archive_df : DataFrame
        Normalized consolidated data.
    export_dfs : list of (name, DataFrame)
        Normalized exports, processed in order.

    Returns
    -------
    merged : DataFrame
    added : dict
        Number of rows added from each export.
    """
    merged = archive_df
    added = {}
    for name, edf in export_dfs:
        if len(edf) < MIN_EXPORT_ROWS:
            logger.warning(
                "%s has only %d rows (< %d); the export may be incomplete",
                name,
                len(edf),
                MIN_EXPORT_ROWS,
            )
        have = set(map(tuple, merged[KEY].itertuples(index=False, name=None)))
        keep = [tuple(r) not in have for r in edf[KEY].itertuples(index=False, name=None)]
        new = edf[keep]
        added[name] = len(new)
        logger.info("%s: %d rows, %d new, %d already present", name, len(edf), len(new), len(edf) - len(new))
        merged = pd.concat([merged, new], ignore_index=True)

    dt, unit = _sort_keys(merged, "merged data")
    order = pd.DataFrame({"dt": dt, "unit": unit}).sort_values(["dt", "unit"], kind="stable").index
    return merged.loc[order].reset_index(drop=True), added


def write_consolidated(df, path):
    """Write the consolidated file atomically with the time zone comment line."""
    path = Path(path)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        f.write(TZ_COMMENT + "\n")
        df.to_csv(f, index=False, lineterminator="\n")
    os.replace(tmp, path)


def merge_banks_exports(archive, exports, outfile=None, dry_run=False):
    """Merge export CSV files into the consolidated archive CSV.

    Returns the merged DataFrame.
    """
    archive = Path(archive)
    outfile = Path(outfile) if outfile else archive
    archive_df = _normalize(_read_text_csv(archive), str(archive))
    export_dfs = [(Path(e).name, _normalize(_read_text_csv(e), str(e))) for e in exports]
    for name, edf in export_dfs:
        _sort_keys(edf, name)  # validate early

    merged, added = merge_exports(archive_df, export_dfs)
    logger.info(
        "Consolidated rows: %d -> %d (%d added)", len(archive_df), len(merged), len(merged) - len(archive_df)
    )
    if dry_run:
        logger.info("Dry run: %s not written", outfile)
    else:
        write_consolidated(merged, outfile)
        logger.info("Wrote %s", outfile)
    return merged


def _expand(patterns):
    files = []
    for p in patterns:
        hits = sorted(glob.glob(p)) or [p]
        files.extend(hits)
    return files


@click.command("merge_banks_exports")
@click.argument("exports", nargs=-1, required=True)
@click.option("--archive", type=click.Path(exists=True, dir_okay=False, path_type=Path), required=True,
              help="Consolidated CSV, e.g. sap_banks_2006_9999_qaqc.csv.")
@click.option("--outfile", type=click.Path(dir_okay=False, path_type=Path), default=None,
              help="Output path. Defaults to overwriting --archive.")
@click.option("--dry-run", is_flag=True, help="Report what would change without writing.")
@click.option("--logdir", type=click.Path(path_type=Path), default="logs",
              help="Directory for log files.")
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.option("--quiet", is_flag=True, help="Suppress console output.")
@click.help_option("-h", "--help")
def merge_banks_exports_cli(exports, archive, outfile, dry_run, logdir, debug, quiet):
    """Merge SWP Banks EXPORT_*.csv change logs into the consolidated CSV.

    EXPORTS are CSV paths or glob patterns, processed in the order given.
    """
    level, console = resolve_loglevel(debug=debug, quiet=quiet)
    configure_logging(
        package_name="dms_datastore",
        level=level,
        console=console,
        logdir=logdir,
        logfile_prefix="merge_banks_exports",
    )
    merge_banks_exports(archive, _expand(exports), outfile=outfile, dry_run=dry_run)


if __name__ == "__main__":
    merge_banks_exports_cli()
