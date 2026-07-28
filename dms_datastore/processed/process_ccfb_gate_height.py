#!/usr/bin/env python
"""Reduce CCFB five-gate radial heights to the processed (ndup, height) product.

The upstream steps (Wonderware retrieval, DST correction, coarsening and
formatting) now happen elsewhere and produce the formatted five-gate product in
the ``structures_formatted`` repo, e.g.
``//cnrastore-bdo/Modeling_Data/repo/structures/formatted/dms_ccfb@radial_height.csv``
with columns ``gate_1 .. gate_5``.

This step reads that product and reduces the per-gate heights to two columns:

* ``ndup``   -- number of gates open above ``thresh_open``
* ``height`` -- mean height of the open gates

Units remain **feet** (no unit conversion is applied here).  The output is
written as a DMS-format CSV via :func:`write_ts_csv` and belongs in the
``structures_processed`` structures repo (moved there separately, e.g. via dropbox).
"""
import logging
from pathlib import Path

import click
import pandas as pd

from dms_datastore.read_multi import read_ts_repo
from dms_datastore.write_ts import write_ts_csv
from dms_datastore.logging_config import configure_logging, resolve_loglevel

logger = logging.getLogger(__name__)

# Threshold (ft) above which a gate is considered in use; filters noise.
THRESH_OPEN = 0.03


def reduce_gate_height(gates, thresh_open=THRESH_OPEN):
    """Reduce a five-gate height frame to ``ndup`` and mean ``height`` (feet).

    Parameters
    ----------
    gates : pandas.DataFrame
        Frame of per-gate heights (columns ``gate_1 .. gate_5``) in feet.
    thresh_open : float, optional
        Height (ft) above which a gate is counted as open.

    Returns
    -------
    pandas.DataFrame
        Frame with columns ``ndup`` (Int64), ``height`` (float, feet), and an
        empty ``comment`` column matching the processed-repo schema.
    """
    height_sum = gates.sum(axis=1)  # sum before ndup is computed
    ndup = (gates > thresh_open).sum(axis=1)
    height = height_sum.divide(ndup, axis="index", fill_value=0.0).round(3)
    df = pd.DataFrame({"ndup": ndup.astype("Int64"), "height": height})
    df.loc[df.ndup == 0, "height"] = 0.0
    df["comment"] = pd.NA
    df.index.name = "datetime"
    return df


def process_ccfb_gate_height(start=None, end=None, thresh_open=THRESH_OPEN):
    """Read the formatted five-gate product and reduce it to ndup/height.

    Returns
    -------
    tuple
        ``(reduced_dataframe, metadata_dict)`` where the metadata is carried
        forward from the source formatted file so units and station identity
        are preserved.
    """
    meta_list, gates = read_ts_repo(
        "ccfb",
        "height",
        subloc="radial",
        repo="structures_formatted",
        force_regular=False,
        start=start,
        end=end,
        meta=True,
    )
    reduced = reduce_gate_height(gates, thresh_open=thresh_open)
    metadata = dict(meta_list[0]) if meta_list else {}
    return reduced, metadata


@click.command("process_ccfb_gate_height")
@click.option("--outfile", type=click.Path(path_type=Path),
              default="dms_ccfb@radial_height.csv", show_default=True,
              help="Output CSV path for the processed ndup/height product.")
@click.option("--start", type=str, default=None,
              help="Optional inclusive start time.")
@click.option("--end", type=str, default=None,
              help="Optional inclusive end time.")
@click.option("--thresh-open", type=float, default=THRESH_OPEN, show_default=True,
              help="Height (ft) above which a gate is counted as open.")
@click.option("--logdir", type=click.Path(path_type=Path), default="logs",
              help="Directory for log files.")
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.option("--quiet", is_flag=True, help="Suppress console output.")
@click.help_option("-h", "--help")
def process_ccfb_gate_height_cli(outfile, start, end, thresh_open, logdir, debug, quiet):
    """Reduce the CCFB five-gate radial heights to the processed ndup/height CSV.

    Reads the formatted five-gate product from the ``structures_formatted`` repo
    and writes ``datetime,ndup,height`` (heights in feet) as a DMS-format CSV.
    """
    level, console = resolve_loglevel(debug=debug, quiet=quiet)
    configure_logging(
        package_name="dms_datastore",
        level=level,
        console=console,
        logdir=logdir,
        logfile_prefix="process_ccfb_gate_height",
    )
    reduced, metadata = process_ccfb_gate_height(
        start=pd.Timestamp(start) if start else None,
        end=pd.Timestamp(end) if end else None,
        thresh_open=thresh_open,
    )
    logger.info("Reduced %d rows; last time %s", len(reduced), reduced.last_valid_index())
    write_ts_csv(reduced, str(outfile), metadata=metadata)
    logger.info("Wrote %s", outfile)


if __name__ == "__main__":
    process_ccfb_gate_height_cli()
