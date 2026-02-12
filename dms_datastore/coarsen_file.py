from __future__ import annotations

import click
import pandas as pd

from vtools.functions.coarsen import ts_coarsen


@click.command("coarsen_ts")
@click.argument(
    "input_csv",
    type=click.Path(exists=True, dir_okay=False, readable=True),
)
@click.argument(
    "output_csv",
    type=click.Path(dir_okay=False, writable=True),
)
@click.option(
    "--datetime-col",
    default="datetime",
    show_default=True,
    help="Name of datetime column in input CSV.",
)
@click.option(
    "--grid",
    default="1min",
    show_default=True,
    help="Resampling grid (pandas offset alias). Use 'none' to disable.",
)
@click.option(
    "--qwidth",
    type=float,
    default=None,
    help="Post-grid quantization width. Omit to disable quantization.",
)
@click.option(
    "--use-original-vals",
    default=True,
    show_default=True,
    help="Emit raw gridded values (default) or quantized values.",
)
@click.option(
    "--heartbeat-freq",
    default="120min",
    show_default=True,
    help="Heartbeat frequency. Use 'none' to disable.",
)
@click.option(
    "--preserve-val",
    multiple=True,
    type=float,
    help="Semantic values to preserve (repeatable, e.g. --preserve-val 0.0).",
)
@click.option(
    "--preserve-eps",
    type=float,
    default=None,
    help="Tolerance around preserve values. Defaults to 0.5*qwidth or 0.",
)
@click.option(
    "--preserve-enter-dwell",
    default="2min",
    show_default=True,
    help="Minimum dwell time to enter a preserved state.",
)
@click.option(
    "--preserve-exit-dwell",
    default="30s",
    show_default=True,
    help="Maximum gap allowed inside a preserved state.",
)
@click.option(
    "--hyst",
    type=float,
    default=1.0,
    show_default=True,
    help="Hysteresis factor for quantization.",
)
@click.option(
    "--float-format",
    default="%.6g",
    show_default=True,
    help="Float format for output CSV.",
)
def coarsen_ts_cli(
    input_csv,
    output_csv,
    datetime_col,
    grid,
    qwidth,
    use_original_vals,
    heartbeat_freq,
    preserve_val,
    preserve_eps,
    preserve_enter_dwell,
    preserve_exit_dwell,
    hyst,
    float_format,
):
    """
    Coarsen a time series CSV by gridding, preserving semantic states,
    quantization, and thinning.
    """

    # ----------------------------
    # Read
    # ----------------------------
    print(f"Reading input CSV: {input_csv}")
    df = pd.read_csv(input_csv, 
                     parse_dates=[datetime_col],
                     index_col=0,
                     comment="#",
                     sep=",",
                     header=0)

    #if datetime_col not in df.columns:
    #    raise click.UsageError(f"Datetime column '{datetime_col}' not found.")

    #df[datetime_col] = pd.to_datetime(df[datetime_col])
    #df = df.set_index(datetime_col)

    # ----------------------------
    # Normalize CLI "none"
    # ----------------------------
    grid = None if grid.lower() == "none" else grid
    heartbeat_freq = None if heartbeat_freq.lower() == "none" else heartbeat_freq

    preserve_vals = tuple(preserve_val)

    # ----------------------------
    # Coarsen
    # ----------------------------
    out = ts_coarsen(
        df,
        grid=grid,
        qwidth=qwidth,
        use_original_vals=use_original_vals,
        heartbeat_freq=heartbeat_freq,
        preserve_vals=preserve_vals,
        preserve_eps=preserve_eps,
        preserve_enter_dwell=preserve_enter_dwell,
        preserve_exit_dwell=preserve_exit_dwell,
        hyst=hyst,
    )

    # ----------------------------
    # Write
    # ----------------------------
    out = out.reset_index()
    out.to_csv(
        output_csv,
        index=False,
        float_format=float_format,
    )
