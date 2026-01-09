#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
CLI utility to merge or splice time series files using ts_merge or ts_splice.

This script uses click to parse command-line arguments, processes a list of file glob
patterns (in the order provided), and applies a merge or splice operation on the files
matching those patterns.

The files are assumed to be CSV files containing time series data with the time index in
the first column. Only the file basename (not the directory) is used for ordering.

WARNING: The --order order is applied both to the file glob ordering and any internal time
sharding. Therefore, order matters!

Usage Example:
    merge_files.py --merge-type splice --order last --pattern "moke_*" "ebmud_moke_*" [--output merged.csv]
"""

import click
import glob
import os
import pandas as pd
from vtools import ts_merge, ts_splice


def load_file(file_path):
    """
    Load a CSV file into a pandas DataFrame.

    This function assumes the CSV file contains a time series with the time index in the first column.
    Adjust the parameters (index_col, parse_dates) if your file format differs.

    Parameters
    ----------
    file_path : str
        The path to the CSV file.

    Returns
    -------
    pandas.DataFrame
        DataFrame containing the time series data.
    """
    try:
        df = pd.read_csv(file_path, index_col=0, parse_dates=True)
        return df
    except Exception as e:
        raise RuntimeError(f"Error loading {file_path}: {e}")


def merge_files(merge_type, order, names, patterns):
    """
    Merge or splice time series files based on the provided glob patterns.

    Parameters
    ----------
    merge_type : str
        Merging strategy to use: 'merge' to fill missing values (ts_merge) or 'splice'
        to stitch time series without overlap (ts_splice).
    order : str
        File ordering orderence: 'first' or 'last'. This determines which files take precedence
        when overlapping data exists. It affects both the file ordering and the internal time sharding.
    patterns : list of str
        List of file glob patterns. Patterns are processed in the order provided, and ordering is
        determined solely based on the file's basename.


    Returns
    -------
    pandas.DataFrame or pandas.Series
        The merged or spliced time series.
    """
    file_entries = []
    # Process each glob pattern in the order provided.
    for pattern_index, pattern in enumerate(patterns):
        files = glob.glob(pattern)
        for file in files:
            # Record each file with its basename, the index of the pattern, and full file path.
            file_entries.append((os.path.basename(file), pattern_index, file))

    if not file_entries:
        raise ValueError("No files found matching the provided patterns.")

    # Sort files based on basename and pattern order.
    # For '--order first': lower pattern index has higher priority.
    # For '--order last': higher pattern index has higher priority.
    reverse_pattern = (order == "last")
    file_entries.sort(key=lambda x: (x[0], x[1] if not reverse_pattern else -x[1]))

    # Extract the sorted file paths.
    file_list = [entry[2] for entry in file_entries]

    # For ts_merge, if '--order last' is specified, reverse file order so that later files override earlier ones.
    if merge_type == "merge" and order == "last":
        file_list = list(reversed(file_list))

    # Load each file as a pandas DataFrame.
    series = []
    for file in file_list:
        df = load_file(file)
        series.append(df)

    # Perform merge or splice based on the merge_type.
    if merge_type == "merge":
        merged = ts_merge(series,names=names)
    elif merge_type == "splice":
        # Pass transition parameter as "order_first" or "order_last" to ts_splice.
        transition = "prefer_" + order
        merged = ts_splice(series,names=names,transition=transition)
    else:
        raise ValueError("Invalid merge_type. Must be 'merge' or 'splice'.")

    return merged


@click.command()
@click.option(
    '--merge-type',
    'merge_type',
    type=click.Choice(['merge', 'splice']),
    required=True,
    help="Merging strategy: 'merge' fills missing values (ts_merge) while 'splice' stitches time series (ts_splice)."
)
@click.option(
    '--order',
    type=click.Choice(['first', 'last']),
    default='last',
    help="File ordering preference: 'first' prioritizes earlier files; 'last' prioritizes later files. Affects both file ordering and time sharding."
)
@click.option(
    '--pattern',
    multiple=True,
    required=True,
    help="List of file glob patterns to match files. Patterns are processed in order, and only the file basename is used for ordering."
)
@click.option(
    '--names',
    default=None,
    help="Names argument to select or rename columns. See the vtools documentation on ts_merge and ts_splice."
)
@click.option(
    '--output',
    default=None,
    help="Optional output CSV file to save the merged result. If not provided, the result is printed to stdout."
)
def merge_files_cli(merge_type, order, pattern, names, output):
    """
    CLI for merging or splicing time series files using ts_merge or ts_splice.
    """

    try:
        result = merge_files(merge_type, order, names, list(pattern))
        # Modify the read/write operations here as needed.
        if output:
            result.to_csv(output)
            print(f"Merged result saved to {output}")
        else:
            print(result)
    except Exception as e:
        print(f"Error during merging: {e}")


if __name__ == "__main__":
    merge_files_cli()
