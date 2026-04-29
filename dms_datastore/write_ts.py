# write_ts.py

#!/usr/bin/env python
# -*- coding: utf-8 -*-

import yaml
import pandas as pd
import warnings
import os
from pathlib import Path

__all__ = ["write_ts_csv"]


def _validate_metadata_format(metadata, format_version):
    if "format" not in metadata:
        raise ValueError("Metadata dict must contain 'format'")
    if format_version is not None and metadata["format"] != format_version:
        raise ValueError(
            f"format_version={format_version!r} does not match "
            f"metadata['format']={metadata['format']!r}"
        )


def chunk_bounds(ts, block_size):
    firstyr = ts.first_valid_index().year
    lastyr = ts.last_valid_index().year
    neat_lower_bound = int(block_size * (firstyr // block_size))
    neat_upper_bound = int(block_size * (lastyr // block_size))
    bounds = []
    for bound in range(neat_lower_bound, neat_upper_bound + 1, block_size):
        lo = max(firstyr, bound)
        hi = bound + block_size - 1
        bounds.append((lo, hi))
    return bounds


def block_comment(txt):
    text = txt.split("\n")
    text = ["# " + x for x in text]
    text = [x.replace("# #", "#  ") for x in text]
    return "\n".join(text)


def block_uncomment(txt):
    split = txt.split("\n")

    def uncomment(x):
        return x[1:] if len(x) > 0 and x[0] == "#" else x

    split = [uncomment(x) for x in split]
    return "\n".join(split)

def _sanitize_yaml_value(x):
    import numpy as np
    import pandas as pd

    if isinstance(x, dict):
        out = {}
        for k, v in x.items():
            key = str(k)

            if key == "original_header" and isinstance(v, str):
                # normalize trailing whitespace/newlines and force block style
                out[key] = LiteralStr(v.rstrip("\n"))
            else:
                out[key] = _sanitize_yaml_value(v)
        return out

    if isinstance(x, list):
        return [_sanitize_yaml_value(v) for v in x]

    if isinstance(x, tuple):
        return [_sanitize_yaml_value(v) for v in x]

    if isinstance(x, np.floating):
        return float(x)

    if isinstance(x, np.integer):
        return int(x)

    if isinstance(x, np.bool_):
        return bool(x)

    if isinstance(x, pd.Timestamp):
        return x.isoformat()

    if x is pd.NaT:
        return None

    return x


class LiteralStr(str):
    pass

def _literal_str_representer(dumper, data):
    return dumper.represent_scalar(
        "tag:yaml.org,2002:str",
        str(data),
        style="|",
    )


yaml.SafeDumper.add_representer(LiteralStr, _literal_str_representer)

def prep_header(metadata, format_version="dwr-dms-1.0"):
    """Prepares metadata in the form of a string or yaml data structure for inclusion
    Prep includes making sure that the lines are commented and start with the format: line
    """

    if isinstance(metadata, str):
        metadata = metadata.split("\n")

        # Detect if any format line exists in first few lines (not just line 0)
        has_format = any("format:" in line for line in metadata[:5])

        if not has_format:
            if metadata[0].startswith("#"):
                metadata = [
                    f"# format: {format_version}",
                    f"# date_formatted: {pd.Timestamp.now().strftime('%Y-%m-%dT%H:%M:%S')}",
                ] + metadata
            else:
                metadata = [
                    f"format: {format_version}",
                    f"date_formatted: {pd.Timestamp.now().strftime('%Y-%m-%dT%H:%M:%S')}",
                ] + metadata

        # Remove duplicate format lines (keep first occurrence)
        seen_format = False
        cleaned = []
        for line in metadata:
            if "format:" in line:
                if seen_format:
                    continue
                seen_format = True
            cleaned.append(line)
        metadata = cleaned

        # Ensure commented
        if not metadata[0].startswith("#"):
            metadata = ["# " + x for x in metadata]
            metadata = [x.replace("# #", "#") for x in metadata]

        header = "\n".join(metadata)

    else:  # yaml
        meta = _sanitize_yaml_value(metadata.copy())
        header_no_comment = yaml.dump(
            meta,
            Dumper=yaml.SafeDumper,
            sort_keys=False,
            allow_unicode=True,
        )
        header = block_comment(header_no_comment)

    if not header.endswith("\n"):
        header = header + "\n"

    return header


def _normalize_year_key(k):
    if isinstance(k, int):
        return k
    if isinstance(k, str) and k.isdigit() and len(k) == 4:
        return int(k)
    raise ValueError(f"Year-mapped metadata keys must be 4-digit years, got {k!r}")


def _is_year_metadata_map(metadata):
    """
    True only for mappings like {2004: {...}, 2005: {...}} or {'2004': {...}}.
    """
    if not isinstance(metadata, dict) or len(metadata) == 0:
        return False

    try:
        norm_keys = [_normalize_year_key(k) for k in metadata.keys()]
    except ValueError:
        return False

    # If all keys look like years, treat as year-mapped metadata.
    return True

def _prepare_single_metadata_header(metadata, format_version):
    if metadata is None:
        return (
            f"# format: {format_version}\n"
            f"# date_formatted: {pd.Timestamp.now().strftime('%Y-%m-%dT%H:%M:%S')}\n"
        )

    if isinstance(metadata, dict):
        if "format" in metadata:
            if format_version is not None and metadata["format"] != format_version:
                raise ValueError(
                    f"format_version={format_version!r} does not match "
                    f"metadata['format']={metadata['format']!r}"
                )
            meta = metadata.copy()
        else:
            if format_version is None:
                raise ValueError(
                    "Metadata dict must contain 'format' when format_version is None"
                )
            meta = {"format": format_version}
            meta.update(metadata)

        if "date_formatted" not in meta:
            meta["date_formatted"] = pd.Timestamp.now().strftime(
                "%Y-%m-%dT%H:%M:%S"
            )

        # Reorder so format and date_formatted are first
        ordered = {
            "format": meta["format"],
            "date_formatted": meta["date_formatted"],
        }
        for k, v in meta.items():
            if k not in ("format", "date_formatted"):
                ordered[k] = v

        return prep_header(ordered, format_version=format_version)

    return prep_header(metadata, format_version=format_version)


def _prepare_metadata_header_for_year(metadata, year, format_version):
    """
    Accept either:
      - one metadata payload for all shards
      - a year -> metadata mapping for chunked writes
    """
    if _is_year_metadata_map(metadata):
        normalized = {_normalize_year_key(k): v for k, v in metadata.items()}
        if year not in normalized:
            raise ValueError(
                f"Missing metadata for output year {year}. "
                f"Available years: {sorted(normalized.keys())}"
            )
        per_year = normalized[year]
        if not isinstance(per_year, (dict, str)):
            raise ValueError(
                f"Per-year metadata for {year} must be dict or str, got {type(per_year)}"
            )
        return _prepare_single_metadata_header(per_year, format_version)

    return _prepare_single_metadata_header(metadata, format_version)

def _shard_has_enough_data(tssub, min_points=16):
    """
    Decide whether a shard should actually be written.

    The policy is intentionally local to write_ts_csv so callers do not need
    to know the minimum-data rule.
    """
    count = tssub.count()
    if hasattr(count, "any"):
        return (count >= min_points).any()
    return count >= min_points


def _effective_chunk_bounds(ts, block_size, min_points=16):
    """
    Return only the chunk bounds that would actually produce output files.
    """
    bounds = chunk_bounds(ts, block_size=block_size)
    effective = []

    for bnd in bounds:
        s = max(pd.Timestamp(bnd[0], 1, 1), ts.first_valid_index())
        e = min(pd.Timestamp(bnd[1], 12, 31, 23, 59, 59), ts.last_valid_index())
        tssub = ts.loc[s:e]

        if _shard_has_enough_data(tssub, min_points=min_points):
            effective.append((bnd, s, e))

    return effective


def write_ts_csv(
    ts,
    fpath,
    metadata=None,
    chunk_years=False,
    format_version="dwr-dms-1.0",
    overwrite_conventions=False,
    block_size=1,
    dtypes={"user_flag": "Int64"},
    sep=",",
    **kwargs,
):
    """
    Write time series to a csv file following a standard format.

    metadata may be:
      - None
      - str
      - dict
      - year -> (dict or str) mapping, but only when chunk_years=True
    """
    if isinstance(ts, pd.Series):
        col_name = ts.name if ts.name is not None else "value"
        ts = ts.to_frame(name=col_name)

    former_index = ts.index.name
    if former_index != "datetime" and not overwrite_conventions:
        ts = ts.copy()
        ts.index.name = "datetime"

    if _is_year_metadata_map(metadata) and not chunk_years:
        raise ValueError("Year-mapped metadata is only valid when chunk_years=True")

    for dtype_col, dtype in dtypes.items():
        if dtype_col in ts.columns:
            ts[dtype_col] = ts[dtype_col].astype(dtype)

    if chunk_years:
        effective_bounds = _effective_chunk_bounds(ts, block_size=block_size)
        single_year_label = block_size == 1

        # Validate year coverage only for shards that would actually be written
        if _is_year_metadata_map(metadata):
            shard_years = [bnd[0] for (bnd, s, e) in effective_bounds]
            normalized = {_normalize_year_key(k): v for k, v in metadata.items()}
            missing = [yr for yr in shard_years if yr not in normalized]
            if missing:
                raise ValueError(
                    f"Metadata mapping does not cover all output shard years. Missing: {missing}"
                )

        for bnd, s, e in effective_bounds:
            tssub = ts.loc[s:e]

            new_date_range_str = f"{bnd[0]}_{bnd[1]}"
            if single_year_label:
                if bnd[0] != bnd[1]:
                    raise ValueError("Blocks not compatible with single_year")
                new_date_range_str = f"{bnd[0]}"

            newfname = fpath
            if f"_{new_date_range_str}" not in str(newfname):
                newfname = str(fpath).replace(".csv", "_" + new_date_range_str + ".csv")

            meta_header = _prepare_metadata_header_for_year(
                metadata,
                year=bnd[0],
                format_version=format_version,
            )

            with open(newfname, "w", newline="\n", encoding="utf-8") as outfile:
                outfile.write(meta_header)
                tssub.to_csv(
                    outfile,
                    header=True,
                    sep=sep,
                    date_format="%Y-%m-%dT%H:%M:%S",
                    lineterminator="\n",
                    **kwargs,
                )
    else:
        meta_header = _prepare_single_metadata_header(metadata, format_version)

        if isinstance(fpath, (str, bytes, os.PathLike)):
            outfile = open(fpath, "w", newline="\n",encoding="utf-8")
        else:
            outfile = fpath
        outfile.write(meta_header)
        ts.to_csv(
            outfile,
            header=True,
            sep=sep,
            date_format="%Y-%m-%dT%H:%M:%S",
            lineterminator="\n",
            **kwargs,
        )