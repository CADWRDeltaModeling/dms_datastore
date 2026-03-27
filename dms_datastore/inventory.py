#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import glob
import click
import pandas as pd

from dms_datastore.filename import interpret_fname
from dms_datastore.dstore_config import coerce_repo_config, repo_registry
from dms_datastore.read_ts import read_yaml_header

__all__ = ["repo_file_inventory", "repo_data_inventory"]


def to_wildcard(fname, remove_source=False):
    """
    Convert a concrete filename into a year-wildcarded pattern.

    If remove_source is True, also wildcard the leading source slot.
    """
    pat1 = r".*_(\d{4}_\d{4})\.\S{3}$"
    re1 = re.compile(pat1)
    if re1.match(fname):
        out = fname[:-13] + "*" + fname[-4:]
    else:
        pat2 = r".*(_\d{4})\.\S{3}$"
        re2 = re.compile(pat2)
        if re2.match(fname):
            out = fname[:-8] + "*" + fname[-4:]
        else:
            raise ValueError(f"Filename does not match expected shard pattern: {fname}")

    if remove_source:
        outparts = out.split("_")
        if not outparts:
            raise ValueError(f"Could not split filename into source slots: {fname}")
        outparts[0] = "*"
        out = "_".join(outparts)

    return out


def scrape_header_metadata(fname):
    yml = read_yaml_header(fname)
    if yml is None:
        return None
    return yml["unit"] if "unit" in yml else None


def _inventory_files(root):
    return glob.glob(os.path.join(root, "*_*.rdb")) + glob.glob(
        os.path.join(root, "*_*.csv")
    )


def _parse_inventory_meta(allfiles):
    return [interpret_fname(fname) for fname in allfiles]


def series_id_from_meta(meta, remove_source=False):
    """
    Construct a stable logical series identifier from parsed metadata.

    For file inventory, include source.
    For data inventory, omit source.
    """
    parts = []

    if not remove_source:
        parts.append(str(meta["agency"]))

    parts.append(str(meta["station_id"]))

    subloc = meta.get("subloc")
    if subloc not in (None, "default"):
        parts.append(str(subloc))

    parts.append(str(meta["param"]))

    modifier = meta.get("modifier")
    if modifier is not None:
        parts.append(str(modifier))

    return "|".join(parts)


def _drop_inventory_noise(df):
    return df.drop(
        columns=[
            "notes",
            "stage",
            "flow",
            "quality",
            "wdl_id",
            "cdec_id",
            "d1641_id",
            "original_filename",
        ],
        errors="ignore",
    )


def repo_file_inventory(repo=None, *, repo_cfg=None, in_path=None):
    """
    Inventory of source-bearing file families in a configured repo.

    Key/index semantics:
        file_pattern is the grouping key.
        series_id is a dependent logical identifier.
    """
    repo_cfg = coerce_repo_config(repo=repo, repo_cfg=repo_cfg)
    root = in_path if in_path is not None else repo_cfg["root"]
    registry = repo_registry(repo_cfg=repo_cfg)

    allfiles = _inventory_files(root)
    allmeta = _parse_inventory_meta(allfiles)
    metadf = pd.DataFrame(allmeta)
    if metadf.empty:
        raise ValueError("Empty inventory")

    metadf["original_filename"] = metadf["filename"]
    metadf["file_pattern"] = metadf["filename"].map(
        lambda x: to_wildcard(x, remove_source=False)
    )
    metadf["series_id"] = metadf.apply(
        lambda row: series_id_from_meta(row, remove_source=False),
        axis=1,
    )

    keep_cols = ["station_id", "subloc", "param", "agency", "agency_id", "series_id"]
    if "modifier" in metadf.columns:
        keep_cols.append("modifier")

    agg = {col: "first" for col in keep_cols}
    agg["original_filename"] = "first"

    if "syear" in metadf.columns and "eyear" in metadf.columns:
        agg["syear"] = "min"
        agg["eyear"] = "max"
    elif "year" in metadf.columns:
        agg["year"] = ["min", "max"]
    else:
        raise ValueError("No year columns found in parsed inventory metadata")

    grouped = metadf.groupby("file_pattern", dropna=False).agg(agg)

    grouped.columns = [
        col if isinstance(col, str) else "_".join(str(x) for x in col if x)
        for col in grouped.columns
    ]
    grouped.columns = [
    c[:-6] if isinstance(c, str) and c.endswith("_first") else c
    for c in grouped.columns
       ]

    rename_map = {
        "agency": "source",
        "year_min": "min_year",
        "year_max": "max_year",
        "syear": "min_year",
        "eyear": "max_year",
    }
    grouped = grouped.rename(columns=rename_map)

    join_key = repo_cfg.get("key_column", "id")

    if join_key not in grouped.columns:
        if join_key == "id" and "station_id" in grouped.columns:
            grouped[join_key] = grouped["station_id"]
        else:
            raise ValueError(
                f"Cannot join registry: grouped inventory missing key column {join_key!r}"
            )

    metastat = grouped.join(
        registry,
        on=join_key,
        rsuffix="_registry",
        how="left",
    )

    if "year" in metadf.columns and "syear" not in metadf.columns:
        metastat["unit"] = metastat.apply(
            lambda x: scrape_header_metadata(os.path.join(root, x.original_filename)),
            axis=1,
        )
    else:
        metastat["unit"] = None

    metastat = _drop_inventory_noise(metastat)
    return metastat


def repo_data_inventory(repo=None, *, repo_cfg=None, in_path=None):
    """
    Inventory of logical datasets in a configured repo.

    Key/index semantics:
        series_id is the grouping key.
        file_pattern is a dependent wildcard pattern for downstream tools.
    """
    repo_cfg = coerce_repo_config(repo=repo, repo_cfg=repo_cfg)
    root = in_path if in_path is not None else repo_cfg["root"]
    registry = repo_registry(repo_cfg=repo_cfg)

    allfiles = _inventory_files(root)
    allmeta = _parse_inventory_meta(allfiles)
    metadf = pd.DataFrame(allmeta)
    if metadf.empty:
        raise ValueError("Empty inventory")

    metadf["original_filename"] = metadf["filename"]
    metadf["file_pattern"] = metadf["filename"].map(
        lambda x: to_wildcard(x, remove_source=True)
    )
    metadf["source"] = metadf["agency"]
    metadf["series_id"] = metadf.apply(
        lambda row: series_id_from_meta(row, remove_source=True),
        axis=1,
    )

    group_cols = ["series_id"]

    agg = {
        "station_id": "first",
        "subloc": "first",
        "param": "first",
        "agency": "first",
        "agency_id": "first",
        "source": "first",
        "file_pattern": "first",
        "original_filename": "first",
    }
    if "modifier" in metadf.columns:
        agg["modifier"] = "first"

    if "syear" in metadf.columns and "eyear" in metadf.columns:
        agg["syear"] = "min"
        agg["eyear"] = "max"
    elif "year" in metadf.columns:
        agg["year"] = ["min", "max"]
    else:
        raise ValueError("No year columns found in parsed inventory metadata")

    grouped = metadf.groupby(group_cols, dropna=False).agg(agg)

    grouped.columns = [
        col if isinstance(col, str) else "_".join(str(x) for x in col if x)
        for col in grouped.columns
    ]

    grouped.columns = [
      c[:-6] if isinstance(c, str) and c.endswith("_first") else c
      for c in grouped.columns
    ]

    rename_map = {
        "year_min": "min_year",
        "year_max": "max_year",
        "syear": "min_year",
        "eyear": "max_year",
    }
    grouped = grouped.rename(columns=rename_map)
    join_key = repo_cfg.get("key_column", "id")

    if join_key not in grouped.columns:
        if join_key == "id" and "station_id" in grouped.columns:
            grouped[join_key] = grouped["station_id"]
        else:
            raise ValueError(
                f"Cannot join registry: grouped inventory missing key column {join_key!r}"
            )

    metastat = grouped.join(
        registry,
        on=join_key,
        rsuffix="_registry",
        how="left",
    )
    if "year" in metadf.columns and "syear" not in metadf.columns:
        metastat["unit"] = metastat.apply(
            lambda x: scrape_header_metadata(os.path.join(root, x.original_filename)),
            axis=1,
        )
    else:
        metastat["unit"] = None

    metastat = _drop_inventory_noise(metastat)
    return metastat


def inventory(repo, out_files=None, out_data=None, in_path=None):
    nowstr = pd.Timestamp.now().strftime("%Y%m%d")

    if out_files is None:
        out_files = f"./inventory_files_{repo}_{nowstr}.csv"
    inv = repo_file_inventory(repo, in_path=in_path)
    inv.to_csv(out_files)

    if out_data is None:
        out_data = f"./inventory_datasets_{repo}_{nowstr}.csv"
    inv2 = repo_data_inventory(repo, in_path=in_path)
    inv2.to_csv(out_data)


@click.command()
@click.option(
    "--repo",
    type=str,
    required=True,
    help="Configured repo name to inventory at least for configuration. If in-path is not provided, the cofigured root dir of repo also will be used as scan directory.",
)
@click.option(
    "--in-path",
    default=None,
    help="Optional directory to scan instead of the configured repo root.",
)
@click.option(
    "--out-files",
    default=None,
    help="Output path for file inventory.",
)
@click.option(
    "--out-data",
    default=None,
    help="Output path for data inventory.",
)
def inventory_cli(repo, out_files, out_data, in_path):
    inventory(repo, out_files, out_data, in_path)



if __name__ == "__main__":
    inventory_cli()