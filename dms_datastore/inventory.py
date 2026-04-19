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


def to_wildcard(fname, remove_provider=False):
    """
    Convert a concrete filename into a year-wildcarded pattern.

    If remove_provider is True, also wildcard the leading provider slot.
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

    if remove_provider:
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


def _parse_inventory_meta(allfiles, repo_cfg=None):
    return [interpret_fname(fname, repo_cfg=repo_cfg) for fname in allfiles]


def series_id_from_meta(meta, repo_cfg=None, remove_provider=False):
    """
    Construct a stable logical series identifier from parsed metadata.

    Parameters
    ----------
    meta : dict-like
        Parsed filename metadata or row-like object.
    repo_cfg : dict or None
        Repo configuration. When provided, identity is driven by repo_cfg["data_key"].
        When omitted, a legacy station-style fallback is used.
    remove_provider : bool
        If True, omit provider from identity.

    Returns
    -------
    str
        series_id
    """
    if repo_cfg is None:
        provider_key = "agency"
        data_key = ["station_id", "subloc", "param"]
    else:
        provider_key = repo_cfg["provider_key"]
        data_key = repo_cfg["data_key"]  # let missing data_key fail naturally for now

    parts = []

    if not remove_provider:
        provider_val = meta.get(provider_key)
        if provider_val is None:
            raise ValueError(
                f"Missing provider field {provider_key!r} in metadata: {meta}"
            )
        parts.append(str(provider_val))

    for key in data_key:
        val = meta.get(key)

        # suppress conventional empty sublocation/modifier noise
        if key in ("subloc", "modifier") and val in (None, "", "default", "none"):
            continue

        if val is None:
            raise ValueError(
                f"Missing data_key field {key!r} in metadata: {meta}"
            )

        parts.append(str(val))

    return "|".join(parts)

def repo_data_inventory(repo=None, *, repo_cfg=None, in_path=None, registry=None):
    """
    Inventory of logical datasets in a configured repo.

    Key/index semantics:
        series_id is the grouping key.
        file_pattern is a dependent wildcard pattern for downstream tools.
    """
    repo_cfg = coerce_repo_config(repo=repo, repo_cfg=repo_cfg)
    root = in_path if in_path is not None else repo_cfg["root"]
    registry = repo_registry(repo_cfg=repo_cfg)

    site_key = repo_cfg["site_key"]
    provider_key = repo_cfg["provider_key"]

    allfiles = _inventory_files(root)
    allmeta = _parse_inventory_meta(allfiles, repo_cfg=repo_cfg)
    metadf = pd.DataFrame(allmeta)

    if metadf.empty:
        raise ValueError("Empty inventory")

    metadf["original_filename"] = metadf["filename"]
    metadf["file_pattern"] = metadf["filename"].map(
        lambda x: to_wildcard(x, remove_provider=True)
    )

    metadf["series_id"] = metadf.apply(
        lambda row: series_id_from_meta(row, repo_cfg, remove_provider=True),
        axis=1,
    )

    agg = {
        site_key: "first",
        "subloc": "first",
        "param": "first",
        provider_key: "first",
        "file_pattern": "first",
        "original_filename": "first",
    }
    if "agency_id" in metadf.columns:
        agg["agency_id"] = "first"
    if "modifier" in metadf.columns:
        agg["modifier"] = "first"

    if "syear" in metadf.columns and "eyear" in metadf.columns:
        agg["syear"] = "min"
        agg["eyear"] = "max"
    elif "year" in metadf.columns:
        agg["year"] = ["min", "max"]
    else:
        raise ValueError("No year columns found in parsed inventory metadata")

    grouped = metadf.groupby(["series_id"], dropna=False).agg(agg)

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

    if site_key not in grouped.columns:
        raise ValueError(
            f"Cannot join registry: grouped inventory missing site key {site_key!r}"
        )

    grouped[site_key] = grouped[site_key].astype(str).str.strip()

    if registry.index.name != site_key:
        if site_key not in registry.columns:
            raise ValueError(
                f"Registry missing join key column {site_key!r}; "
                f"columns are {registry.columns.tolist()}"
            )
        registry = registry.copy()
        registry[site_key] = registry[site_key].astype(str).str.strip()
        registry = registry.set_index(site_key, drop=False)
    else:
        registry = registry.copy()
        registry.index = registry.index.astype(str)
        if site_key in registry.columns:
            registry[site_key] = registry[site_key].astype(str).str.strip()

    metastat = grouped.join(
        registry,
        on=site_key,
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
    Inventory of provider-bearing file families in a configured repo.

    Key/index semantics:
        file_pattern is the grouping key.
        series_id is a dependent logical identifier.
    """
    repo_cfg = coerce_repo_config(repo=repo, repo_cfg=repo_cfg)
    root = in_path if in_path is not None else repo_cfg["root"]
    registry = repo_registry(repo_cfg=repo_cfg)

    site_key = repo_cfg["site_key"]

    allfiles = _inventory_files(root)
    allmeta = _parse_inventory_meta(allfiles, repo_cfg=repo_cfg)
    metadf = pd.DataFrame(allmeta)
    if metadf.empty:
        raise ValueError("Empty inventory")

    metadf["original_filename"] = metadf["filename"]
    metadf["file_pattern"] = metadf["filename"].map(
        lambda x: to_wildcard(x, remove_provider=False)
    )
    metadf["series_id"] = metadf.apply(
        lambda row: series_id_from_meta(row, repo_cfg=repo_cfg, remove_provider=False),
        axis=1,
    )

    keep_cols = [site_key, "subloc", "param", repo_cfg["provider_key"], "series_id"]
    if "agency_id" in metadf.columns:
        keep_cols.append("agency_id")
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
        "year_min": "min_year",
        "year_max": "max_year",
        "syear": "min_year",
        "eyear": "max_year",
    }
    grouped = grouped.rename(columns=rename_map)

    if site_key not in grouped.columns:
        raise ValueError(
            f"Cannot join registry: grouped inventory missing site key {site_key!r}"
        )

    metastat = grouped.join(
        registry,
        on=site_key,
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