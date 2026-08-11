#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import pandas as pd
import click
from dms_datastore import dstore_config

# Canonical column names used for display/search, matching the convention
# documented in AGENTS.md (station_name, latitude, longitude,
# projection_x_coordinate, projection_y_coordinate). Used as a fallback for
# registries that don't declare their own column_map.
DEFAULT_COLUMN_MAP = {
    "name": "station_name",
    "lat": "latitude",
    "lon": "longitude",
    "x": "projection_x_coordinate",
    "y": "projection_y_coordinate",
}

# Columns shown/searched in the combined result, in display order.
DISPLAY_COLUMNS = [
    "registry",
    "station_id",
    "agency",
    "agency_id",
    "station_name",
    "latitude",
    "longitude",
    "projection_x_coordinate",
    "projection_y_coordinate",
]

# Columns matched against the search phrase, when present in a registry.
SEARCH_COLUMNS = ["station_id", "station_name", "agency_id", "agency"]


def list_registries():
    """
    Print and return the configured registries with their consuming repos.

    Returns
    -------
    list of tuple
        ``(registry_name, [repo_name, ...])`` pairs, in registry declaration
        order. An empty repo list means no configured repo currently
        references that registry.
    """
    repos_by_registry = dstore_config.registries_to_repos()
    rows = []
    print("Available registries:")
    for name in dstore_config.registry_names():
        repos = repos_by_registry.get(name, [])
        repo_list = ", ".join(repos) if repos else "none"
        print(f"  {name} (repos: {repo_list})")
        rows.append((name, repos))
    return rows


def list_repos():
    """
    Print and return the configured repos with their registry and location.

    Returns
    -------
    list of tuple
        ``(repo_name, registry_names, root)`` triples, in repo declaration
        order. ``registry_names`` is a list (a repo may declare more than
        one registry, e.g. ``processed``).
    """
    rows = []
    print("Available repos:")
    for name in dstore_config.repo_names():
        repo_cfg = dstore_config.repo_config(name)
        registries = repo_cfg.get("registry")
        if registries is None:
            registries = []
        elif isinstance(registries, str):
            registries = [registries]
        registry_list = ", ".join(registries) if registries else "none"
        root = repo_cfg.get("root")
        print(f"  {name} (registry: {registry_list}) -> {root}")
        rows.append((name, registries, root))
    return rows


def _load_registry_for_search(registry_name):
    """
    Load a registry and normalize its columns for cross-registry search.

    Parameters
    ----------
    registry_name : str
        Name of a registry under the top-level ``registries`` configuration.

    Returns
    -------
    pandas.DataFrame
        Registry table with columns renamed to the canonical names used by
        :data:`DISPLAY_COLUMNS`, reindexed to include all of them (missing
        columns filled with ``NA``).
    """
    df = dstore_config.registry_df(registry_name).copy()
    column_map = dstore_config.registry_column_map(registry_name) or DEFAULT_COLUMN_MAP
    df = df.rename(columns=column_map)
    df["registry"] = registry_name
    return df.reindex(columns=DISPLAY_COLUMNS)


def station_info(search, registries=None):
    """
    Lookup station metadata by partial string match on id, name, or agency.

    Searches across all configured registries by default, tagging each
    match with the registry it came from.

    Arguments:
        SEARCHPHRASE: Search phrase which can be blank if using --config
        registries: Optional subset of registry names to search (default:
            all registries returned by dstore_config.registry_names()).
    """
    if search == "config":
        print(dstore_config.configuration())
        return

    if registries is None:
        registries = dstore_config.registry_names()

    lsearch = search.lower()
    matched_frames = []
    for registry_name in registries:
        df = _load_registry_for_search(registry_name)

        mask = pd.Series(False, index=df.index)
        for col in SEARCH_COLUMNS:
            if col in df.columns:
                values = df[col].astype(str).str.lower()
                mask = mask | values.str.contains(lsearch, na=False)
        matched_frames.append(df.loc[mask])

    print("Matches:")
    if matched_frames:
        mlook = pd.concat(matched_frames, ignore_index=True)
    else:
        mlook = pd.DataFrame(columns=DISPLAY_COLUMNS)
    mlook = mlook.sort_values(by=["registry", "station_id"])

    if mlook.shape[0] == 0:
        print("None")
    else:
        print(mlook.to_string(index=False))
    return mlook

@click.command()
@click.option(
    "--config",
    is_flag=True,
    default=False,
    help="Print configuration and location of lookup files",
)
@click.option(
    "--list-registries",
    "list_registries_flag",
    is_flag=True,
    default=False,
    help="List configured registries and the repos that use each, then exit",
)
@click.option(
    "--list-repos",
    "list_repos_flag",
    is_flag=True,
    default=False,
    help="List configured repos with their registry and root location, then exit",
)
@click.option(
    "--registry",
    "registry",
    multiple=True,
    help="Restrict search to this registry (repeatable). Default: search all registries.",
)
@click.argument("searchphrase", required=False, default="")
def station_info_cli(config, list_registries_flag, list_repos_flag, registry, searchphrase):
    """CLI for searching station information across all registries.

    Arguments:
        SEARCHPHRASE: Search phrase which can be blank if using --config, --list-registries or --list-repos
    """
    if list_registries_flag:
        list_registries()
        return
    if list_repos_flag:
        list_repos()
        return
    if config:
        searchphrase = "config"
    if not searchphrase and not config:
        raise ValueError("searchphrase required")

    registries = None
    if registry:
        valid = set(dstore_config.registry_names())
        unknown = [r for r in registry if r not in valid]
        if unknown:
            raise ValueError(
                f"Unknown registry name(s): {unknown}. Available: {sorted(valid)}"
            )
        registries = list(registry)

    station_info(searchphrase, registries=registries)


if __name__ == "__main__":
    station_info_cli()

