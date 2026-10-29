#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Remove redundant screened shards so they can be regenerated.

The screened tier must hold exactly one file per
``(station_id@subloc, param, year)``.  ``agency_id`` occupies a filename slot
but is absent from the repo ``file_key``, so a run that resolves it differently
writes a parallel file instead of overwriting.  The result is two or more shards
for one logical series-year, which makes ``read_ts_repo`` raise
``Overlapping shard windows detected``.

For agencies that re-download on a routine cycle the cheapest repair is to
delete every variant in an affected group and let the pipeline rebuild it.  This
tool does that, restricted to agencies given by ``--agency`` and refusing to act
unless a counterpart exists in the staging repository.

Dry run by default; pass ``--delete`` to act.

Examples
--------
::

    python prune_screened_duplicates.py
    python prune_screened_duplicates.py --scope group
    python prune_screened_duplicates.py --exclude-station bac --exclude-station odm2
    python prune_screened_duplicates.py --delete
"""

import csv
import os
import re
from collections import defaultdict

import click

from dms_datastore import dstore_config

__all__ = [
    "find_duplicate_groups",
    "prune_screened_duplicates",
]

# {agency}_{station_id@subloc}_{agency_id}_{param}_{year}.csv
FNAME_RE = re.compile(
    r"^(?P<agency>[^_]+)_(?P<key>[^_]+)_(?P<agency_id>[^_]+)_"
    r"(?P<param>[^_]+)_(?P<year>\d{4})\.csv$"
)

DEFAULT_AGENCIES = ("dwr_ncro",)
DEFAULT_EXCLUDE = ()
DEFAULT_STAGING = r"//cnrastore-bdo/Modeling_Data/repo_staging_auto/continuous/screened"


def _parse(name):
    """Split a screened filename into its grammar slots.

    Parameters
    ----------
    name : str
        Bare filename.

    Returns
    -------
    dict or None
        Keys ``agency``, ``station_id``, ``subloc``, ``agency_id``, ``param``,
        ``year``, or ``None`` if the name does not match the screened grammar.
    """
    m = FNAME_RE.match(name)
    if m is None:
        return None
    g = m.groupdict()
    station, _, subloc = g["key"].partition("@")
    return {
        "agency": g["agency"],
        "station_id": station,
        "subloc": subloc,
        "agency_id": g["agency_id"],
        "param": g["param"],
        "year": g["year"],
    }


def find_duplicate_groups(root):
    """Group screened files by logical series-year and keep the redundant ones.

    Parameters
    ----------
    root : str
        Screened repository directory.

    Returns
    -------
    groups : dict
        Maps ``(station_id, subloc, param, year)`` to a list of parsed file
        dicts, each with an added ``filename`` key.  Only keys with more than
        one file are included.
    unparsed : list of str
        Filenames that do not match the screened grammar.
    """
    groups = defaultdict(list)
    unparsed = []
    for name in os.listdir(root):
        if not name.lower().endswith(".csv"):
            continue
        meta = _parse(name)
        if meta is None:
            unparsed.append(name)
            continue
        meta["filename"] = name
        groups[
            (meta["station_id"], meta["subloc"], meta["param"], meta["year"])
        ].append(meta)
    return {k: v for k, v in groups.items() if len(v) > 1}, unparsed


def _station_agency(registry):
    """Map station_id to its registry agency, lower-cased."""
    return {
        str(sid): str(registry.loc[sid, "agency"]).strip().lower()
        for sid in registry.index
    }


def _staging_index(staging_root):
    """Index staging filenames by ``(station_id, subloc, param, year)``."""
    index = defaultdict(list)
    for name in os.listdir(staging_root):
        if not name.lower().endswith(".csv"):
            continue
        meta = _parse(name)
        if meta is not None:
            index[
                (meta["station_id"], meta["subloc"], meta["param"], meta["year"])
            ].append(name)
    return index


def prune_screened_duplicates(
    root,
    staging_root,
    agencies=DEFAULT_AGENCIES,
    exclude_stations=DEFAULT_EXCLUDE,
    scope="series",
    require_staging=True,
    delete=False,
    manifest="pruned_screened_shards.csv",
):
    """Delete every variant of redundant screened shards for selected agencies.

    Parameters
    ----------
    root : str
        Screened repository directory to prune.
    staging_root : str
        Staging screened directory used to confirm a series-year can be
        regenerated before anything is removed.
    agencies : sequence of str, optional
        Registry ``agency`` values to act on, matched case-insensitively as a
        substring so ``"ncro"`` covers ``"dwr_ncro"``.  Default
        ``("dwr_ncro",)``.
    exclude_stations : sequence of str, optional
        Station ids to leave untouched.  Empty by default.
    scope : {"group", "series"}, optional
        ``"series"`` (default) removes every shard of an affected
        ``(station_id, subloc, param)`` series, which also clears cross-year
        ``agency_id`` inconsistency.  ``"group"`` removes only the colliding
        year shards.
    require_staging : bool, optional
        If ``True`` (default), skip any group with no counterpart in
        *staging_root*.
    delete : bool, optional
        If ``False`` (default) report only.  If ``True`` remove files.
    manifest : str or None, optional
        Path for a CSV record of the planned or performed deletions.

    Returns
    -------
    dict
        Summary with ``planned``, ``deleted``, ``skipped_no_staging``,
        ``skipped_excluded``, ``skipped_agency`` and ``stations``.

    Raises
    ------
    ValueError
        If *scope* is not recognised, or either directory is missing.
    """
    if scope not in ("group", "series"):
        raise ValueError(f"scope must be 'group' or 'series', got {scope!r}")
    for label, path in (("root", root), ("staging_root", staging_root)):
        if not os.path.isdir(path):
            raise ValueError(f"{label} is not a directory: {path}")

    agencies = tuple(a.strip().lower() for a in agencies)
    exclude = {s.strip().lower() for s in exclude_stations}

    groups, unparsed = find_duplicate_groups(root)
    registry = dstore_config.repo_registry("screened")
    agency_of = _station_agency(registry)
    staging = _staging_index(staging_root) if require_staging else {}

    targeted = {}
    skipped_excluded = skipped_agency = skipped_no_staging = 0
    unknown_station = set()

    for key, members in groups.items():
        station, subloc, param, year = key
        if station in exclude:
            skipped_excluded += 1
            continue
        agency = agency_of.get(station)
        if agency is None:
            unknown_station.add(station)
            skipped_agency += 1
            continue
        if not any(a in agency for a in agencies):
            skipped_agency += 1
            continue
        if require_staging and not staging.get(key):
            skipped_no_staging += 1
            continue
        targeted[key] = members

    # expand to whole series when asked
    selected_groups = len(targeted)
    if scope == "series":
        wanted = {(k[0], k[1], k[2]) for k in targeted}
        expanded = defaultdict(list)
        for name in os.listdir(root):
            if not name.lower().endswith(".csv"):
                continue
            meta = _parse(name)
            if meta is None:
                continue
            sk = (meta["station_id"], meta["subloc"], meta["param"])
            if sk in wanted:
                meta["filename"] = name
                expanded[
                    (meta["station_id"], meta["subloc"], meta["param"], meta["year"])
                ].append(meta)
        targeted = dict(expanded)

    rows = []
    for key in sorted(targeted):
        station, subloc, param, year = key
        for meta in sorted(targeted[key], key=lambda m: m["filename"]):
            rows.append(
                {
                    "station_id": station,
                    "subloc": subloc,
                    "param": param,
                    "year": year,
                    "agency": meta["agency"],
                    "agency_id": meta["agency_id"],
                    "filename": meta["filename"],
                    "staging_counterparts": len(staging.get(key, [])),
                }
            )

    stations = sorted({r["station_id"] for r in rows})

    print(f"screened root : {root}")
    print(f"staging root  : {staging_root}")
    print(f"agencies      : {list(agencies)}")
    print(f"excluded      : {sorted(exclude)}")
    print(f"scope         : {scope}")
    print()
    print(f"unparsed filenames          : {len(unparsed)}")
    print(f"redundant groups found      : {len(groups)}")
    print(f"  skipped, excluded station : {skipped_excluded}")
    print(f"  skipped, agency filter    : {skipped_agency}")
    print(f"  skipped, no staging copy  : {skipped_no_staging}")
    print(f"redundant groups selected   : {selected_groups}")
    if scope == "series":
        print(f"  expanded to year-shards   : {len(targeted)}"
              f"  (+{len(targeted) - selected_groups} non-colliding years)")
    print(f"files targeted              : {len(rows)}")
    print(f"stations                    : {len(stations)}")
    if unknown_station:
        print(f"  station ids not in registry: {sorted(unknown_station)}")

    per_station = defaultdict(int)
    for r in rows:
        per_station[r["station_id"]] += 1
    if per_station:
        print("\nfiles per station:")
        for st in sorted(per_station):
            print(f"    {st:12s} {per_station[st]:5d}")

    deleted = 0
    if delete:
        for r in rows:
            path = os.path.join(root, r["filename"])
            try:
                os.remove(path)
                deleted += 1
            except OSError as exc:
                print(f"    FAILED {r['filename']}: {exc}")

    if manifest:
        with open(manifest, "w", newline="") as fh:
            w = csv.DictWriter(
                fh,
                fieldnames=[
                    "station_id", "subloc", "param", "year",
                    "agency", "agency_id", "filename", "staging_counterparts",
                ],
            )
            w.writeheader()
            w.writerows(rows)
        print(f"\nwrote {manifest}")

    verb = f"Deleted {deleted}" if delete else f"Would delete {len(rows)}"
    print(f"\n{verb} file(s) across {selected_groups} redundant group(s)."
          + ("" if delete else "  Re-run with --delete to apply."))

    return {
        "planned": len(rows),
        "deleted": deleted,
        "selected_groups": selected_groups,
        "year_shards": len(targeted),
        "skipped_no_staging": skipped_no_staging,
        "skipped_excluded": skipped_excluded,
        "skipped_agency": skipped_agency,
        "stations": stations,
    }


@click.command(
    help="Delete every variant of redundant screened shards so the pipeline "
         "can regenerate them. Dry run unless --delete is given."
)
@click.help_option("-h", "--help")
@click.option(
    "--root",
    default=None,
    help="Screened repo directory. Default: configured screened root.",
)
@click.option(
    "--staging-root",
    default=DEFAULT_STAGING,
    show_default=True,
    help="Staging screened directory used to confirm regenerability.",
)
@click.option(
    "--agency",
    "agencies",
    multiple=True,
    default=DEFAULT_AGENCIES,
    show_default=True,
    help="Registry agency to act on; substring match. Repeatable.",
)
@click.option(
    "--exclude-station",
    "exclude_stations",
    multiple=True,
    default=DEFAULT_EXCLUDE,
    help="Station id to leave untouched. Repeatable. No exclusions by default.",
)
@click.option(
    "--scope",
    type=click.Choice(["group", "series"]),
    default="series",
    show_default=True,
    help="Remove every shard of an affected series, or only colliding years.",
)
@click.option(
    "--require-staging/--allow-missing-staging",
    default=True,
    show_default=True,
    help="Refuse to delete a group with no staging counterpart.",
)
@click.option(
    "--manifest",
    default="pruned_screened_shards.csv",
    show_default=True,
    help="CSV record of planned or performed deletions.",
)
@click.option("--delete", is_flag=True, help="Actually remove files.")
def prune_screened_duplicates_cli(
    root, staging_root, agencies, exclude_stations, scope,
    require_staging, manifest, delete,
):
    """Command-line entry point for :func:`prune_screened_duplicates`."""
    if root is None:
        root = dstore_config.repo_root("screened")
    prune_screened_duplicates(
        root=root,
        staging_root=staging_root,
        agencies=agencies,
        exclude_stations=exclude_stations,
        scope=scope,
        require_staging=require_staging,
        delete=delete,
        manifest=manifest,
    )


if __name__ == "__main__":
    prune_screened_duplicates_cli()
