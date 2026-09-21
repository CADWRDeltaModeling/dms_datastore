"""Find and optionally delete CDEC formatted files shadowed by NCRO data.

The formatted repository retains both NCRO's quality-controlled series and its
CDEC realtime supplement. Once NCRO coverage is available, the supplement can
be removed for a logical series only when all of the following agree:

* a CDEC file and an NCRO file have the same station, sublocation, and parameter;
* the station registry identifies the base station as ``dwr_ncro``; and
* both filenames parse under the formatted repository naming convention.

The default is a dry run. Each candidate lists the CDEC file and a matching
NCRO file. Ambiguous or malformed entries are reported but never deleted.
"""

import argparse
import os
from collections import defaultdict

from dms_datastore import dstore_config
from dms_datastore.filename import interpret_fname, naming_spec


DEFAULT_ROOT = r"//cnrastore-bdo/Modeling_Data/repo/continuous/formatted"


def _series_key(meta):
    """Return a logical formatted-series key from parsed filename metadata."""
    return (
        str(meta["station_id"]).lower(),
        str(meta.get("subloc") or "default").lower(),
        str(meta["param"]).lower(),
    )


def _parse_formatted_files(root):
    """Return parsed CDEC and NCRO files plus filenames that could not be parsed."""
    naming = naming_spec(repo="formatted")
    cdec_files = []
    ncro_files = []
    unparseable = []
    for name in sorted(os.listdir(root)):
        if not name.lower().endswith(".csv"):
            continue
        path = os.path.join(root, name)
        try:
            meta = interpret_fname(name, naming=naming)
        except ValueError:
            unparseable.append(path)
            continue
        source = str(meta.get("source", "")).lower()
        if source == "cdec":
            cdec_files.append((path, meta))
        elif source == "ncro":
            ncro_files.append((path, meta))
    return cdec_files, ncro_files, unparseable


def plan_cdec_shadow_deletions(root, registry):
    """Return confirmed CDEC deletions and ambiguous CDEC cases for ``root``.

    ``registry`` must contain ``station_id`` and ``agency`` columns. A CDEC
    file is a deletion candidate only when its logical series has an NCRO file
    and its registry agency is ``dwr_ncro``.
    """
    required_columns = {"station_id", "agency"}
    missing_columns = required_columns.difference(registry.columns)
    if missing_columns:
        raise ValueError(
            "Registry is missing required column(s): "
            + ", ".join(sorted(missing_columns))
        )

    cdec_files, ncro_files, unparseable = _parse_formatted_files(root)
    ncro_by_key = defaultdict(list)
    for path, meta in ncro_files:
        ncro_by_key[_series_key(meta)].append(path)

    registry_agency = registry.copy()
    registry_agency["station_id"] = (
        registry_agency["station_id"].astype(str).str.strip().str.lower()
    )
    registry_agency["agency"] = (
        registry_agency["agency"].astype(str).str.strip().str.lower()
    )
    agencies_by_station = registry_agency.groupby("station_id")["agency"].agg(set)

    candidates = []
    ambiguous = []
    for cdec_path, cdec_meta in cdec_files:
        key = _series_key(cdec_meta)
        matching_ncro = sorted(ncro_by_key.get(key, []))
        if not matching_ncro:
            continue

        station_id = key[0]
        agencies = agencies_by_station.get(station_id, set())
        if agencies == {"dwr_ncro"}:
            candidates.append((cdec_path, matching_ncro[0]))
            continue

        if not agencies:
            reason = "station_id absent from registry"
        else:
            reason = "registry agency is " + ", ".join(sorted(agencies))
        ambiguous.append((cdec_path, matching_ncro[0], reason))

    return candidates, ambiguous, unparseable


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        default=DEFAULT_ROOT,
        help="Formatted repository directory to scan.",
    )
    parser.add_argument(
        "--delete",
        action="store_true",
        help="Delete confirmed CDEC shadow files. Omit for a dry run.",
    )
    args = parser.parse_args()

    registry = dstore_config.repo_registry("formatted")
    candidates, ambiguous, unparseable = plan_cdec_shadow_deletions(
        args.root, registry
    )

    for cdec_path, ncro_path in candidates:
        print(f"CDEC: {os.path.basename(cdec_path)}")
        print(f"NCRO: {os.path.basename(ncro_path)}")
        if args.delete:
            os.remove(cdec_path)
            print("ACTION: deleted")
        else:
            print("ACTION: would delete")

    if ambiguous:
        print("\nAmbiguous CDEC files not deleted:")
        for cdec_path, ncro_path, reason in ambiguous:
            print(f"CDEC: {os.path.basename(cdec_path)}")
            print(f"NCRO: {os.path.basename(ncro_path)}")
            print(f"REASON: {reason}")

    if unparseable:
        print("\nUnparseable CSV files ignored:")
        for path in unparseable:
            print(os.path.basename(path))

    action = "Deleted" if args.delete else "Would delete"
    print(f"\n{action} {len(candidates)} confirmed CDEC file(s).")


if __name__ == "__main__":
    main()