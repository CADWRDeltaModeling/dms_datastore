"""Report USGS sublocations present before the USGS-to-USBR transition.

This script reads the transition list and scans the archived formatted and
screened repositories. It does not create, modify, rename, or delete files.
"""

import argparse
import csv
import re
from pathlib import Path


ROOT = Path(__file__).parent
DEFAULT_TRANSITION_LIST = (
    ROOT / "dms_datastore" / "config_data" / "usgs_usbr_transition_list.csv"
)
DEFAULT_REPOSITORIES = (
    Path(r"W:\archive_repo\repo\continuous\formatted"),
    Path(r"W:\archive_repo\repo\continuous\screened"),
)


def transition_stations(transition_list):
    """Return original and matched station IDs in transition-list order."""
    with transition_list.open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        required_columns = {"original_station_id", "matched_station_id"}
        missing_columns = required_columns.difference(reader.fieldnames or [])
        if missing_columns:
            raise ValueError(
                f"{transition_list} lacks required column(s): {sorted(missing_columns)}"
            )

        stations = []
        seen_original_ids = set()
        for row in reader:
            original_station_id = (
                row.get("original_station_id") or ""
            ).strip().lower()
            matched_station_id = (row.get("matched_station_id") or "").strip().lower()
            if original_station_id and original_station_id not in seen_original_ids:
                stations.append((original_station_id, matched_station_id))
                seen_original_ids.add(original_station_id)
    return stations


def station_patterns(station_ids):
    """Compile exact filename patterns for each original USGS station ID."""
    return {
        station_id: re.compile(
            rf"^usgs_{re.escape(station_id)}(?:@(?P<subloc>[^_]+))?_.+\.csv$",
            re.IGNORECASE,
        )
        for station_id in station_ids
    }


def find_examples(repository, patterns):
    """Return one filename per station/sublocation state in a repository."""
    examples = {station_id: {} for station_id in patterns}
    if not repository.is_dir():
        return examples

    for entry in sorted(repository.glob("usgs_*.csv"), key=lambda path: path.name.lower()):
        for station_id, pattern in patterns.items():
            match = pattern.fullmatch(entry.name)
            if match is None:
                continue
            subloc = match.group("subloc")
            state = subloc.lower() if subloc else "default"
            examples[station_id].setdefault(state, entry.name)
            break
    return examples


def print_report(transition_list, repositories):
    """Print the archive inventory grouped by original station ID."""
    stations = transition_stations(transition_list)
    station_ids = [original_station_id for original_station_id, _ in stations]
    patterns = station_patterns(station_ids)

    print(f"Transition list: {transition_list}")
    print(f"Original station IDs: {len(station_ids)}")

    for repository in repositories:
        examples = find_examples(repository, patterns)
        print(f"\nRepository: {repository}")
        if not repository.is_dir():
            print("  MISSING DIRECTORY")
            continue

        for station_id, matched_station_id in stations:
            found = examples[station_id]
            default_example = found.pop("default", None)
            subloc_examples = sorted(found.items())

            if default_example:
                print(
                    f"{station_id},{matched_station_id},default,{default_example}"
                )
            for subloc, example in subloc_examples:
                print(f"{station_id},{matched_station_id},{subloc},{example}")
            if not default_example and not subloc_examples:
                print(f"{station_id},{matched_station_id},NONE,")


def main():
    parser = argparse.ArgumentParser(
        description="Report archived USGS files with and without sublocations."
    )
    parser.add_argument(
        "--transition-list",
        type=Path,
        default=DEFAULT_TRANSITION_LIST,
        help="CSV containing original_station_id values.",
    )
    parser.add_argument(
        "--repository",
        action="append",
        type=Path,
        default=None,
        help="Archive repository to scan. May be supplied more than once.",
    )
    args = parser.parse_args()
    repositories = tuple(args.repository) if args.repository else DEFAULT_REPOSITORIES
    print_report(args.transition_list, repositories)


if __name__ == "__main__":
    main()