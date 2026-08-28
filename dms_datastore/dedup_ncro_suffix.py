"""Find and optionally remove ragged NCRO agency_id suffix duplicates.

NCRO subprograms sometimes serve the same physical station/param under more
than one agency_id suffix variant -- base (no suffix), ``q``, or ``00`` -- e.g.::

    ncro_mir_b91470_flow_2025.csv   vs   ncro_mir_b91470q_flow_2025.csv
    ncro_lis_b91560_flow_2025.csv   vs   ncro_lis_b91560q_flow_2025.csv
    ncro_<id>_<agency>_<param>_..   vs   ncro_<id>_<agency>00_<param>_..

``read_ts_repo`` treats these as overlapping shards and errors. This tool finds
such duplicate groups and, with ``--delete``, removes all but one variant.

The variant kept is chosen per-parameter using the same a priori preference
order as the download-time fix in ``download_ncro.py`` (the single source of
truth for the hierarchy, imported from there rather than duplicated here):
``flow``/``velocity`` prefer ``q``, ``elev`` prefers the base (no-suffix) id,
and everything else (e.g. ``temp``) prefers ``00``. See
``download_ncro.NCRO_SUFFIX_PREFERENCE`` / ``DEFAULT_SUFFIX_PREFERENCE``.

Dry run by default.

Examples
--------
    python dedup_ncro_suffix.py                       # dry run, screened repo
    python dedup_ncro_suffix.py --root D:/some/dir    # dry run, other dir
    python dedup_ncro_suffix.py --delete              # actually remove files
"""

import argparse
import os
import re
from collections import defaultdict

from dms_datastore.download_ncro import (
    NCRO_SUFFIX_PREFERENCE,
    DEFAULT_SUFFIX_PREFERENCE,
    _split_agency_suffix,
)

DEFAULT_ROOT = r"//cnrastore-bdo/Modeling_Data/repo/continuous/screened"

# ncro_{station_id[@subloc]}_{agency_id}_{param}_{shard}.csv
FNAME_RE = re.compile(
    r"^ncro_(?P<station>[^_]+)_(?P<agency>[^_]+)_(?P<param>[^_]+)_(?P<shard>.+)\.csv$"
)


def find_duplicate_groups(root):
    """Return {(station, param, shard, base): [(path, agency, suffix, size), ...]}
    for groups that contain more than one file (i.e. real duplicates)."""
    groups = defaultdict(list)
    for name in os.listdir(root):
        m = FNAME_RE.match(name)
        if not m:
            continue
        agency = m.group("agency")
        base, suffix = _split_agency_suffix(agency)
        suffix = suffix.lower()
        key = (m.group("station"), m.group("param"), m.group("shard"), base)
        path = os.path.join(root, name)
        try:
            size = os.path.getsize(path)
        except OSError:
            size = -1
        groups[key].append((path, agency, suffix, size))
    return {k: v for k, v in groups.items() if len(v) > 1}


def plan_group(members, param):
    """Given the members of one duplicate group, return (keeper, [to_delete]).

    The keeper is chosen per the a priori suffix preference order for `param`
    (imported from download_ncro.py). If more than one member shares the
    top-ranked suffix (shouldn't normally happen), the first one (by path) is
    kept and the rest are deleted.
    """
    order = NCRO_SUFFIX_PREFERENCE.get(param, DEFAULT_SUFFIX_PREFERENCE)
    by_suffix = defaultdict(list)
    for member in members:
        by_suffix[member[2]].append(member)

    keeper = None
    for suffix in order:
        if by_suffix.get(suffix):
            keeper = sorted(by_suffix[suffix], key=lambda m: m[0])[0]
            break
    if keeper is None:
        # No member matched a known suffix in the preference order; fall back
        # to a deterministic pick rather than leaving it unresolved.
        keeper = sorted(members, key=lambda m: m[0])[0]

    to_delete = [m for m in members if m[0] != keeper[0]]
    return keeper, to_delete


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", default=DEFAULT_ROOT, help="Directory to scan (default: screened repo).")
    ap.add_argument("--delete", action="store_true", help="Actually delete. Omit for a dry run.")
    args = ap.parse_args()

    groups = find_duplicate_groups(args.root)
    if not groups:
        print(f"No ragged q/00 duplicate groups found under {args.root}")
        return

    total_del = 0
    for key in sorted(groups):
        station, param, shard, base = key
        members = groups[key]
        keeper, to_delete = plan_group(members, param)
        print(f"\n[{station} {param} {shard}] base={base}")
        for path, agency, suffix, size in sorted(members, key=lambda m: m[1]):
            print(f"    agency={agency:12s} size={size:>9} {os.path.basename(path)}")
        print(f"    -> KEEP   {os.path.basename(keeper[0])}")
        for path, agency, suffix, size in to_delete:
            if args.delete:
                os.remove(path)
                print(f"    -> DELETED {os.path.basename(path)}")
            else:
                print(f"    -> DELETE  {os.path.basename(path)}  (dry run)")
            total_del += 1

    action = "Deleted" if args.delete else "Would delete"
    print(f"\n{action} {total_del} file(s) across {len(groups)} group(s)."
          + ("" if args.delete else "  Re-run with --delete to apply."))


if __name__ == "__main__":
    main()
