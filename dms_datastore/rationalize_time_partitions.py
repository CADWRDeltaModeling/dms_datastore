"""
YAML-driven and legacy rationalization of time-partitioned instrument files.

Provides:
  - rationalize_time_partitions(pat): legacy filename-span-based supersession logic
  - rationalize_time_partitions_from_yaml(...): explicit YAML-driven canonical
    time partitioning with slicing semantics, omit completeness checks, and
    optional renaming to match actual data span.

Key guarantees for YAML-driven mode:
  - Uses half-open windows: keep timestamps t such that
        t >= start_k and t < start_{k+1}
    (last window has no upper bound; ${START} means no lower bound)
  - `${LAST}` resolves to max eyear parsed from filenames in the matched pool.
  - `${SUPERSEDED}` expands via the existing filename-span supersession rule.
  - include ∩ omit is an error (user can always be explicit).
  - If renaming is enabled, both syear and eyear in filenames are updated to match
    the actual sliced data span (min.year..max.year), regardless of month/day.
  - Read/write are idempotent w.r.t. formatting by preserving the original header
    text (commented YAML block) via read_ts.original_header and write_ts.write_ts_csv.
"""

from __future__ import annotations

import os
import glob
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any

import yaml
import pandas as pd

# Toolkit functions (no new readers/writers invented here)
from .filename import interpret_fname
from .read_ts import read_ts, original_header
from .write_ts import write_ts_csv

logger = logging.getLogger(__name__)

_START = "${START}"
_LAST = "${LAST}"
_SUPERSEDED = "${SUPERSEDED}"


# --------------------------------------------------------------------------------------
# Legacy behavior (kept intact)
# --------------------------------------------------------------------------------------

# --------------------------------------------------------------------------------------
# Public wrapper: YAML per-series + legacy fallback (restores original broad-pattern behavior)
# --------------------------------------------------------------------------------------

def rationalize_time_partitions(
    pat: str,
    *,
    yaml_path: str | Path | None = None,
    root_dir: str | Path | None = None,
    dry_run: bool = False,
    warn_on_remaining_overlap: bool = True
) -> None:
    """
    Rationalize time-partitioned instrument files with optional YAML overrides.

    This function restores the original behavior for broad glob patterns by first
    grouping matched files into distinct time series (agency/param/station_id/subloc),
    then processing each group independently.

    Behavior
    --------
    - If `yaml_path` is None:
        Apply legacy "superset deletes subset" logic within each series group.
        (The legacy logic already enforces same-series comparisons, so it is safe
        to call it on broad patterns.)

    - If `yaml_path` is provided:
        For each series group:
          1) Apply any YAML rule(s) whose `pattern` matches one or more files in that
             group. Each YAML rule is applied only to the subset ("pool") of files in
             that group that match the rule pattern.
          2) Apply legacy supersession to the remaining files in that group that are
             not owned by any YAML rule pattern.

        YAML rules that match nothing in the `pat` universe are ignored.

    Parameters
    ----------
    pat : str
        Glob pattern defining the universe of files to consider.
    yaml_path : str or pathlib.Path, optional
        YAML rationalization spec path. If omitted, legacy-only mode is used.
    root_dir : str or pathlib.Path, optional
        Root directory for evaluating `pat` and YAML patterns. If provided, both
        `pat` and YAML rule patterns are interpreted relative to this directory
        (unless already absolute).
    dry_run : bool, default=False
        If True, do not modify/delete files; only log intended actions.
    warn_on_remaining_overlap : bool, default=True
        If True, emit advisory warnings if overlaps remain after YAML slicing.


    Returns
    -------
    None
    """
    import fnmatch

    root_dir_p = Path(root_dir) if root_dir is not None else None

    if root_dir_p is not None:
        p = Path(pat)
        if not p.is_absolute():
            pat = str(root_dir_p / p)

    allpaths = sorted(Path(p) for p in glob.glob(pat))
    if not allpaths:
        raise ValueError(f"Pattern matched no files: {pat}")

    if yaml_path is None:
        # Legacy is already safe on broad patterns (it only compares within series)
        _legacy_rationalize_time_partitions(pat)
        return
    
    yaml_path_p = Path(yaml_path)
    if not yaml_path_p.exists():
        # Allow project-style config lookup (optional; no-op if not available)
        try:
            from dms_datastore.dstore_config import config_file  # type: ignore
        except Exception:
            config_file = None  # type: ignore

        if config_file is not None:
            candidate = Path(config_file(str(yaml_path)))
            if candidate.exists():
                yaml_path_p = candidate

    if not yaml_path_p.exists():
        raise ValueError(f"YAML rationalization config not found at {yaml_path_p}")

    with yaml_path_p.open("r") as fp:
        cfg = yaml.safe_load(fp)

    rules = cfg.get("rationalize", [])
    if not isinstance(rules, list):
        raise ValueError("rationalize must be a list of rules")

    # Group by series key (legacy behavior)
    groups: Dict[tuple, List[Path]] = {}
    for p in allpaths:
        meta = interpret_fname(p.name)
        key = (meta["agency"], meta["param"], meta["station_id"], meta["subloc"])
        groups.setdefault(key, []).append(p)

    # Apply YAML per group/pool with ambiguity check across rules
    claimed: Dict[Path, int] = {}
    applied_patterns: set[str] = set()

    for irule, rule in enumerate(rules):
        pattern = rule["pattern"]
        matched_any = False

        for key, gpaths in groups.items():
            pool = [p for p in gpaths if fnmatch.fnmatchcase(p.name, pattern)]
            if not pool:
                continue

            matched_any = True
            applied_patterns.add(pattern)

            for p in pool:
                if p in claimed and claimed[p] != irule:
                    raise ValueError(
                        f"File {p.name} matched multiple rationalize rules "
                        f"(rule {claimed[p]} and {irule})"
                    )
                claimed[p] = irule

            _apply_rule(
                rule=rule,
                pool=pool,
                dry_run=dry_run,
                warn_on_remaining_overlap=warn_on_remaining_overlap
            )

        # ignore rules that match nothing in this `pat` universe
        if not matched_any:
            continue

    # Legacy fallback: only for files not owned by YAML rule patterns.
    # If YAML wrote/renamed, re-glob to get current state.
    if dry_run:
        remaining = [p for p in allpaths if p not in claimed]
    else:
        allpaths2 = sorted(Path(p) for p in glob.glob(pat))
        if applied_patterns:
            remaining = [
                p for p in allpaths2
                if not any(fnmatch.fnmatchcase(p.name, yp) for yp in applied_patterns)
            ]
        else:
            remaining = allpaths2

    if remaining:
        logger.info(f"legacy fallback: {len(remaining)} of {len(allpaths)} files")
        _legacy_supersession_paths(remaining, dry_run=dry_run)
    else:
        logger.info("legacy fallback: 0 files (all matched by YAML patterns)")

def _legacy_rationalize_time_partitions(pat: str) -> None:
    """
    Remove files whose filename year span is fully superseded by another file
    representing the same series.
    """
    allpaths = glob.glob(pat)
    if not allpaths:
        return

    repodir = os.path.split(allpaths[0])[0]
    allfiles = [os.path.split(x)[1] for x in allpaths]

    allmeta = [interpret_fname(fname) for fname in allfiles]
    already_checked = set()
    superseded = []

    for meta in allmeta:
        if meta["filename"] in already_checked:
            continue

        near_misses = []
        for meta2 in allmeta:
            if meta is meta2:
                continue

            same_series = (
                meta["agency"] == meta2["agency"]
                and meta["param"] == meta2["param"]
                and meta["station_id"] == meta2["station_id"]
                and meta["subloc"] == meta2["subloc"]
            )
            if same_series:
                near_misses.append(meta2)
                already_checked.add(meta2["filename"])

        already_checked.add(meta["filename"])
        if not near_misses:
            continue

        near_misses.append(meta)

        for m in near_misses:
            superseding = []
            for m2 in near_misses:
                if m is m2:
                    continue
                if m2["syear"] <= m["syear"] and m2["eyear"] >= m["eyear"]:
                    superseding.append(m2)

            if superseding:
                fn = m["filename"]
                logger.info(f"superseded: {fn}")
                for s in superseding:
                    logger.info(f"  superseded by {s['filename']}")
                os.remove(os.path.join(repodir, fn))
                superseded.append(fn)

    if superseded:
        logger.info("Superseded files:")
        for s in superseded:
            logger.info(s)


# --------------------------------------------------------------------------------------
# YAML-driven behavior
# --------------------------------------------------------------------------------------

def rationalize_time_partitions_from_yaml(
    yaml_path: str | Path,
    *,
    root_dir: str | Path | None = None,
    dry_run: bool = False,
    warn_on_remaining_overlap: bool = True
) -> None:
    """
    Apply YAML-defined canonical time partitions to pools of instrument files.

    See module docstring for semantics.
    """
    yaml_path = Path(yaml_path)
    with yaml_path.open("r") as fp:
        cfg = yaml.safe_load(fp)

    rules = cfg.get("rationalize", [])
    if not isinstance(rules, list):
        raise ValueError("rationalize must be a list of rules")

    root_dir = Path(root_dir) if root_dir else Path(".")

    # Track which files are claimed by which rule (ambiguity check)
    claimed: Dict[Path, int] = {}

    for irule, rule in enumerate(rules):
        pattern = rule["pattern"]
        pool = sorted(Path(p) for p in glob.glob(str(root_dir / pattern)))

        if not pool:
            continue

        for p in pool:
            if p in claimed:
                raise ValueError(
                    f"File {p.name} matched multiple rationalize rules "
                    f"(rule {claimed[p]} and {irule})"
                )
            claimed[p] = irule

        _apply_rule(
            rule=rule,
            pool=pool,
            dry_run=dry_run,
            warn_on_remaining_overlap=warn_on_remaining_overlap
        )


def _apply_rule(
    *,
    rule: dict,
    pool: List[Path],
    dry_run: bool,
    warn_on_remaining_overlap: bool
) -> None:
    """
    Apply a single YAML rationalize rule to a concrete pool of files (paths).

    Pool membership is determined externally; within this function, YAML `fname`
    values must match pool basenames exactly (after ${LAST} substitution).
    """
    pool_names = {p.name for p in pool}
    pool_by_name = {p.name: p for p in pool}
    pool_meta: Dict[str, dict] = {p.name: interpret_fname(p.name) for p in pool}

    include = rule.get("include")
    if not include or not isinstance(include, list):
        raise ValueError("include list may not be empty")

    # Resolve ${LAST} within this pool
    max_eyear = max(int(m["eyear"]) for m in pool_meta.values())

    include_entries: List[Tuple[Path, Optional[pd.Timestamp]]] = []
    for entry in include:
        fname_tmpl = entry["fname"]
        start = entry["start"]

        fname = fname_tmpl.replace(_LAST, str(max_eyear)) if _LAST in fname_tmpl else fname_tmpl

        if fname not in pool_by_name:
            raise ValueError(f"include fname not found in pool: {fname}")

        if start == _START:
            start_ts = None
        else:
            start_ts = pd.Timestamp(start)

        include_entries.append((pool_by_name[fname], start_ts))

    # Strictly increasing starts (ignoring leading None)
    starts = [s for (_, s) in include_entries if s is not None]
    if starts != sorted(starts):
        raise ValueError("include start times must be increasing")
    if len(starts) != len(set(starts)):
        raise ValueError("include start times must be strictly increasing")

    include_names = {p.name for (p, _) in include_entries}

    # Resolve omit (optional)
    omit: set[str] = set()
    omit_present = "omit" in rule
    for item in rule.get("omit", []) or []:
        if item == _SUPERSEDED:
            omit |= _find_superseded(pool_meta)
        else:
            if item not in pool_names:
                raise ValueError(f"omit entry not in pool: {item}")
            omit.add(item)

    # include/omit clash is an error
    clash = include_names & omit
    if clash:
        raise ValueError(f"Files appear in both include and omit: {sorted(clash)}")

    # Completeness check if omit present
    if omit_present:
        if include_names | omit != pool_names:
            raise ValueError(
                "include + omit do not cover pool:\n"
                f"  pool: {sorted(pool_names)}\n"
                f"  include+omit: {sorted(include_names | omit)}"
            )

    # Phase 1: Read+slice all includes, determine planned target names and validate collisions.
    plan: List[Dict[str, Any]] = []
    planned_targets: Dict[str, str] = {}

    for i, (fname, start_ts) in enumerate(include_entries):
        next_start = include_entries[i + 1][1] if i + 1 < len(include_entries) else None

        header_str = original_header(str(fname))
        df = read_ts(str(fname), force_regular=False, freq=None)

        idx = df.index
        if not isinstance(idx, pd.DatetimeIndex):
            raise TypeError(f"{fname.name}: read_ts did not return a DatetimeIndex")

        # Half-open: t >= start_k and t < start_{k+1}
        mask = pd.Series(True, index=idx)
        if start_ts is not None:
            mask &= idx >= start_ts
        if next_start is not None:
            mask &= idx < next_start

        sliced = df.loc[mask]
        if sliced.empty:
            raise ValueError(f"Slicing produced empty series for {fname.name}")

        old_name = fname.name
        old_path = fname
        new_path = old_path

        new_syear = int(sliced.index.min().year)
        new_eyear = int(sliced.index.max().year)

        old_meta = pool_meta[old_name]
        new_name = _rename_year_span(
            old_name,
            old_syear=int(old_meta["syear"]),
            old_eyear=int(old_meta["eyear"]),
            new_syear=new_syear,
            new_eyear=new_eyear,
        )
        new_path = old_path.parent / new_name

        # include->include collision is an error
        if new_name in planned_targets:
            raise ValueError(
                f"Include rename collision: {old_name} and {planned_targets[new_name]} "
                f"both map to {new_name}"
            )
        planned_targets[new_name] = old_name

        plan.append(
            dict(
                old_path=old_path,
                new_path=new_path,
                sliced=sliced,
                window=(start_ts, next_start),
                header_str=header_str,
            )
        )

    # Collision rules against pool/omit:
    # - if target exists and is not omitted => error
    for item in plan:
        new_name = item["new_path"].name
        old_name = item["old_path"].name
        if new_name == old_name:
            continue
        if new_name in pool_names and new_name not in omit:
            raise ValueError(
                f"Rename target {new_name} already exists in pool and is not omitted "
                f"(from {old_name})"
            )

    # Phase 2: Write temp -> atomic replace using write_ts_csv with preserved header.
    for item in plan:
        old_path: Path = item["old_path"]
        new_path: Path = item["new_path"]
        sliced = item["sliced"]
        header_str = item["header_str"]
        start_ts, next_start = item["window"]

        logger.info(
            f"sliced {old_path.name} -> {new_path.name}: "
            f"[{start_ts if start_ts is not None else '-inf'}, "
            f"{next_start if next_start is not None else '+inf'})"
        )

        if dry_run:
            continue

        tmp_path = new_path.with_name(new_path.name + ".tmp_rationalize")
        write_ts_csv(
            sliced,
            tmp_path,
            metadata=header_str,
            chunk_years=False,
            overwrite_conventions=False,
        )
        os.replace(tmp_path, new_path)

        if old_path != new_path and old_path.exists():
            old_path.unlink()

    # Phase 3: delete omitted files that still exist (some may have been overwritten)
    for fname in sorted(omit):
        p = pool_by_name[fname]
        if dry_run:
            logger.info(f"omitted {fname}")
            continue
        if p.exists():
            p.unlink()
            logger.info(f"omitted {fname}")
        else:
            logger.info(f"omitted {fname} (already absent)")

    # Advisory overlap warning
    if warn_on_remaining_overlap:
        if dry_run:
            ranges = [
                (
                    item["new_path"].name,
                    item["sliced"].index.min(),
                    item["sliced"].index.max(),
                )
                for item in plan
            ]
            if len(ranges) > 1:
                _warn_if_overlap_ranges(ranges)
        else:
            final_paths = [item["new_path"] for item in plan]
            if len(final_paths) > 1:
                _warn_if_overlap(final_paths)


def _find_superseded(pool_meta: Dict[str, dict]) -> set[str]:
    superseded: set[str] = set()
    metas = list(pool_meta.values())

    for m in metas:
        for m2 in metas:
            if m is m2:
                continue
            if m2["syear"] <= m["syear"] and m2["eyear"] >= m["eyear"]:
                superseded.add(m["filename"])
                break

    return superseded


def _rename_year_span(
    fname: str,
    *,
    old_syear: int,
    old_eyear: int,
    new_syear: int,
    new_eyear: int,
) -> str:
    """
    Replace a single `_old_syear_old_eyear` segment with `_new_syear_new_eyear`.

    Fail-fast: if the old segment is not found exactly once, error.
    """
    old_seg = f"_{old_syear}_{old_eyear}"
    new_seg = f"_{new_syear}_{new_eyear}"

    n = fname.count(old_seg)
    if n != 1:
        raise ValueError(
            f"Cannot rename {fname}: expected exactly one '{old_seg}' segment, found {n}"
        )

    return fname.replace(old_seg, new_seg, 1)


def _warn_if_overlap_ranges(
    ranges: List[Tuple[str, pd.Timestamp, pd.Timestamp]]
) -> None:
    """
    Advisory-only overlap check using in-memory (name, tmin, tmax) tuples.
    """
    for i in range(len(ranges)):
        n1, a1, b1 = ranges[i]
        for j in range(i + 1, len(ranges)):
            n2, a2, b2 = ranges[j]
            if a1 <= b2 and a2 <= b1:
                logger.warning(f"post-slice overlap remains between {n1} and {n2}")


def _warn_if_overlap(paths: List[Path]) -> None:
    """
    Advisory-only overlap check using data timestamps in the resulting files.
    """
    ranges: List[Tuple[str, pd.Timestamp, pd.Timestamp]] = []
    for p in paths:
        df = read_ts(p, force_regular=False, freq=None)
        ranges.append((p.name, df.index.min(), df.index.max()))

    for i in range(len(ranges)):
        for j in range(i + 1, len(ranges)):
            n1, a1, b1 = ranges[i]
            n2, a2, b2 = ranges[j]
            if a1 <= b2 and a2 <= b1:
                logger.warning(f"post-slice overlap remains between {n1} and {n2}")


def _legacy_supersession_paths(paths: List[Path], *, dry_run: bool) -> None:
    """
    Legacy supersession on an explicit list of paths.

    Deletes files whose filename year span is fully superseded by another file
    representing the same series. Comparison is restricted to same-series keys
    (agency/param/station_id/subloc).
    """
    if not paths:
        return

    repodir = paths[0].parent
    allfiles = [p.name for p in paths]

    allmeta = [interpret_fname(fname) for fname in allfiles]
    already_checked = set()
    superseded: List[str] = []

    for meta in allmeta:
        if meta["filename"] in already_checked:
            continue

        near_misses = []
        for meta2 in allmeta:
            if meta is meta2:
                continue

            same_series = (
                meta["agency"] == meta2["agency"]
                and meta["param"] == meta2["param"]
                and meta["station_id"] == meta2["station_id"]
                and meta["subloc"] == meta2["subloc"]
            )
            if same_series:
                near_misses.append(meta2)
                already_checked.add(meta2["filename"])

        already_checked.add(meta["filename"])
        if not near_misses:
            continue

        near_misses.append(meta)

        for m in near_misses:
            superseding = []
            for m2 in near_misses:
                if m is m2:
                    continue
                if m2["syear"] <= m["syear"] and m2["eyear"] >= m["eyear"]:
                    superseding.append(m2)

            if superseding:
                fn = m["filename"]
                logger.info(f"superseded: {fn}")
                for s in superseding:
                    logger.info(f"  superseded by {s['filename']}")
                if not dry_run:
                    os.remove(repodir / fn)
                superseded.append(fn)

    if superseded:
        logger.info("Superseded files:")
        for s in superseded:
            logger.info(s)



# --------------------------------------------------------------------------------------
# Click CLI (simple, direct)
# --------------------------------------------------------------------------------------

import click  # module-level import is fine; this is a CLI-facing module

@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.argument("pat", type=str)
@click.option(
    "--yaml",
    "yaml_path",
    type=click.Path(exists=False, dir_okay=False, path_type=Path),
    default=None,
    help="YAML rationalization config. If omitted, run legacy-only supersession logic.",
)
@click.option(
    "--root-dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=None,
    help="Root directory for evaluating YAML rule patterns (if --yaml is provided).",
)
@click.option("--dry-run", is_flag=True, help="Do not modify/delete files; only log actions.")
@click.option(
    "--no-warn-overlap",
    is_flag=True,
    help="Disable advisory warning if overlaps remain after YAML slicing.",
)
@click.option(
    "-v",
    "--verbose",
    count=True,
    help="Increase logging verbosity (-v INFO, -vv DEBUG).",
)
def rationalize_time_partitions_cli(
    pat: str,
    yaml_path: Path | None,
    root_dir: Path | None,
    dry_run: bool,
    no_warn_overlap: bool,
    verbose: int,
) -> None:
    """
    CLI entry point for rationalizing time-partitioned instrument files.

    This is intended for testing / ad-hoc runs. Wiring into __main__ / entry points
    is handled elsewhere.
    """
    # Interpret `pat` relative to --root-dir (same as YAML patterns)
    from dms_datastore.logging_config import configure_logging, resolve_loglevel
    level, console = resolve_loglevel(verbose=verbose)
    configure_logging(
        package_name="dms_datastore",
        logfile_prefix="rationalize_time_partitions",
        level=level,
        console=console,
        clear_handlers=True,   # keeps it re-entrant like the rest of the package
        propagate=False,
    )


    rationalize_time_partitions(
        pat,
        yaml_path=yaml_path,
        root_dir=root_dir,
        dry_run=dry_run,
        warn_on_remaining_overlap=not no_warn_overlap,
    )