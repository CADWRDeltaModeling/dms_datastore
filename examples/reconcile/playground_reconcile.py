#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Playground / walkthrough for reconcile_data.py

This script builds a miniature repo layout on disk, mutates it through a few
realistic workflows, and prints the state at each step.

It is intentionally "observable" (prints plans, actions, and small diffs) rather
than being a pure assert-based unit test suite.

Directory layout created (relative to --root):
    test_repos/
      staging/
        formatted/
        screened/
      repo/
        formatted/
        screened/
      user/
        screened/

Run:
    python playground_reconcile.py --root . --reset
"""

from __future__ import annotations

import argparse
import hashlib
import os
import shutil
from pathlib import Path
from typing import Dict, Tuple

import numpy as np
import pandas as pd

# In your package, these are:
#   from dms_datastore.write_ts import write_ts_csv
#   from dms_datastore.reconcile_data import update_repo, update_flagged_data
#
# Here, we assume this playground sits alongside your package environment.
from dms_datastore.write_ts import write_ts_csv
from dms_datastore.reconcile_data import update_repo, update_flagged_data


NOW = pd.Timestamp(
    "2026-02-08"
)  # fixed "now" for deterministic age windows in the demo
FFMT = "%.2f"


def _root_from_dirs(dirs: Dict[str, Path]) -> Path:
    """Return the directory that contains 'test_repos'.

    This is robust to where the playground lives (examples/reconcile, etc.).

    Parameters
    ----------
    dirs : Dict[str, Path]
        Dictionary mapping directory keys to Path objects, must contain 'repo_formatted'.

    Returns
    -------
    Path
        The root directory containing 'test_repos'.

    Raises
    ------
    RuntimeError
        If 'test_repos' cannot be located in the path.
    """
    p = dirs["repo_formatted"]
    # Traverse up the path hierarchy to find 'test_repos' directory.
    # p is .../test_repos/repo/formatted, so we walk up parents checking each name.
    for parent in p.parents:
        if parent.name == "test_repos":
            return parent.parent
    raise RuntimeError(f"Could not locate 'test_repos' in path: {p}")


def archive_repo(dirs: Dict[str, Path], label: str) -> Path:
    """Copy test_repos/repo -> test_repos/archive/<label>/repo (overwrite if exists).

    Parameters
    ----------
    dirs : Dict[str, Path]
        Dictionary of directory paths; used to locate the repository root.
    label : str
        Subdirectory name under archive/ (e.g., 'A_header_only', 'B_recent_value_changed').

    Returns
    -------
    Path
        The path to the archived repo directory created or overwritten.
    """
    root = _root_from_dirs(dirs)
    repo = root / "test_repos" / "repo"
    arch_repo = root / "test_repos" / "archive" / label / "repo"
    if arch_repo.exists():
        shutil.rmtree(arch_repo)
    arch_repo.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(repo, arch_repo)
    return arch_repo


def snapshot_manifest(repo_dir: Path) -> Dict[str, Tuple[int, float]]:
    """Map relative file path -> (size_bytes, mtime).

    Captures a lightweight snapshot of a repository for change detection.

    Parameters
    ----------
    repo_dir : Path
        Root directory of the repository to snapshot.

    Returns
    -------
    Dict[str, Tuple[int, float]]
        Dictionary mapping relative CSV file paths to (size_bytes, modification_time) tuples.
    """
    out: Dict[str, Tuple[int, float]] = {}
    for p in repo_dir.rglob("*.csv"):
        rel = str(p.relative_to(repo_dir)).replace("\\", "/")
        st = p.stat()
        out[rel] = (st.st_size, st.st_mtime)
    return out


def show_repo_changes(before_repo: Path, after_repo: Path, *, limit: int = 30) -> None:
    """
    Print added/removed/changed CSV files between two repo snapshots.

    This is intentionally shallow (file-level), so you can then open the files.

    Parameters
    ----------
    before_repo : Path
        Path to the repository snapshot to compare from.
    after_repo : Path
        Path to the repository snapshot to compare to.
    limit : int, optional
        Maximum number of files to display per category (default: 30).

    Returns
    -------
    None
        Prints diff summary to stdout.
    """
    b = snapshot_manifest(before_repo)
    a = snapshot_manifest(after_repo)

    # Compute set operations to identify files in each category
    added = sorted(set(a) - set(b))
    removed = sorted(set(b) - set(a))
    changed = sorted(k for k in set(a) & set(b) if a[k] != b[k])

    print("\nRepo diff summary:")
    print(f"  before: {before_repo}")
    print(f"  after : {after_repo}")
    print(f"  added  : {len(added)}")
    print(f"  removed: {len(removed)}")
    print(f"  changed: {len(changed)}")

    def _print_list(title, items):
        if not items:
            return
        print(f"\n  {title}:")
        for x in items[:limit]:
            print("   -", x)
        if len(items) > limit:
            print(f"   ... (+{len(items) - limit} more)")

    _print_list("ADDED", added)
    _print_list("REMOVED", removed)
    _print_list("CHANGED", changed)


def stable_u01(key: str) -> float:
    """Generate a stable uniform [0, 1) random number from a string key.

    Uses SHA256 hashing to ensure deterministic, reproducible random values
    from the same key. This matches the reconcile_data._stable_u01 implementation
    and is used here to craft deterministic test examples.

    Parameters
    ----------
    key : str
        Input string to hash.

    Returns
    -------
    float
        A deterministic uniform random number in [0, 1) derived from the hash of key.
    """
    digest = hashlib.sha256(key.encode("utf-8")).digest()
    u = int.from_bytes(digest[:8], "big", signed=False)
    return u / float(2**64)


def tree(root: Path) -> None:
    """Print a tree-like directory structure showing all files.

    Recursively walks the directory and prints all file paths relative to root.
    Directories are traversed but not displayed separately, only files are shown.

    Parameters
    ----------
    root : Path
        Root directory to start the tree traversal from.

    Returns
    -------
    None
        Prints tree structure to stdout.
    """
    print(f"\n== Tree: {root} ==")
    for p in sorted(root.rglob("*")):
        rel = p.relative_to(root)
        if p.is_dir():
            continue
        print(f"  {rel}")


def read_data_section(path: Path) -> pd.DataFrame:
    """Read CSV file with optional header comments and datetime index.

    Parameters
    ----------
    path : Path
        Path to the CSV file to read. Lines starting with '#' are treated as comments.

    Returns
    -------
    pd.DataFrame
        DataFrame with 'datetime' column parsed as datetimes and set as index.
    """
    return pd.read_csv(
        path, comment="#", parse_dates=["datetime"], index_col="datetime"
    )


def show_head(path: Path, n: int = 5) -> None:
    """Display head and tail of a CSV file for inspection.

    Parameters
    ----------
    path : Path
        Path to the CSV file to display.
    n : int, optional
        Number of rows to display from head and tail (default: 5).

    Returns
    -------
    None
        Prints head and tail sections to stdout.
    """
    df = read_data_section(path)
    print(f"\n--- {path} (head {n}) ---")
    print(df.head(n))
    print(f"--- {path} (tail {n}) ---")
    print(df.tail(n))


def make_formatted_series(
    start: str,
    end: str,
    *,
    freq: str = "D",
    seed: int = 0,
    colname: str = "value",
) -> pd.DataFrame:
    """Generate a synthetic univariate time series with normal random values.

    Parameters
    ----------
    start : str
        Start date string in format parseable by pd.date_range (e.g., '2020-01-01').
    end : str
        End date string in format parseable by pd.date_range.
    freq : str, optional
        Frequency for date range (default: 'D' for daily). See pandas frequency aliases.
    seed : int, optional
        Random seed for reproducibility (default: 0).
    colname : str, optional
        Name of the value column (default: 'value').

    Returns
    -------
    pd.DataFrame
        DataFrame with datetime index and a single column of normal random values.
        Index name is 'datetime'.
    """
    idx = pd.date_range(start, end, freq=freq)
    rng = np.random.default_rng(seed)
    vals = rng.normal(loc=0.0, scale=1.0, size=len(idx))
    df = pd.DataFrame({colname: vals}, index=idx)
    df.index.name = "datetime"
    return df


def make_multivariate_series(
    start: str,
    end: str,
    *,
    freq: str = "D",
    seed: int = 0,
) -> pd.DataFrame:
    """Generate a synthetic multivariate time series with two normal random columns.

    Parameters
    ----------
    start : str
        Start date string in format parseable by pd.date_range (e.g., '2020-01-01').
    end : str
        End date string in format parseable by pd.date_range.
    freq : str, optional
        Frequency for date range (default: 'D' for daily). See pandas frequency aliases.
    seed : int, optional
        Random seed for reproducibility (default: 0).

    Returns
    -------
    pd.DataFrame
        DataFrame with datetime index and two columns: 'a' (unit variance) and 'b' (10x variance).
        Index name is 'datetime'.
    """
    idx = pd.date_range(start, end, freq=freq)
    rng = np.random.default_rng(seed)
    df = pd.DataFrame(
        {
            "a": rng.normal(size=len(idx)),
            "b": rng.normal(size=len(idx)) * 10.0,
        },
        index=idx,
    )
    df.index.name = "datetime"
    return df


def make_screened_from_values(
    values: pd.DataFrame, flag_rule: str = "none"
) -> pd.DataFrame:
    """Create a screened DataFrame with value and user_flag columns.

    Combines a values DataFrame with a user_flag column based on the specified flag_rule.
    This mimics the structure produced by autoscreen workflows.

    Parameters
    ----------
    values : pd.DataFrame
        Single-column DataFrame with column name 'value' and datetime index.
    flag_rule : str, optional
        Rule for setting user_flag values (default: 'none'):
        - 'none': all flags set to pd.NA (blank)
        - 'spikes': flag set to 1 where abs(value) > 1.5, otherwise pd.NA

    Returns
    -------
    pd.DataFrame
        DataFrame with columns ['value', 'user_flag'] and datetime index.
        user_flag is Int64 nullable dtype.

    Raises
    ------
    ValueError
        If values does not have exactly one column named 'value', or if flag_rule is invalid.
    """
    v = values.copy()
    if list(v.columns) != ["value"]:
        raise ValueError("values must be single-column with name 'value'")
    out = pd.DataFrame(index=v.index)
    out["value"] = v["value"]
    # Apply the specified flagging rule
    if flag_rule == "none":
        # Initialize all flags as blank (pd.NA)
        out["user_flag"] = pd.Series(pd.NA, index=v.index, dtype="Int64")
    elif flag_rule == "spikes":
        # Flag outliers where absolute value exceeds threshold
        out["user_flag"] = pd.Series(pd.NA, index=v.index, dtype="Int64")
        out.loc[v["value"].abs() > 1.5, "user_flag"] = 1
    else:
        raise ValueError(flag_rule)
    out.index.name = "datetime"
    return out


def write_sharded(df: pd.DataFrame, path: Path, meta: str, block_size: int = 1) -> None:
    """Write a time series DataFrame to sharded CSV files, one per year.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with datetime index to write.
    path : Path
        Base path for output files. Files will be named with year suffix (e.g., path_2020.csv).
    meta : str
        Metadata header string to prepend to CSV files.
    block_size : int, optional
        Block size parameter for write_ts_csv (default: 1).

    Returns
    -------
    None
        Writes files to disk.
    """
    write_ts_csv(
        df,
        str(path),
        metadata=meta,
        chunk_years=True,
        block_size=block_size,
        float_format=FFMT,
    )


def setup_base(root: Path) -> Dict[str, Path]:
    """Create base test directory structure and populate with seed time series.

    Initializes a multi-tier repository structure with:
    - Staging formatted/screened directories
    - Main repo formatted/screened directories
    - User screened directory

    Populates with representative time series data at different frequencies and
    time periods to support various reconciliation workflows.

    Parameters
    ----------
    root : Path
        Root directory where 'test_repos/' will be created.

    Returns
    -------
    Dict[str, Path]
        Dictionary mapping directory keys to their Path objects:
        - staging_formatted, staging_screened
        - repo_formatted, repo_screened
        - user_screened
    """
    dirs = {
    "staging_formatted": root / "test_repos" / "staging" / "formatted",
    "staging_screened": root / "test_repos" / "staging" / "screened",
    "repo_formatted": root / "test_repos" / "repo" / "formatted",
    "repo_screened": root / "test_repos" / "repo" / "screened",
    "user_screened": root / "test_repos" / "user" / "screened",

    # NEW:
    "staging_processed": root / "test_repos" / "staging" / "processed",
    "repo_processed": root / "test_repos" / "repo" / "processed",
    }

    # Create all required directories in the hierarchy
    for d in dirs.values():
        d.mkdir(parents=True, exist_ok=True)

    # --- Formatted (univariate), long POR so we have >10y, 3-10y, and recent shards
    # Use a filename that fits your naming conventions.
    fname = "cdec_foo_123_flow.csv"
    base_series = make_formatted_series("2009-01-01", "2015-12-31", seed=1)
    meta = "station_id: foo\nparam: flow\nunit: ft^3/s\n"
    # Write to repo and staging; initially identical
    write_sharded(base_series, dirs["repo_formatted"] / fname, meta)
    write_sharded(base_series, dirs["staging_formatted"] / fname, meta)

    # --- Formatted (multivariate) (treat like "processed" style but still sharded)
    fname_mv = "cdec_bar_456_temp.csv"
    mv = make_multivariate_series("2018-01-01", "2025-12-31", seed=2)
    meta_mv = "station_id: bar\nparam: temp\nunit: deg_c\n"
    # Both repo and staging get identical multivariate data initially
    write_sharded(mv, dirs["repo_formatted"] / fname_mv, meta_mv)
    write_sharded(mv, dirs["staging_formatted"] / fname_mv, meta_mv)

    # --- Screened (repo): based on a short recent year with some explicit user overrides
    # Create 2024 data and generate autoscreen flags based on spike detection
    fname_sc = "cdec_baz_789_ec_2024.csv"
    vals_2024 = make_formatted_series("2024-01-01", "2024-12-31", seed=3)
    vals_2024.columns = ["value"]
    screened_repo = make_screened_from_values(vals_2024, flag_rule="spikes")

    # Inject some explicit user overrides (0) and a user re-assert anomaly (1)
    # These simulate a user manually reviewed and corrected the autoscreen flags
    screened_repo["user_flag"] = screened_repo["user_flag"].astype("Int64")
    # pick deterministic indices
    ix = screened_repo.index
    screened_repo.loc[ix[10], "user_flag"] = 0  # explicit "not anomaly"
    screened_repo.loc[ix[20], "user_flag"] = 1  # explicit anomaly
    screened_repo.loc[ix[30], "user_flag"] = 0

    meta_sc = "station_id: baz\nparam: ec\nunit: uS/cm\nscreen: true\n"
    write_sharded(screened_repo, dirs["repo_screened"] / fname_sc, meta_sc)

    # Staging autoscreen output: same values, but will start as blank/1 only
    # Autoscreen never produces explicit 0 flags; only blank or 1 (anomaly)
    screened_staged = make_screened_from_values(vals_2024, flag_rule="spikes")
    # emulate autoscreen: it will never contain 0; ensure blanks for user overrides
    screened_staged.loc[ix[10], "user_flag"] = pd.NA
    screened_staged.loc[ix[30], "user_flag"] = pd.NA
    write_sharded(screened_staged, dirs["staging_screened"] / fname_sc, meta_sc)

    # --- Processed (unsharded), crosses annual boundary
    # A single file spanning across year boundary (no _YYYY suffix), so it becomes "__single__"
    fname_p = "cdec_proc_foo_123_flow_processed.csv"
    proc = make_formatted_series("2024-07-01", "2025-06-30", seed=7)
    meta_p = "station_id: foo\nparam: flow\nunit: ft^3/s\ntier: processed\nnote: unsharded-cross-year\n"

    # Write unsharded (chunk_years=False) to repo and staging; initially identical
    write_ts_csv(proc, str(dirs["repo_processed"] / fname_p), metadata=meta_p, chunk_years=False, float_format=FFMT)
    write_ts_csv(proc, str(dirs["staging_processed"] / fname_p), metadata=meta_p, chunk_years=False, float_format=FFMT)


    return dirs


def step_header_only_change(dirs: Dict[str, Path]) -> None:
    """Demonstrate that header-only changes in staging trigger no update actions.

    Parameters
    ----------
    dirs : Dict[str, Path]
        Dictionary of directory paths for the test repository.

    Returns
    -------
    None
        Prints plan and actions to stdout, archives state before and after.
    """
    # We rewrite ONE staging shard file with new header metadata but identical data.
    # Find first CSV and re-write with updated metadata, keeping data unchanged
    any_file = sorted(dirs["staging_formatted"].glob("*.csv"))[0]
    df = read_data_section(any_file)
    meta = "station_id: foo\nparam: flow\nunit: ft^3/s\nnote: header-only-change\n"
    # Write without chunk_years to avoid renaming; overwrite exact file for the demo
    write_ts_csv(df, str(any_file), metadata=meta, chunk_years=False, float_format=FFMT)

    print("\nSTEP A) Header-only change in staging formatted (no data change expected)")
    arch = archive_repo(dirs, "A_header_only")

    actions = update_repo(
        str(dirs["staging_formatted"]), str(dirs["repo_formatted"]), now=NOW, plan=True
    )
    print("Plan actions:", actions)
    actions_apply = update_repo(
        str(dirs["staging_formatted"]), str(dirs["repo_formatted"]), now=NOW, plan=False
    )
    print("Apply actions:", actions_apply)
    root = _root_from_dirs(dirs)
    show_repo_changes(arch, root / "test_repos" / "repo")


def step_recent_value_append(dirs: Dict[str, Path]) -> None:
    """Demonstrate value changes in a recent shard trigger splice_write action.

    Parameters
    ----------
    dirs : Dict[str, Path]
        Dictionary of directory paths for the test repository.

    Returns
    -------
    None
        Prints plan and actions to stdout, archives state before and after.
    """
    # Modify the 2015 shard in staging (find it dynamically: naming depends on chunking base name).
    # This represents a real-world scenario where historical data is corrected
    matches = sorted(dirs["staging_formatted"].glob("*_2015.csv"))
    if not matches:
        raise FileNotFoundError(
            "Could not find a 2015 shard in staging_formatted (expected '*_2015.csv')."
        )
    sp = matches[0]
    rp = dirs["repo_formatted"] / sp.name

    sdf = read_data_section(sp)
    # Modify values late in the year to trigger a change detection
    sdf.iloc[-2:, 0] = sdf.iloc[-2:, 0] + 5.0
    meta = "station_id: foo\nparam: flow\nunit: ft^3/s\nnote: late-year tweak\n"
    write_ts_csv(sdf, str(sp), metadata=meta, chunk_years=False, float_format=FFMT)

    arch = archive_repo(dirs, "B_recent_value_changed")
    print(
        "\nSTEP B) Recent-ish shard values changed in staging (should plan splice_write)"
    )
    actions = update_repo(
        str(dirs["staging_formatted"]),
        str(dirs["repo_formatted"]),
        now=NOW,
        recent_years=3,
        p10=1.0,
        p3=1.0,
        plan=True,
    )
    for a in actions:
        print("  ", a)
    actions_apply = update_repo(
        str(dirs["staging_formatted"]),
        str(dirs["repo_formatted"]),
        now=NOW,
        recent_years=3,
        p10=1.0,
        p3=1.0,
        plan=False,
    )
    print(f"Applied {len(actions_apply)} actions.")
    show_head(rp)
    root = _root_from_dirs(dirs)
    show_repo_changes(arch, root / "test_repos" / "repo")


def step_old_history_change_triggers_escalation(dirs: Dict[str, Path]) -> None:
    """Demonstrate that old shard changes trigger escalation to include recent shards.

    When historical data is modified, the reconciliation system escalates the action
    to include recent-window shards for full context, rather than just the changed shard.

    Parameters
    ----------
    dirs : Dict[str, Path]
        Dictionary of directory paths for the test repository.

    Returns
    -------
    None
        Prints plan and actions to stdout, archives state before and after.
    """
    # Force old-history check by setting p10/p3=1.0 in this demo.
    # This simulates a data correction deep in the historical archive
    matches = sorted(dirs["staging_formatted"].glob("*_2009.csv"))
    if not matches:
        print("Could not find 2009 shard; skipping old-history escalation demo.")
        return
    old_file = matches[0]

    df = read_data_section(old_file)
    # Change the first value to trigger escalation logic
    df.iloc[0, 0] = df.iloc[0, 0] + 10.0
    write_ts_csv(
        df,
        str(old_file),
        metadata="station_id: foo\nparam: flow\nunit: ft^3/s\nnote: old-history tweak\n",
        chunk_years=False,
        float_format=FFMT,
    )

    arch = archive_repo(dirs, "C_old_history_change")
    print(
        "\nSTEP C) Old shard changed in staging; escalation should include recent window shards too"
    )
    actions = update_repo(
        str(dirs["staging_formatted"]),
        str(dirs["repo_formatted"]),
        now=NOW,
        recent_years=3,
        p10=1.0,
        p3=1.0,
        plan=True,
    )
    for a in actions:
        if a.series_id.endswith("flow_*.csv"):
            print("  ", a)
    root = _root_from_dirs(dirs)
    show_repo_changes(arch, root / "test_repos" / "repo")


def step_screened_autoscreen_merge(dirs: Dict[str, Path]) -> None:
    """Demonstrate screened data merge logic: autoscreen flags meeting repo flags.

    Shows how blank vs 1 flags are resolved when comparing staged autoscreen output
    with existing repo screening, including handling of value changes.

    Parameters
    ----------
    dirs : Dict[str, Path]
        Dictionary of directory paths for the test repository.

    Returns
    -------
    None
        Prints merge actions and results to stdout, archives state before and after.
    """

    arch = archive_repo(dirs, "D_autoscreen_merge")
    print("\nSTEP D) Screened autoscreen -> repo merge")

    # Mutate staging screened to demonstrate merge rules:
    # 1) Make staged blank where repo has 1 (should keep repo 1 if values unchanged)
    # 2) Add a staged 1 where repo blank (should become 1)
    # 3) Change a value at a repo 0 point to show staged value wins (and clears explicit 0 flag)
    sp = dirs["staging_screened"] / "cdec_baz_789_ec_2024.csv"
    rp = dirs["repo_screened"] / "cdec_baz_789_ec_2024.csv"
    s = read_data_section(sp)
    r = read_data_section(rp)

    # Force some deterministic merge test points
    ix = s.index
    # where repo has 1, make staged blank - tests conservative retention of flags
    s.loc[ix[20], "user_flag"] = pd.NA
    # where repo blank, add staged 1 - tests propagating new anomalies
    s.loc[ix[40], "user_flag"] = 1

    # Change a value at a repo 0 point: staged should win, dropping repo's explicit 0
    # This tests that value changes take precedence in autoscreen context
    s.loc[ix[10], "value"] = s.loc[ix[10], "value"] + 99.0
    # Keep staged flag blank to show rule "values changed => staged wins flag"
    s.loc[ix[10], "user_flag"] = pd.NA

    write_ts_csv(
        s,
        str(sp),
        metadata="station_id: baz\nparam: ec\nunit: uS/cm\nscreen: true\nnote: staged edits\n",
        chunk_years=False,
        float_format=FFMT,
    )

    actions = update_flagged_data(
        str(dirs["staging_screened"]), str(dirs["repo_screened"]), plan=True
    )
    for a in actions:
        print("  ", a)
    update_flagged_data(
        str(dirs["staging_screened"]), str(dirs["repo_screened"]), plan=False
    )

    print("\nRepo screened after merge (selected rows):")
    merged = read_data_section(rp)
    show = merged.loc[ix[[10, 20, 40]], :]
    print(show)
    root = _root_from_dirs(dirs)
    show_repo_changes(arch, root / "test_repos" / "repo")


def step_user_checkout_and_bad_merge_warning(dirs: Dict[str, Path]) -> None:
    """Demonstrate user return behavior with value_reference='repo' and conflict handling.

    Simulates a user checking out repo data, editing flags (explicit 0/1), while repo
    values change independently. Shows how value mismatches prevent user edits from
    being applied to protect data integrity.

    Parameters
    ----------
    dirs : Dict[str, Path]
        Dictionary of directory paths for the test repository.

    Returns
    -------
    None
        Prints user edit results to stdout, archives state before and after.
    """
    arch = archive_repo(dirs, "E_user_checkout_and_bad_merge_warning")
    print(
        "\nSTEP E) User checkout + repo changed values: user edits apply only where values match"
    )

    rp = dirs["repo_screened"] / "cdec_baz_789_ec_2024.csv"
    up = dirs["user_screened"] / "cdec_baz_789_ec_2024.csv"

    # User "checks out" a copy of repo file to work with
    shutil.copy2(rp, up)

    # User makes explicit flag edits (0 = not anomaly, 1 = anomaly) without changing values
    u = read_data_section(up)
    ix = u.index
    u.loc[ix[50], "user_flag"] = 0
    u.loc[ix[60], "user_flag"] = 1
    write_ts_csv(
        u,
        str(up),
        metadata="station_id: baz\nparam: ec\nunit: uS/cm\nscreen: true\nnote: user edits\n",
        chunk_years=False,
        float_format=FFMT,
    )

    # Meanwhile, repo values changed after user checkout (nightly update or correction)
    # This creates a value mismatch at index 50 that should invalidate the user edit there
    r = read_data_section(rp)
    r.loc[ix[50], "value"] = r.loc[ix[50], "value"] + 7.0
    write_ts_csv(
        r,
        str(rp),
        metadata="station_id: baz\nparam: ec\nunit: uS/cm\nscreen: true\nnote: repo value changed post-checkout\n",
        chunk_years=False,
        float_format=FFMT,
    )

    print(
        "User edited flags at t50 (set 0) and t60 (set 1). Repo changed value at t50 after checkout."
    )
    print(
        "Applying user return with value_reference='repo' and explicit_conflict='prefer_staged'..."
    )

    # Apply user return with repo value reference and conflict handling
    # This means: repo values are authoritative; user flags apply only where values match
    # At t50: values diverged, so user edit is ignored (data integrity preserved)
    # At t60: values match, so user flag (1) is applied
    actions = update_flagged_data(
        str(dirs["user_screened"]),
        str(dirs["repo_screened"]),
        value_reference="repo",
        explicit_conflict="prefer_staged",
        plan=True,
    )
    for a in actions:
        print("  ", a)

    update_flagged_data(
        str(dirs["user_screened"]),
        str(dirs["repo_screened"]),
        value_reference="repo",
        explicit_conflict="prefer_staged",
        plan=False,
    )

    merged = read_data_section(rp)
    print("\nRepo screened after user return (selected rows):")
    show = merged.loc[ix[[50, 60]], :]
    print(show)
    root = _root_from_dirs(dirs)
    show_repo_changes(arch, root / "test_repos" / "repo")


def step_backfill_lower_priority_archive(dirs: Dict[str, Path]) -> None:
    """Demonstrate backfilling earlier history from a lower-priority source.

    This simulates finding an archive (or auxiliary feed) that extends the POR,
    but should not override the repo where the repo already has values.

    Uses update_repo(..., prefer="repo") to keep repo as authoritative and only
    fill gaps / earlier history.
    """
    print(
        "\nSTEP F) Supplement with lower priority archive or realtime data: prefer='repo'"
    )

    # --- Mutate STAGING: create an "archive" series that extends earlier than the repo
    # The base setup has foo flow from 2009-2015. We'll fabricate a 2007-2008 archive.
    fname = "cdec_foo_123_flow.csv"
    meta = "station_id: foo\nparam: flow\nunit: ft^3/s\nnote: special-supply-archive\n"

    early = make_formatted_series("2007-01-01", "2008-12-31", seed=99)
    write_sharded(early, dirs["staging_formatted"] / fname, meta)

    # --- Archive repo BEFORE applying reconcile (same pattern as steps A–E)
    arch = archive_repo(dirs, "F_backfill_lower_priority_archive")

    # --- Plan then apply
    actions = update_repo(
        str(dirs["staging_formatted"]),
        str(dirs["repo_formatted"]),
        prefer="repo",
        now=NOW,
        plan=True,
    )
    for a in actions:
        print("  ", a)

    actions_apply = update_repo(
        str(dirs["staging_formatted"]),
        str(dirs["repo_formatted"]),
        prefer="repo",
        now=NOW,
        plan=False,
    )
    print(f"Applied {len(actions_apply)} actions.")

    # --- Show repo diffs AFTER apply (same as steps A–E)
    root = _root_from_dirs(dirs)
    show_repo_changes(arch, root / "test_repos" / "repo")

    # Optional: show a representative shard head if you want something concrete on screen
    # (this mirrors what STEP B does)
    # e.g., show_head(dirs["repo_formatted"] / "cdec_foo_123_flow_2007.csv")


def step_processed_unsharded_append_repo_priority(dirs: Dict[str, Path]) -> None:
    """Processed/unsharded file update: append new data but keep existing repo values.

    This simulates adding new timestamps to a single-file processed product while
    ensuring the existing repo record remains authoritative on overlaps.
    """
    print(
        '\nSTEP G) Processed unsharded append, prioritize what was there: prefer="repo"'
    )

    sp = dirs["staging_processed"] / "cdec_proc_foo_123_flow_processed.csv"
    rp = dirs["repo_processed"] / sp.name

    # Mutate staging: append new dates AND deliberately tweak an overlapping value
    sdf = read_data_section(sp)

    # 1) tweak overlap (should NOT override repo when prefer="repo")
    sdf.iloc[-3:, 0] = sdf.iloc[-3:, 0] + 123.0

    # 2) append new timestamps (should be added)
    tail_start = (sdf.index.max() + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    tail = make_formatted_series(tail_start, "2025-07-31", seed=77)  # new month
    sdf2 = pd.concat([sdf, tail])

    meta = "station_id: foo\nparam: flow\nunit: ft^3/s\ntier: processed\nnote: appended-new-data\n"
    write_ts_csv(sdf2, str(sp), metadata=meta, chunk_years=False, float_format=FFMT)

    arch = archive_repo(dirs, "G_processed_unsharded_append_repo_priority")

    # Plan/apply against processed dirs, with repo priority
    actions = update_repo(
        str(dirs["staging_processed"]),
        str(dirs["repo_processed"]),
        prefer="repo",
        now=NOW,
        plan=True,
    )
    for a in actions:
        print("  ", a)

    actions_apply = update_repo(
        str(dirs["staging_processed"]),
        str(dirs["repo_processed"]),
        prefer="repo",
        now=NOW,
        plan=False,
    )
    print(f"Applied {len(actions_apply)} actions.")

    # Show result: should have appended new dates; overlap should remain repo’s values
    show_head(rp, n=8)

    root = _root_from_dirs(dirs)
    show_repo_changes(arch, root / "test_repos" / "repo")


def main() -> None:
    """Run the complete reconciliation workflow demonstration.

    Executes all steps of the playground demo:
    1. Header-only changes (no data impact)
    2. Recent value changes (splice-write action)
    3. Old history changes (escalation logic)
    4. Screened autoscreen merge (flag reconciliation)
    5. User checkout with repo value changes (conflict detection)

    Returns
    -------
    None
        Prints step-by-step output and file tree to stdout.
    """
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--root", default=".", help="Root directory where test_repos/ will be created"
    )
    ap.add_argument(
        "--reset", action="store_true", help="Delete any existing test_repos/ first"
    )
    args = ap.parse_args()

    root = Path(args.root).resolve()
    base = root / "test_repos"
    # Clean up any prior runs if requested
    if args.reset and base.exists():
        shutil.rmtree(base)

    # Initialize test repository structure and seed data
    dirs = setup_base(root)
    tree(root / "test_repos")

    # Execute each demonstration step in order
    step_header_only_change(dirs)
    step_recent_value_append(dirs)
    step_old_history_change_triggers_escalation(dirs)
    step_screened_autoscreen_merge(dirs)
    step_user_checkout_and_bad_merge_warning(dirs)
    step_backfill_lower_priority_archive(dirs)
    step_processed_unsharded_append_repo_priority(dirs)
    print("\nDone. Inspect files under:", root / "test_repos")


if __name__ == "__main__":
    main()
