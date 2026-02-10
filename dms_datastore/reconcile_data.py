#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Time-series reconciliation utilities.

This module provides "content-smart" reconciliation workflows intended to replace
name-based directory copying (e.g., compare_directories.compare_dir).

Two public operations are provided:

* update_repo: reconcile staged vs repo data in formatted/processed tiers.
* update_flagged_data: reconcile staged autoscreened vs repo screened tier while
  preserving user overrides encoded in tri-state user_flag.

Design goals
------------
* Avoid rewriting files unless the *data section* is meaningfully different
  (metadata/header churn such as date_formatted should not trigger rewrites).
* Deterministically thin comparisons for older shards (stable pseudo-random by
  series_id + shard), while always inspecting the most recent window.
* For screened data, treat user_flag as tri-state with precedence:
    blank := no-opinion / not-anomaly (implicit)
    1     := anomaly (explicit)
    0     := explicit user override (not anomaly, sticky)
  and apply "staged wins" only when the underlying values changed.

Notes
-----
* This is a first-cut implementation focused on correctness and policy clarity.
  It is written to be strict/fail-fast for malformed input (e.g., column mismatch).
* Sharding support:
  - "..._YYYY.csv" and "..._YYYY_YYYY.csv" are treated as sharded.
  - Files without an apparent year token are treated as a single "__single__" shard.
"""

from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass
from glob import glob
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
from vtools import ts_merge
from dms_datastore.inventory import to_wildcard
from dms_datastore.read_ts import original_header
from dms_datastore.dstore_config import config_file



__all__ = [
    "ReconcileAction",
    "update_repo",
    "update_flagged_data",
]


# -----------------------------
# Action / plan representation
# -----------------------------


@dataclass(frozen=True)
class ReconcileAction:
    """ Planned or executed reconciliation action.

    This is the primary "plan/explain" surface for reconciliation workflows.
    The functions in this module can return actions in a dry-run mode
    (``plan=True``) so callers can review what would change before writing.

    Parameters
    ----------
    series_id : str
        Stable identity for a time series across shards. Typically derived from a
        filename by wildcarding any year token (and optionally wildcarding the
        source slot).
    shard : str
        Shard label, usually a year (``"YYYY"``) or year-range
        (``"YYYY_YYYY"``). Files without a year token are treated as a single
        shard labeled ``"__single__"``.
    action : str
        Action type. Current values include:

        - ``"write"``: write (or copy) staged data into repo.
        - ``"splice_write"``: merge staged and repo by time index and write.

    reason : str
        Human-readable reason string explaining why this action is needed.
        These reason strings are intended for traceability and test fixtures.
    staged_path : str, optional
        Path to the staged (incoming) file, when applicable.
    repo_path : str, optional
        Path to the repo (destination) file, when applicable.

    Notes
    -----
    Actions are intentionally lightweight (strings + paths) so they can be
    serialized or logged without pulling in pandas objects.
    """

    series_id: str
    shard: str
    action: str
    reason: str
    staged_path: Optional[str] = None
    repo_path: Optional[str] = None


# -----------------------------
# Filename / shard helpers
# -----------------------------


_RE_YEAR2 = re.compile(
    r"^(?P<stem>.*)_(?P<syear>\d{4})_(?P<eyear>\d{4})(?P<ext>\..{3,4})$"
)
_RE_YEAR1 = re.compile(r"^(?P<stem>.*)_(?P<year>\d{4})(?P<ext>\..{3,4})$")


def _parse_shard(basename: str) -> Tuple[str, Optional[int]]:
    """ Parse a shard label from a filename.

    Parameters
    ----------
    basename : str
        Base filename (no directory components).

    Returns
    -------
    shard_label : str
        One of:

        - ``"YYYY"`` if the filename matches ``*_YYYY.<ext>``.
        - ``"YYYY_YYYY"`` if the filename matches ``*_YYYY_YYYY.<ext>``.
        - ``"__single__"`` if no shard year token is detected.
    shard_end_year : int or None
        End year for the shard (``YYYY`` for single-year shards; the second year for
        range shards). Returns ``None`` for ``"__single__"``.

    Notes
    -----
    This helper is used for two related purposes:

    1. Grouping related files under a stable series identity.
    2. Age-based sampling for update detection in :func:`update_repo`.
    """
    

    m2 = _RE_YEAR2.match(basename)
    if m2 is not None:
        syear = int(m2.group("syear"))
        eyear = int(m2.group("eyear"))
        return f"{syear}_{eyear}", eyear
    m1 = _RE_YEAR1.match(basename)
    if m1 is not None:
        year = int(m1.group("year"))
        return f"{year}", year
    return "__single__", None


def _series_id_from_name(basename: str, remove_source: bool) -> str:
    """ Compute a stable series identity from a shard filename.

    Parameters
    ----------
    basename : str
        Base filename (no directory components).
    remove_source : bool
        If True, wildcard the "source" slot (if present in the naming convention)
        when forming the identity. This is useful when the same physical series
        can appear with multiple sources and should reconcile as one.

    Returns
    -------
    series_id : str
        Wildcarded identity string for grouping shards of the same logical series.

    Notes
    -----
    This relies on :func:`dms_datastore.inventory.to_wildcard` and falls back to
    treating the filename as its own identity if the naming convention does not
    match.
    """


    try:
        return to_wildcard(basename, remove_source=remove_source)
    except Exception:
        # If a name doesn't match conventions, treat it as its own series.
        return basename


# -----------------------------
# Hash / equality helpers
# -----------------------------


def _hash_data_section(path: str, comment: str = "#") -> str:
    """ Hash the data portion of a time-series CSV.

    This function skips the leading commented YAML-like header (lines beginning
    with ``comment``) and hashes the remaining bytes.

    Parameters
    ----------
    path : str
        Path to a CSV file written in the dms-datastore format (commented header
        followed by a CSV table).
    comment : str, default '#'
        Comment prefix used for header lines.

    Returns
    -------
    digest : str
        Hex-encoded SHA256 digest of the non-header bytes.

    Notes
    -----
    This is used as a fast path to avoid rewriting files when only metadata changes
    (e.g., ``date_formatted`` updates). When hashes differ, the code may fall back
    to parsed-data comparison to ignore benign formatting differences in numeric
    strings.
    """
    h = hashlib.sha256()
    with open(path, "rb") as f:
        # Skip header: lines beginning with comment in text mode.
        # We do this in binary while checking for leading b'#'.
        while True:
            pos = f.tell()
            line = f.readline()
            if not line:
                break
            if line.startswith(comment.encode("utf-8")):
                continue
            # first non-comment line: rewind and hash remaining bytes
            f.seek(pos)
            break
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _stable_u01(key: str) -> float:
    """ Deterministic pseudo-random number in [0, 1).

    Parameters
    ----------
    key : str
        Stable key identifying the item being sampled (e.g., ``series_id|shard|p3``).

    Returns
    -------
    u : float
        Deterministic value uniformly distributed in [0, 1) under the assumption of
        a good cryptographic hash.

    Notes
    -----
    Used to thin historical shard comparisons (e.g., ``p10`` for >10y, ``p3`` for
    3-10y) while keeping behavior reproducible across runs.
    """

    digest = hashlib.sha256(key.encode("utf-8")).digest()
    # Take 8 bytes -> uint64 -> scale.
    u = int.from_bytes(digest[:8], "big", signed=False)
    return u / float(2**64)


def _read_csv_timeseries(path: str) -> pd.DataFrame:
    """ Read a dms-datastore CSV file into a DataFrame.

    Parameters
    ----------
    path : str
        Path to a CSV file with a commented YAML-like header and a ``datetime``
        column.

    Returns
    -------
    df : pandas.DataFrame
        DataFrame indexed by a ``DatetimeIndex``.

    Raises
    ------
    ValueError
        If the file does not produce a DatetimeIndex or contains duplicate
        timestamps.

    Notes
    -----
    This reader forces ``dtype={'user_flag': str}`` so that screened flag columns do
    not become floats due to NA inference. Normalization to nullable Int64 is
    performed separately by :func:`_normalize_flag`.
    """


    df = pd.read_csv(
        path,
        comment="#",
        parse_dates=["datetime"],
        index_col="datetime",
        dtype={"user_flag": str},
    )
    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError(f"Expected datetime index in {path}")
    if df.index.has_duplicates:
        raise ValueError(f"Duplicate timestamps in {path}")
    return df


def _values_equal(
    a: pd.DataFrame, b: pd.DataFrame, *, atol: float, rtol: float
) -> bool:
    """ Compare two aligned DataFrames for equality.

    Parameters
    ----------
    a, b : pandas.DataFrame
        DataFrames assumed to have identical index and columns.
    atol, rtol : float
        Tolerances passed to :func:`numpy.isclose`. If both are zero, uses exact
        equality semantics via :meth:`pandas.DataFrame.equals`.

    Returns
    -------
    equal : bool
        True if the DataFrames are equal (within tolerance).

    Notes
    -----
    This is a numeric comparison helper used after parsing CSVs, primarily to
    distinguish semantic changes from harmless text-formatting differences.
    """


    if a.shape != b.shape:
        return False
    if atol == 0.0 and rtol == 0.0:
        # Strict: pandas compares NaNs as equal in equals().
        return a.equals(b)
    av = a.to_numpy(dtype="float64")
    bv = b.to_numpy(dtype="float64")
    return np.isclose(av, bv, atol=atol, rtol=rtol, equal_nan=True).all()


def _diff_mask(
    a: pd.DataFrame, b: pd.DataFrame, *, atol: float, rtol: float
) -> np.ndarray:
    """ Compute a per-row difference mask for aligned single-column frames.

    Parameters
    ----------
    a, b : pandas.DataFrame
        Single-column DataFrames aligned on the same index.
    atol, rtol : float
        Tolerances for :func:`numpy.isclose`.

    Returns
    -------
    mask : numpy.ndarray of bool
        Boolean array where True indicates a difference at that timestamp.

    Raises
    ------
    ValueError
        If either DataFrame is not single-column.

    Notes
    -----
    Used by screened reconciliation to decide whether a timestamp is in the
    "value-changed/new" set versus the "value-equal" set.
    """
    if a.shape[1] != 1 or b.shape[1] != 1:
        raise ValueError("_diff_mask expects single-column data")
    av = a.iloc[:, 0].to_numpy(dtype="float64")
    bv = b.iloc[:, 0].to_numpy(dtype="float64")
    if atol == 0.0 and rtol == 0.0:
        # exact compare with NaN semantics
        return ~(
            (np.isnan(av) & np.isnan(bv))
            | (np.isfinite(av) & np.isfinite(bv) & (av == bv))
        )
    return ~np.isclose(av, bv, atol=atol, rtol=rtol, equal_nan=True)


# -----------------------------
# Writing (preserve header)
# -----------------------------


def _write_preserving_header(
    *,
    df: pd.DataFrame,
    dest_path: str,
    header_text: str,
    date_format: str = "%Y-%m-%dT%H:%M:%S",
) -> None:
    os.makedirs(os.path.dirname(dest_path) or ".", exist_ok=True)
    if header_text and not header_text.endswith("\n"):
        header_text = header_text + "\n"
    with open(dest_path, "w", newline="\n") as f:
        if header_text:
            f.write(header_text)
        df.to_csv(f, header=True, sep=",", date_format=date_format)


# -----------------------------
# Discovery helpers
# -----------------------------


def _list_csv_files(d: str, pattern: str = "*.csv") -> List[str]:
    return sorted(glob(os.path.join(d, pattern)))


def _index_by_series_and_shard(
    files: Sequence[str], *, remove_source: bool
) -> Dict[str, Dict[str, str]]:
    """ Index files by series identity and shard label.

    Parameters
    ----------
    files : sequence[str]
        File paths.
    remove_source : bool
        Passed to :func:`_series_id_from_name`.

    Returns
    -------
    mapping : dict[str, dict[str, str]]
        Nested mapping ``series_id -> shard_label -> filepath``.

    Notes
    -----
    This is the core discovery structure used by both :func:`update_repo` and
    :func:`update_flagged_data`.
    """
    out: Dict[str, Dict[str, str]] = {}
    for p in files:
        base = os.path.basename(p)
        sid = _series_id_from_name(base, remove_source=remove_source)
        shard, _ = _parse_shard(base)
        out.setdefault(sid, {})[shard] = p
    return out


# -----------------------------
# Flag merge logic (screened)
# -----------------------------


def _normalize_flag(s: pd.Series) -> pd.Series:
    """ Normalize user_flag to {NA, 0, 1} as pandas nullable Int64.

    Accepted input representations:
      - pandas nullable Int64 already (returned as-is)
      - None/NA/NaN/"" (treated as NA)
      - 0/1 as ints
      - 0.0/1.0 as floats (common when CSV is read with default dtype)
      - "0"/"1" (strings, possibly with surrounding whitespace)
    """
    if s.dtype.name == "Int64":
        return s

    x = s.astype("object")
    x = x.where(~x.isna(), None)

    def _coerce(v):
        if v is None:
            return None
        # numeric types first
        if isinstance(v, (int, np.integer)):
            if v in (0, 1):
                return int(v)
            return "__BAD__"
        if isinstance(v, (float, np.floating)):
            # accept integer-like floats 0.0/1.0
            if np.isfinite(v) and float(v).is_integer() and int(v) in (0, 1):
                return int(v)
            return "__BAD__"
        # strings (including things like "0.0" from prior stringification)
        s2 = str(v).strip()
        if s2 in ("", "nan", "NaN", "None"):
            return None
        if s2 in ("0", "1"):
            return int(s2)
        if s2 in ("0.0", "1.0"):
            return int(float(s2))
        return "__BAD__"

    x = x.apply(_coerce)
    bad = x[x == "__BAD__"]
    if len(bad) > 0:
        raise ValueError(f"Invalid user_flag values: {sorted(set(bad.tolist()))}")

    return x.astype("Int64")


def _merge_screened_flags(
    repo: pd.DataFrame,
    staged: pd.DataFrame,
    *,
    atol: float,
    rtol: float,
    value_reference: str,
    explicit_conflict: str,
) -> pd.DataFrame:
    """ Merge screened dataframes (``value`` + ``user_flag``) according to policy.

    Parameters
    ----------
    repo, staged : pandas.DataFrame
        Screened tier dataframes with exactly columns ``['value', 'user_flag']`` and
        a DatetimeIndex.
    atol, rtol : float
        Tolerances for determining whether values are "equal" at a timestamp.
    value_reference : {'repo', 'staged'}
        Which side is authoritative at timestamps where values differ or are new on
        one side. Both the value and flag are taken from this side for those
        timestamps.
    explicit_conflict : {'prefer_repo', 'prefer_staged', 'error'}
        Resolution policy when *both* sides provide explicit flags (0/1) but disagree.

    Returns
    -------
    merged : pandas.DataFrame
        Screened dataframe on the union index.

    Raises
    ------
    ValueError
        If input columns are not exactly ``['value','user_flag']``, if a flag value
        is invalid, or if ``explicit_conflict='error'`` and an explicit conflict is
        encountered.

    Notes
    -----
    This merge is intentionally two-phase:

    1. **Value-changed/new set**: take both value and flag from ``value_reference``.
    This prevents stale checkouts from writing flags against a different value
    record when ``value_reference='repo'``.

    2. **Value-equal set**: values are effectively the same, so only flags are
    reconciled using tri-state precedence: explicit (0/1) beats blank (NA).
    """
    required_cols = ["value", "user_flag"]
    if list(repo.columns) != required_cols or list(staged.columns) != required_cols:
        raise ValueError(
            f"Expected columns {required_cols} in both repo and staged; "
            f"got repo={list(repo.columns)} staged={list(staged.columns)}"
        )

    if value_reference not in ("repo", "staged"):
        raise ValueError("value_reference must be 'repo' or 'staged'")
    if explicit_conflict not in ("prefer_repo", "prefer_staged", "error"):
        raise ValueError(
            "explicit_conflict must be 'prefer_repo', 'prefer_staged', or 'error'"
        )

    idx = repo.index.union(staged.index)
    r = repo.reindex(idx)
    s = staged.reindex(idx)

    # Normalize flags
    rflag = _normalize_flag(r["user_flag"])
    sflag = _normalize_flag(s["user_flag"])

    # Determine where value differs/new:
    # - if one side missing value, treat as "different/new"
    rv = r[["value"]]
    sv = s[["value"]]
    both_have = rv["value"].notna() & sv["value"].notna()

    different = pd.Series(True, index=idx)
    if both_have.any():
        dm = _diff_mask(
            rv.loc[both_have, ["value"]],
            sv.loc[both_have, ["value"]],
            atol=atol,
            rtol=rtol,
        )
        different.loc[both_have] = dm

    # If both missing, treat as "not different"
    both_missing = rv["value"].isna() & sv["value"].isna()
    different.loc[both_missing] = False

    equal = ~different

    # Start output by taking the reference side everywhere (covers "different/new").
    if value_reference == "repo":
        out = r.copy()
    else:
        out = s.copy()

    # For equal-value timestamps: keep repo value to reduce churn, and merge flags.
    if equal.any():
        out.loc[equal, "value"] = rv.loc[equal, "value"]

        rf = rflag.loc[equal]
        sf = sflag.loc[equal]

        # Identify explicit vs blank
        rf_exp = rf.notna()
        sf_exp = sf.notna()

        merged = pd.Series(pd.NA, index=rf.index, dtype="Int64")

        # One-sided explicit beats blank
        merged.loc[rf_exp & ~sf_exp] = rf.loc[rf_exp & ~sf_exp]
        merged.loc[~rf_exp & sf_exp] = sf.loc[~rf_exp & sf_exp]

        # Both blank -> blank (already NA)

        # Both explicit:
        both_exp = rf_exp & sf_exp
        if both_exp.any():
            same = both_exp & (rf == sf)
            merged.loc[same] = rf.loc[same]

            conflict = both_exp & (rf != sf)
            if conflict.any():
                if explicit_conflict == "prefer_repo":
                    merged.loc[conflict] = rf.loc[conflict]
                elif explicit_conflict == "prefer_staged":
                    merged.loc[conflict] = sf.loc[conflict]
                else:
                    t0 = conflict[conflict].index[0]
                    raise ValueError(
                        f"Explicit user_flag conflict at {t0}: repo={rf.loc[t0]} staged={sf.loc[t0]}"
                    )

        # Avoid dtype issues when out["user_flag"] was inferred as float/object
        uf = _normalize_flag(out["user_flag"])
        uf.loc[equal] = merged.astype("Int64")
        out["user_flag"] = uf

    # Ensure dtype
    out["user_flag"] = _normalize_flag(out["user_flag"])
    return out


# -----------------------------
# Public APIs
# -----------------------------


def update_repo(
    staged_dir: str,
    repo_dir: str,
    *,
    pattern: str = "*.csv",
    prefer: str = "staged",
    remove_source: bool = False,
    now: Optional[pd.Timestamp] = None,
    recent_years: int = 3,
    p10: float = 0.05,
    p3: float = 0.15,
    atol: float = 0.0,
    rtol: float = 0.0,
    plan: bool = False,
) -> List[ReconcileAction]:
    """ Reconcile staged vs repo time-series CSV files (formatted/processed tiers).

    This operation is "content-smart" and is intended to replace purely name-based
    directory copying. It uses a combination of data-section hashing, parsed-data
    comparison, and deterministic sampling for older shards to decide when files
    should be rewritten.

    Parameters
    ----------
    staged_dir, repo_dir : str
        Directories containing CSV files for the staged and repo tiers.
    prefer : {'staged', 'repo'}, default 'staged'
        Which side wins on overlapping timestamps when splicing staged and repo
        records.
    remove_source : bool, default False
        If True, wildcard the source slot when building series identities.
    now : pandas.Timestamp, optional
        Reference timestamp used to compute shard ages for sampling. Defaults to
        ``Timestamp.now()``.
    recent_years : int, default 3
        Window of most recent years that is always inspected. If an old shard
        change is detected, these shards are also reconciled (escalation).
    p10, p3 : float, default 0.05, 0.15
        Deterministic sampling probabilities for inspecting shards older than 10
        years and between 3 and 10 years old.
    atol, rtol : float, default 0.0, 0.0
        Tolerances for parsed-data comparisons. If both are zero, comparisons are
        exact.
    plan : bool, default False
        If True, return planned actions without writing.

    Returns
    -------
    actions : list[ReconcileAction]
        Planned (and optionally executed) actions.

    Raises
    ------
    ValueError
        If shard columns do not match between staged and repo for a series.

    Notes
    -----
    **Rewrite minimization**

    - Data-section hashing ignores header churn (e.g., ``date_formatted``).
    - Parsed comparisons further ignore harmless numeric string formatting changes.

    **Deterministic sampling**

    Older shard inspections are thinned using a stable hash-based draw per
    ``(series_id, shard)`` so behavior is reproducible and testable.

    **Escalation**

    If a meaningful change is detected outside the recent window, the function
    adds reconcile actions for the recent window shards as well.
    """
    if now is None:
        now = pd.Timestamp.now()
    this_year = int(now.year)
    if prefer not in ["repo", "staged"]:
        raise ValueError("prefer must be 'repo' or 'staged'")

    if os.path.exists(repo_dir):
        repo_dir = repo_dir
    else:
        repo_dir = dstore_config.config_file(repo_dir)
        if not os.path.exists(repo_dir):
            raise ValueError(f"Repo directory does not exist as a directory or as config entry that maps to directory: {repo_dir}")
    
        
    staged_files = _list_csv_files(staged_dir, pattern=pattern)
    repo_files = _list_csv_files(repo_dir, pattern="*")   # match all to detect deletions
    staged_map = _index_by_series_and_shard(staged_files, remove_source=remove_source)
    repo_map = _index_by_series_and_shard(repo_files, remove_source=remove_source)

    actions: List[ReconcileAction] = []

    for series_id, staged_shards in staged_map.items():
        repo_shards = repo_map.get(series_id, {})

        # Determine which shards to inspect for "change detection" outside recent window
        changed_old = False
        for shard, spath in staged_shards.items():
            rpath = repo_shards.get(shard)
            if rpath is None:
                # New shard/file
                actions.append(
                    ReconcileAction(
                        series_id=series_id,
                        shard=shard,
                        action="write",
                        reason="missing_in_repo",
                        staged_path=spath,
                        repo_path=os.path.join(repo_dir, os.path.basename(spath)),
                    )
                )
                continue

            # If we can determine a year, apply sampling; otherwise treat as recent.
            _, end_year = _parse_shard(os.path.basename(spath))
            if end_year is None:
                inspect = True
            else:
                age = this_year - end_year
                if age <= recent_years:
                    inspect = True
                elif age > 10:
                    inspect = _stable_u01(f"{series_id}|{shard}|p10") < p10
                else:
                    inspect = _stable_u01(f"{series_id}|{shard}|p3") < p3

            if not inspect:
                continue

            # Fast compare: data-section hash
            if _hash_data_section(spath) == _hash_data_section(rpath):
                continue

            # Parsed compare to ignore harmless numeric/formatting differences
            sdf = _read_csv_timeseries(spath)
            rdf = _read_csv_timeseries(rpath)
            if list(sdf.columns) != list(rdf.columns):
                raise ValueError(
                    f"Column mismatch for {series_id} shard {shard}: "
                    f"repo={list(rdf.columns)} staged={list(sdf.columns)}"
                )
            # Compare intersection window if indices differ (common for growing series)
            common_idx = sdf.index.intersection(rdf.index)
            if len(common_idx) == 0:
                different = True
            else:
                different = not _values_equal(
                    sdf.loc[common_idx], rdf.loc[common_idx], atol=atol, rtol=rtol
                )
            if different:
                if end_year is not None and (this_year - end_year) > recent_years:
                    changed_old = True
                # Mark shard for update; actual writing happens below
                actions.append(
                    ReconcileAction(
                        series_id=series_id,
                        shard=shard,
                        action="splice_write",
                        reason="data_changed",
                        staged_path=spath,
                        repo_path=rpath,
                    )
                )

        # If old history changed, escalate to reconciling the recent window (and itself).
        if changed_old:
            # Ensure all shards in recent window are reconciled.
            for shard, spath in staged_shards.items():
                _, end_year = _parse_shard(os.path.basename(spath))
                if end_year is None or (this_year - end_year) <= recent_years:
                    if shard not in {
                        a.shard for a in actions if a.series_id == series_id
                    }:
                        actions.append(
                            ReconcileAction(
                                series_id=series_id,
                                shard=shard,
                                action="splice_write",
                                reason="escalated_due_to_old_change",
                                staged_path=spath,
                                repo_path=repo_shards.get(shard),
                            )
                        )

    if plan:
        return actions

    # Execute actions
    for a in actions:
        if a.action == "write":
            # Preserve existing repo header if present; otherwise preserve staged header.
            if a.repo_path is None:
                raise ValueError("Internal error: repo_path is None")
            if os.path.exists(a.repo_path):
                head = original_header(a.repo_path)
            else:
                head = original_header(a.staged_path) if a.staged_path else ""
            df = _read_csv_timeseries(a.staged_path)  # type: ignore[arg-type]
            _write_preserving_header(df=df, dest_path=a.repo_path, header_text=head)
        elif a.action == "splice_write":
            if a.staged_path is None:
                raise ValueError("Internal error: staged_path is None")
            # If repo missing, treat as write
            if a.repo_path is None or (not os.path.exists(a.repo_path)):
                dest = os.path.join(repo_dir, os.path.basename(a.staged_path))
                head = original_header(a.staged_path)
                df = _read_csv_timeseries(a.staged_path)
                _write_preserving_header(df=df, dest_path=dest, header_text=head)
                continue
            head = original_header(a.repo_path)
            sdf = _read_csv_timeseries(a.staged_path)
            rdf = _read_csv_timeseries(a.repo_path)
            if prefer == "repo":
                merged = ts_merge([rdf, sdf], strict_priority=True)
            elif prefer == "staged":
                merged = ts_merge([sdf, rdf], strict_priority=True)
            else:
                raise ValueError("prefer must be 'repo' or 'staged'")
            _write_preserving_header(df=merged, dest_path=a.repo_path, header_text=head)
        else:
            raise ValueError(f"Unknown action {a.action}")

    return actions


def update_flagged_data(
    staged_dir: str,
    repo_dir: str,
    *,
    remove_source: bool = False,
    atol: float = 0.0,
    rtol: float = 0.0,
    value_reference: str = "staged",
    explicit_conflict: str = "prefer_repo",
    plan: bool = False,
) -> List[ReconcileAction]:
    """Reconcile staged screened data into repo screened data (flag-smart).

    This assumes screened csvs contain exactly columns: ['value','user_flag'].

    The merge policy is defined in two phases:

    1) For timestamps where values differ (or are new on one side), take both value
       and flag from ``value_reference`` ("repo" or "staged").

    2) For timestamps where values are equal (within tolerance), merge flags using
       tri-state semantics:
         - explicit (0/1) beats blank (NA)
         - explicit conflicts (0 vs 1) are resolved by ``explicit_conflict``:
             * "prefer_repo"   -> take repo explicit value (good for preserving prior human overrides)
             * "prefer_staged" -> take staged explicit value (good for user-return patching)
             * "error"         -> fail-fast on conflicts

    Repo headers are preserved whenever possible.

    Writes preserve the existing repo header (metadata) whenever possible.
    """
    staged_files = _list_csv_files(staged_dir)
    repo_files = _list_csv_files(repo_dir)
    staged_map = _index_by_series_and_shard(staged_files, remove_source=remove_source)
    repo_map = _index_by_series_and_shard(repo_files, remove_source=remove_source)

    actions: List[ReconcileAction] = []

    for series_id, staged_shards in staged_map.items():
        repo_shards = repo_map.get(series_id, {})
        for shard, spath in staged_shards.items():
            rpath = repo_shards.get(shard)
            dest = (
                rpath
                if rpath is not None
                else os.path.join(repo_dir, os.path.basename(spath))
            )

            if rpath is None or (not os.path.exists(rpath)):
                actions.append(
                    ReconcileAction(
                        series_id=series_id,
                        shard=shard,
                        action="write",
                        reason="missing_in_repo",
                        staged_path=spath,
                        repo_path=dest,
                    )
                )
                continue

            # Hash fast path (data section): if identical, skip.
            if _hash_data_section(spath) == _hash_data_section(rpath):
                continue

            sdf = _read_csv_timeseries(spath)
            rdf = _read_csv_timeseries(rpath)
            merged = _merge_screened_flags(
                rdf,
                sdf,
                atol=atol,
                rtol=rtol,
                value_reference=value_reference,
                explicit_conflict=explicit_conflict,
            )

            # Decide if rewrite is needed by comparing parsed data section
            same_idx = merged.index.equals(rdf.index)
            same_cols = list(merged.columns) == list(rdf.columns)
            same_vals = (
                same_idx
                and same_cols
                and _values_equal(
                    merged, rdf.reindex(merged.index), atol=atol, rtol=rtol
                )
            )
            if same_vals:
                continue

            actions.append(
                ReconcileAction(
                    series_id=series_id,
                    shard=shard,
                    action="write",
                    reason="flag_or_value_merge_changed",
                    staged_path=spath,
                    repo_path=dest,
                )
            )

    if plan:
        return actions

    for a in actions:
        if a.staged_path is None or a.repo_path is None:
            raise ValueError("Internal error: missing paths in action")
        head = (
            original_header(a.repo_path)
            if os.path.exists(a.repo_path)
            else original_header(a.staged_path)
        )

        if a.reason == "missing_in_repo":
            df = _read_csv_timeseries(a.staged_path)
        else:
            sdf = _read_csv_timeseries(a.staged_path)
            rdf = (
                _read_csv_timeseries(a.repo_path)
                if os.path.exists(a.repo_path)
                else sdf.iloc[0:0]
            )
            df = _merge_screened_flags(
                rdf,
                sdf,
                atol=atol,
                rtol=rtol,
                value_reference=value_reference,
                explicit_conflict=explicit_conflict,
            )

        _write_preserving_header(df=df, dest_path=a.repo_path, header_text=head)

    return actions
