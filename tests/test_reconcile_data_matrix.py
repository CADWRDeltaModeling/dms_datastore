#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Incremental test matrix for reconcile_data.py

This is intentionally small and "white-box-ish" at first:
- It uses deterministic parameters (p10=p3=1, now fixed) to avoid sampling
  making tests flaky.
- It asserts *plans* first (plan=True), which is a good bridge between
  black-box tests and interactive exploration.

Once behavior is validated via the playground, you can tighten these tests and
add stronger assertions about exact merged frames and write minimization.

Run:
    pytest -q test_reconcile_data_matrix.py
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import numpy as np
import pytest

from dms_datastore.write_ts import write_ts_csv
from dms_datastore.reconcile_data import update_repo, update_flagged_data

NOW = pd.Timestamp("2026-02-08")


def _mk_values(start="2024-01-01", end="2024-01-10", seed=0) -> pd.DataFrame:
    idx = pd.date_range(start, end, freq="D")
    rng = np.random.default_rng(seed)
    df = pd.DataFrame({"value": rng.normal(size=len(idx))}, index=idx)
    df.index.name = "datetime"
    return df


def _mk_screened(values: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=values.index)
    out["value"] = values["value"]
    out["user_flag"] = pd.Series(pd.NA, index=values.index, dtype="Int64")
    out.index.name = "datetime"
    return out


def test_update_repo_ignores_header_only_change(tmp_path: Path) -> None:
    staged = tmp_path / "staging"
    repo = tmp_path / "repo"
    staged.mkdir()
    repo.mkdir()

    df = _mk_values("2015-01-01", "2015-12-31", seed=1)
    meta1 = "station_id: foo\nparam: flow\nunit: ft^3/s\n"
    meta2 = "station_id: foo\nparam: flow\nunit: ft^3/s\nnote: header-only\n"
    f = "cdec_foo_123_flow_2015.csv"

    # repo write
    write_ts_csv(df, repo / f, metadata=meta1, chunk_years=False)
    # staged write with different header, same data
    write_ts_csv(df, staged / f, metadata=meta2, chunk_years=False)

    actions = update_repo(str(staged), str(repo), now=NOW, p10=1.0, p3=1.0, plan=True)
    assert actions == []


def test_update_repo_plans_splice_write_on_data_change(tmp_path: Path) -> None:
    staged = tmp_path / "staging"
    repo = tmp_path / "repo"
    staged.mkdir()
    repo.mkdir()

    df = _mk_values("2015-01-01", "2015-12-31", seed=2)
    f = "cdec_foo_123_flow_2015.csv"
    meta = "station_id: foo\nparam: flow\nunit: ft^3/s\n"
    write_ts_csv(df, repo / f, metadata=meta, chunk_years=False)

    df2 = df.copy()
    df2.iloc[-1, 0] = df2.iloc[-1, 0] + 10.0
    write_ts_csv(df2, staged / f, metadata=meta, chunk_years=False)

    actions = update_repo(str(staged), str(repo), now=NOW, p10=1.0, p3=1.0, plan=True)
    assert len(actions) == 1
    assert actions[0].action in ("splice_write", "write")


@pytest.mark.parametrize(
    "repo_flag, staged_flag, value_changed, expected",
    [
        (pd.NA, pd.NA, False, pd.NA),
        (1, pd.NA, False, 1),      # staged blank doesn't clear repo 1
        (0, pd.NA, False, 0),      # repo 0 sticky when unchanged
        (pd.NA, 1, False, 1),      # staged 1 sets anomaly
        (1, 1, False, 1),
        (0, 1, False, 0),          # repo 0 sticky vs staged 1 when unchanged
        (0, pd.NA, True, pd.NA),   # changed => staged wins flag (drops 0)
        (1, pd.NA, True, pd.NA),   # changed => staged wins flag
        (1, 1, True, 1),
    ],
)
def test_screened_truth_table(tmp_path: Path, repo_flag, staged_flag, value_changed, expected) -> None:
    staged = tmp_path / "staging"
    repo = tmp_path / "repo"
    staged.mkdir()
    repo.mkdir()

    idx = pd.date_range("2024-01-01", periods=1, freq="D")
    idx.name = "datetime"

    rv = 10.0
    sv = 10.0 if not value_changed else 11.0

    rdf = pd.DataFrame({"value": [rv], "user_flag": [repo_flag]}, index=idx)
    sdf = pd.DataFrame({"value": [sv], "user_flag": [staged_flag]}, index=idx)
    # use dtype compatibility with reader logic
    rdf["user_flag"] = rdf["user_flag"].astype("Int64")
    sdf["user_flag"] = sdf["user_flag"].astype("Int64")

    f = "cdec_baz_789_ec_2024.csv"
    meta = "station_id: baz\nparam: ec\nunit: uS/cm\nscreen: true\n"

    write_ts_csv(rdf, repo / f, metadata=meta, chunk_years=False)
    write_ts_csv(sdf, staged / f, metadata=meta, chunk_years=False)

    update_flagged_data(str(staged), str(repo), plan=False)
    out = pd.read_csv(repo / f, comment="#", parse_dates=["datetime"], index_col="datetime")
    got = out["user_flag"].astype("Int64").iloc[0]
    assert (pd.isna(got) and pd.isna(expected)) or (got == expected)



def test_user_patch_ignores_flags_on_value_mismatch(tmp_path: Path) -> None:
    """If user returns stale values, treat their edits as wasted at mismatched timestamps.

    We simulate a user file (staged) that sets an explicit flag, but with a stale value.
    With value_reference='repo', repo should prevail for both value and flag at that timestamp.
    """
    staged = tmp_path / "user"
    repo = tmp_path / "repo"
    staged.mkdir()
    repo.mkdir()

    idx = pd.date_range("2024-01-01", periods=1, freq="D")
    idx.name = "datetime"

    # Repo has updated value and no flag decision
    rdf = pd.DataFrame({"value": [10.0], "user_flag": [pd.NA]}, index=idx)
    rdf["user_flag"] = rdf["user_flag"].astype("Int64")

    # User has stale value but tries to set explicit anomaly
    sdf = pd.DataFrame({"value": [11.0], "user_flag": [1]}, index=idx)
    sdf["user_flag"] = sdf["user_flag"].astype("Int64")

    f = "cdec_baz_789_ec_2024.csv"
    meta = "station_id: baz\nparam: ec\nunit: uS/cm\nscreen: true\n"

    write_ts_csv(rdf, repo / f, metadata=meta, chunk_years=False)
    write_ts_csv(sdf, staged / f, metadata=meta, chunk_years=False)

    update_flagged_data(
        str(staged),
        str(repo),
        value_reference="repo",
        explicit_conflict="prefer_staged",  # even if they would win on explicit conflicts, mismatch blocks it
        plan=False,
    )

    out = pd.read_csv(repo / f, comment="#", parse_dates=["datetime"], index_col="datetime")
    assert out["value"].iloc[0] == 10.0
    assert pd.isna(out["user_flag"].astype("Int64").iloc[0])


def test_user_patch_applies_explicit_on_equal_values(tmp_path: Path) -> None:
    """If values match, user explicit flags should apply when explicit_conflict prefers staged."""
    staged = tmp_path / "user"
    repo = tmp_path / "repo"
    staged.mkdir()
    repo.mkdir()

    idx = pd.date_range("2024-01-01", periods=1, freq="D")
    idx.name = "datetime"

    rdf = pd.DataFrame({"value": [10.0], "user_flag": [0]}, index=idx)  # repo override
    rdf["user_flag"] = rdf["user_flag"].astype("Int64")

    sdf = pd.DataFrame({"value": [10.0], "user_flag": [1]}, index=idx)  # user re-assert anomaly
    sdf["user_flag"] = sdf["user_flag"].astype("Int64")

    f = "cdec_baz_789_ec_2024.csv"
    meta = "station_id: baz\nparam: ec\nunit: uS/cm\nscreen: true\n"

    write_ts_csv(rdf, repo / f, metadata=meta, chunk_years=False)
    write_ts_csv(sdf, staged / f, metadata=meta, chunk_years=False)

    update_flagged_data(
        str(staged),
        str(repo),
        value_reference="repo",           # repo is value record
        explicit_conflict="prefer_staged",# user edits prevail on explicit conflict
        plan=False,
    )

    out = pd.read_csv(repo / f, comment="#", parse_dates=["datetime"], index_col="datetime")
    assert out["value"].iloc[0] == 10.0
    assert out["user_flag"].astype("Int64").iloc[0] == 1
