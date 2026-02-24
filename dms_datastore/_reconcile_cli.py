# dms_datastore/cli/_reconcile_cli.py
from __future__ import annotations

import csv
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Iterable, Optional


def echo_actions_text(actions: Iterable[object]) -> None:
    """
    Print one action per line.

    Assumes action objects have attributes:
      series_id, shard, action, reason, staged_path, repo_path
    """
    actions = list(actions)
    if not actions:
        print("no actions (repo already up to date for inspected shards)")
        return    
    for a in actions:
        series_id = getattr(a, "series_id", None)
        shard = getattr(a, "shard", None)
        action = getattr(a, "action", None)
        reason = getattr(a, "reason", None)
        staged = getattr(a, "staged_path", None)
        repo = getattr(a, "repo_path", None)

        parts = [
            str(action),
            f"series_id={series_id}",
            f"shard={shard}",
            f"reason={reason}",
        ]
        if staged:
            parts.append(f"staged={staged}")
        if repo:
            parts.append(f"repo={repo}")

        print("  ".join(parts))


def write_actions_csv(actions: Iterable[object], out_csv: str) -> None:
    """
    Write actions to a CSV file.

    Columns are stable and explicit to support downstream parsing and fixtures.
    """
    out_path = Path(out_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = ["series_id", "shard", "action", "reason", "staged_path", "repo_path"]

    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for a in actions:
            if is_dataclass(a):
                row = asdict(a)
            else:
                row = {
                    "series_id": getattr(a, "series_id", None),
                    "shard": getattr(a, "shard", None),
                    "action": getattr(a, "action", None),
                    "reason": getattr(a, "reason", None),
                    "staged_path": getattr(a, "staged_path", None),
                    "repo_path": getattr(a, "repo_path", None),
                }

            # enforce stable column ordering
            w.writerow({k: row.get(k, None) for k in fieldnames})


def resolve_plan_flag(apply: bool, plan: bool) -> bool:
    """
    Default is plan=True unless apply=True.

    If the user explicitly sets --plan, it wins (and --apply should be absent).
    """
    if plan:
        return True
    if apply:
        return False
    return True  # default


def maybe_fail_if_changes(actions: list[object], fail_if_changes: bool) -> None:
    """
    Exit code convention:
      - 0: no actions (or apply mode)
      - 2: actions exist and fail_if_changes requested
    """
    if fail_if_changes and len(actions) > 0:
        raise SystemExit(2)