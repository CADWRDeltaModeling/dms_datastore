# dms_datastore/cli/update_flagged_data.py
from __future__ import annotations

import click
import pandas as pd

from dms_datastore.reconcile_data import update_flagged_data
from dms_datastore._reconcile_cli import (
    echo_actions_text,
    maybe_fail_if_changes,
    resolve_plan_flag,
    write_actions_csv,
)


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.argument("staged_dir", type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.argument("repo_dir", type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.option(
    "--remove-source",
    is_flag=True,
    default=False,
    show_default=True,
    help="If set, wildcard the source slot when building series identities.",
)
@click.option("--atol", default=0.0, type=float, show_default=True, help="Absolute tolerance for value comparisons.")
@click.option("--rtol", default=0.0, type=float, show_default=True, help="Relative tolerance for value comparisons.")
@click.option(
    "--value-source",
    type=click.Choice(["staged", "repo"], case_sensitive=False),
    default="staged",
    show_default=True,
    help="For timestamps where values differ/new, take (value,flag) from this side.",
)
@click.option(
    "--flag-conflict",
    type=click.Choice(["prefer_repo", "prefer_staged", "error"], case_sensitive=False),
    default="prefer_repo",
    show_default=True,
    help="Resolve explicit flag conflicts (0 vs 1) when values are equal.",
)
@click.option("--plan", is_flag=True, default=False, help="Dry-run: compute and print actions without writing.")
@click.option("--apply", is_flag=True, default=False, help="Execute: write changes to the repo.")
@click.option(
    "--out-actions",
    default=None,
    type=click.Path(dir_okay=False),
    help="Optional CSV path to write the action list (audit/fixtures).",
)
@click.option(
    "--fail-if-changes",
    is_flag=True,
    default=False,
    help="In plan mode, exit with code 2 if any actions would be taken.",
)
def main(
    staged_dir: str,
    repo_dir: str,
    remove_source: bool,
    atol: float,
    rtol: float,
    value_source: str,
    flag_conflict: str,
    plan: bool,
    apply: bool,
    out_actions: str | None,
    fail_if_changes: bool,
) -> None:
    """
    Reconcile staged screened data into repo screened data (flag-smart).
    """
    if plan and apply:
        raise click.UsageError("Choose at most one of --plan or --apply (default is --plan).")

    plan_effective = resolve_plan_flag(apply=apply, plan=plan)

    actions = update_flagged_data(
        staged_dir=staged_dir,
        repo_dir=repo_dir,
        remove_source=remove_source,
        atol=atol,
        rtol=rtol,
        value_reference=value_source,
        explicit_conflict=flag_conflict,
        plan=plan_effective,
    )

    echo_actions_text(actions)

    if out_actions is not None:
        if not out_actions.lower().endswith(".csv"):
            raise click.UsageError("--out-actions must be a .csv path")
        write_actions_csv(actions, out_actions)

    if plan_effective:
        maybe_fail_if_changes(actions, fail_if_changes)