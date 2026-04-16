# dms_datastore/cli/update_flagged_data.py
from __future__ import annotations

import logging
from pathlib import Path

import click
import pandas as pd

from dms_datastore.logging_config import configure_logging, resolve_loglevel
from dms_datastore.reconcile_data import update_flagged_data
from dms_datastore._reconcile_cli import (
    echo_actions_text,
    maybe_fail_if_changes,
    resolve_plan_flag,
    write_actions_csv,
)


logger = logging.getLogger(__name__)



@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.argument("staged_dir", type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.argument("repo_dir", type=str)
@click.option(
    "--pattern",
    default="*.csv",
    show_default=True,
    help="Filename glob within each tier directory.",
)
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
    "--freq-mismatch",
    type=click.Choice(["quarantine", "replace"], case_sensitive=False),
    default="quarantine",
    show_default=True,
    help="When staged and repo are both regular but have different inferred frequencies, quarantine staged or replace the repo file.",
)
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
@click.option(
    "--max-workers",
    default=4,
    type=int,
    show_default=True,
    help="Number of worker threads used for vetting staged shards.",
)
@click.option("--logdir", type=click.Path(path_type=Path), default=None, help="Optional log directory.")
@click.option("--debug", is_flag=True, help="Enable debug logging and per-action output.")
@click.option("--quiet", is_flag=True, help="Disable console logging.")
def main(
    staged_dir: str,
    repo_dir: str,
    pattern: str,
    remove_source: bool,
    atol: float,
    rtol: float,
    value_source: str,
    flag_conflict: str,
    freq_mismatch: str,
    plan: bool,
    apply: bool,
    out_actions: str | None,
    fail_if_changes: bool,
    max_workers: int,
    logdir: Path | None,
    debug: bool,
    quiet: bool,
) -> None:
    """
    Reconcile staged screened data into repo screened data (flag-smart).
    """
    level, console = resolve_loglevel(debug=debug, quiet=quiet)
    configure_logging(
        package_name="dms_datastore",
        level=level,
        console=console,
        logdir=logdir,
        logfile_prefix="update_flagged_data",
    )

    if plan and apply:
        raise click.UsageError("Choose at most one of --plan or --apply (default is --plan).")

    plan_effective = resolve_plan_flag(apply=apply, plan=plan)

    actions = update_flagged_data(
        staged_dir=staged_dir,
        repo_dir=repo_dir,
        pattern=pattern,
        remove_source=remove_source,
        atol=atol,
        rtol=rtol,
        value_reference=value_source,
        explicit_conflict=flag_conflict,
        freq_mismatch=freq_mismatch,
        plan=plan_effective,
        max_workers=max_workers,
    )

    logger.info("update_flagged_data CLI: %d action(s) computed", len(actions))
    if debug:
        echo_actions_text(actions)

    if out_actions is not None:
        if not out_actions.lower().endswith(".csv"):
            raise click.UsageError("--out-actions must be a .csv path")
        write_actions_csv(actions, out_actions)

    if plan_effective:
        maybe_fail_if_changes(actions, fail_if_changes)