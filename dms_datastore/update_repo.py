# dms_datastore/cli/update_repo.py
from __future__ import annotations

import click
import pandas as pd

from dms_datastore.reconcile_data import update_repo
from dms_datastore._reconcile_cli import (
    echo_actions_text,
    maybe_fail_if_changes,
    resolve_plan_flag,
    write_actions_csv,
)


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.argument("staged_dir", type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.argument("repo_dir", type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.option("--pattern", default="*.csv", show_default=True, help="Filename glob within each tier directory.")
@click.option(
    "--prefer",
    type=click.Choice(["staged", "repo"], case_sensitive=False),
    default="staged",
    show_default=True,
    help="Which side wins on overlapping timestamps when splicing staged and repo records.",
)
@click.option(
    "--allow-new-series/--no-allow-new-series",
    default=True,
    show_default=True,
    help="If false, require namesake repo series before updating (catches naming mistakes).",
)
@click.option(
    "--remove-source",
    is_flag=True,
    default=False,
    show_default=True,
    help="If set, wildcard the source slot when building series identities.",
)
@click.option("--now", default=None, help="Reference timestamp (ISO) used to compute shard ages for sampling.")
@click.option("--recent-years", default=3, type=int, show_default=True, help="Shards newer than this are always inspected.")
@click.option("--p3", default=0.15, type=float, show_default=True, help="Sampling probability for shards in [recent-years, 10) years old.")
@click.option("--p10", default=0.05, type=float, show_default=True, help="Sampling probability for shards >= 10 years old.")
@click.option("--atol", default=0.0, type=float, show_default=True, help="Absolute tolerance for parsed-data comparisons.")
@click.option("--rtol", default=0.0, type=float, show_default=True, help="Relative tolerance for parsed-data comparisons.")
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
    pattern: str,
    prefer: str,
    allow_new_series: bool,
    remove_source: bool,
    now: str | None,
    recent_years: int,
    p3: float,
    p10: float,
    atol: float,
    rtol: float,
    plan: bool,
    apply: bool,
    out_actions: str | None,
    fail_if_changes: bool,
) -> None:
    """
    Reconcile staged vs repo time-series CSV files (formatted/processed tiers).
    """
    if plan and apply:
        raise click.UsageError("Choose at most one of --plan or --apply (default is --plan).")

    plan_effective = resolve_plan_flag(apply=apply, plan=plan)

    actions = update_repo(
        staged_dir=staged_dir,
        repo_dir=repo_dir,
        pattern=pattern,
        prefer=prefer,
        allow_new_series=allow_new_series,
        remove_source=remove_source,
        now = None if now is None else pd.Timestamp(now),
        recent_years=recent_years,
        p10=p10,
        p3=p3,
        atol=atol,
        rtol=rtol,
        plan=plan_effective,
    )

    echo_actions_text(actions)

    if out_actions is not None:
        if not out_actions.lower().endswith(".csv"):
            raise click.UsageError("--out-actions must be a .csv path")
        write_actions_csv(actions, out_actions)

    if plan_effective:
        maybe_fail_if_changes(actions, fail_if_changes)