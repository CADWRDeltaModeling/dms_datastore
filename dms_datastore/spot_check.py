from __future__ import annotations

import logging
import math
import re
import os.path
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence
import dms_datastore.dstore_config
import click
from dms_datastore.logging_config import (
    LoggingConfig,
    configure_logging_config,
    resolve_loglevel,
)
from dms_datastore import read_ts
from omegaconf import OmegaConf  # fail-fast if missing
from dms_datastore import dstore_config

log = logging.getLogger("dms_datastore.spot_check")


@dataclass(frozen=True)
class SpotCheckFailureCounts:
    count_failures: int = 0
    required_missing: int = 0
    required_open_failures: int = 0

    def exit_code(self) -> int:
        """
        Jenkins-friendly: non-zero if any failures occurred.
        Bitmask encoding:
          1 = count failures
          2 = required missing
          4 = required open failures
        """
        code = 0
        if self.count_failures:
            code |= 1
        if self.required_missing:
            code |= 2
        if self.required_open_failures:
            code |= 4
        return code


@dataclass(frozen=True)
class SpotCheckResult:
    failures: SpotCheckFailureCounts
    warnings: int
    groups_checked: int
    tiers_checked: int


def run_spot_check(
    config_path: str,
    *,
    groups: Optional[Sequence[str]] = None,
    tiers: Optional[Sequence[str]] = None,
) -> SpotCheckResult:
    """
    Run spot checks defined in YAML.

    The YAML supports two categories of checks:

    1. Count checks by group and tier.
       Each tier declares a single glob ``pattern`` whose count in staging is
       compared to the corresponding repo count.

    2. File/year checks.
       A group may declare either:
       - ``check_files``: legacy single-pattern checks, or
       - ``check_streams``: tier-aware logical checks where the acceptable glob
         may differ by tier.

    ``check_streams`` is intended for cases where the same logical stream is
    represented by different filename prefixes in different tiers, such as a
    realtime ``cdec_*`` stream in formatted that is absorbed into an ``ncro_*``
    screened product.
    """



    cfg = _load_config(config_path)

    selected_groups = set(groups) if groups else None
    selected_tiers = set(tiers) if tiers else None

    defaults = cfg.get("defaults", {})
    default_n_count_fract = float(defaults.get("n_count_fract", 0.90))
    default_min_count_abs = defaults.get("min_count_abs", None)
    default_check_defaults = defaults.get("check_files", {})
    default_require = bool(default_check_defaults.get("require", True))
    default_open = bool(default_check_defaults.get("open", True))

    repo_dirs = _expect_mapping(cfg, "repo_dirs")
    staging_dirs = _expect_mapping(cfg, "staging_dirs")

    failures = SpotCheckFailureCounts()
    warnings = 0
    groups_checked = 0
    tiers_checked = 0

    groups_list = _expect_list(cfg, "groups")
    for g in groups_list:
        gname = _expect_str(g, "name")
        if selected_groups is not None and gname not in selected_groups:
            continue

        tiers_map = _expect_mapping(g, "tiers")
        legacy_check_files = g.get("check_files", []) or []
        check_streams = g.get("check_streams", []) or []

        groups_checked += 1

        for tier_name, tier_cfg in tiers_map.items():
            if selected_tiers is not None and tier_name not in selected_tiers:
                continue

            tiers_checked += 1
            staging_dir = Path(_expect_str(staging_dirs, tier_name))
            pattern = _expect_str(tier_cfg, "pattern")
            n_count_fract = float(tier_cfg.get("n_count_fract", default_n_count_fract))
            use_repo_ref = bool(tier_cfg.get("use_repo_ref", True))

            if use_repo_ref:
                repo_dir = Path(_expect_str(repo_dirs, tier_name))
                repo_count = _glob_count(repo_dir, pattern)
            else:
                repo_count = 0

            staging_count = _glob_count(staging_dir, pattern)
            required_min = int(math.ceil(n_count_fract * repo_count)) if repo_count > 0 else 0
            min_abs = tier_cfg.get("min_count_abs", default_min_count_abs)
            if min_abs is not None:
                required_min = max(required_min, int(min_abs))

            if staging_count < required_min:
                failures = SpotCheckFailureCounts(
                    count_failures=failures.count_failures + 1,
                    required_missing=failures.required_missing,
                    required_open_failures=failures.required_open_failures,
                )
                log.error(
                    "COUNT FAIL group=%s tier=%s: staging=%d repo=%d required_min=%d (fract=%0.3f pattern=%s)",
                    gname,
                    tier_name,
                    staging_count,
                    repo_count,
                    required_min,
                    n_count_fract,
                    pattern,
                )
            else:
                log.info(
                    "COUNT PASS group=%s tier=%s: staging=%d repo=%d required_min=%d (fract=%0.3f pattern=%s)",
                    gname,
                    tier_name,
                    staging_count,
                    repo_count,
                    required_min,
                    n_count_fract,
                    pattern,
                )

            for chk in legacy_check_files:
                failures, warnings = _run_legacy_file_check(
                    chk=chk,
                    group_name=gname,
                    tier_name=tier_name,
                    staging_dir=staging_dir,
                    default_require=default_require,
                    default_open=default_open,
                    failures=failures,
                    warnings=warnings,
                )

            for chk in check_streams:
                failures, warnings = _run_stream_check(
                    chk=chk,
                    group_name=gname,
                    tier_name=tier_name,
                    staging_dir=staging_dir,
                    default_require=default_require,
                    default_open=default_open,
                    failures=failures,
                    warnings=warnings,
                )

    code = failures.exit_code()
    log.info(
        "SUMMARY groups_checked=%d tiers_checked=%d warnings=%d "
        "count_failures=%d required_missing=%d required_open_failures=%d exit_code=%d",
        groups_checked,
        tiers_checked,
        warnings,
        failures.count_failures,
        failures.required_missing,
        failures.required_open_failures,
        code,
    )

    return SpotCheckResult(
        failures=failures,
        warnings=warnings,
        groups_checked=groups_checked,
        tiers_checked=tiers_checked,
    )


def _run_legacy_file_check(
    *,
    chk: Dict[str, Any],
    group_name: str,
    tier_name: str,
    staging_dir: Path,
    default_require: bool,
    default_open: bool,
    failures: SpotCheckFailureCounts,
    warnings: int,
) -> tuple[SpotCheckFailureCounts, int]:
    """Run a legacy single-pattern file/year check for one tier."""
    pattern = _expect_str(chk, "name")
    years = _expect_list(chk, "years")
    require = bool(chk.get("require", default_require))
    do_open = bool(chk.get("open", default_open))

    candidates = sorted(staging_dir.glob(pattern))
    if not candidates:
        log.debug(
            "No staging candidates for group=%s tier=%s check=%s under %s",
            group_name,
            tier_name,
            pattern,
            staging_dir,
        )

    return _evaluate_year_checks(
        check_label=pattern,
        candidates=candidates,
        years=years,
        require=require,
        do_open=do_open,
        group_name=group_name,
        tier_name=tier_name,
        staging_dir=staging_dir,
        failures=failures,
        warnings=warnings,
    )


def _run_stream_check(
    *,
    chk: Dict[str, Any],
    group_name: str,
    tier_name: str,
    staging_dir: Path,
    default_require: bool,
    default_open: bool,
    failures: SpotCheckFailureCounts,
    warnings: int,
) -> tuple[SpotCheckFailureCounts, int]:
    """
    Run a tier-aware logical stream check for one tier.

    Accepted YAML forms:

    check_streams:
      - name: bac_ec_realtime_tail
        years: [2025, 2026]
        tier_patterns:
          raw: cdec_bac_*_ec_*
          formatted: cdec_bac_*_ec_*
          screened: ncro_bac_*_ec_*

    or:

        patterns:
          raw: [cdec_bac_*_ec_*]
          screened: [ncro_bac_*_ec_*, dwr_bac_*_ec_*]
    """
    check_name = _expect_str(chk, "name")
    years = _expect_list(chk, "years")
    require = bool(chk.get("require", default_require))
    do_open = bool(chk.get("open", default_open))
    patterns = _patterns_for_tier(chk, tier_name)

    all_candidates: List[Path] = []
    for pattern in patterns:
        all_candidates.extend(sorted(staging_dir.glob(pattern)))

    candidates = _dedupe_paths(all_candidates)
    if not candidates:
        log.debug(
            "No staging candidates for group=%s tier=%s stream=%s patterns=%s under %s",
            group_name,
            tier_name,
            check_name,
            patterns,
            staging_dir,
        )

    label = f"{check_name} patterns={patterns}"
    return _evaluate_year_checks(
        check_label=label,
        candidates=candidates,
        years=years,
        require=require,
        do_open=do_open,
        group_name=group_name,
        tier_name=tier_name,
        staging_dir=staging_dir,
        failures=failures,
        warnings=warnings,
    )


def _evaluate_year_checks(
    *,
    check_label: str,
    candidates: Sequence[Path],
    years: Sequence[Any],
    require: bool,
    do_open: bool,
    group_name: str,
    tier_name: str,
    staging_dir: Path,
    failures: SpotCheckFailureCounts,
    warnings: int,
) -> tuple[SpotCheckFailureCounts, int]:
    """Evaluate year coverage and optional open checks for a candidate set."""
    for year in years:
        y = int(year)
        year_matches = _filter_files_covering_year(candidates, y)
        if not year_matches:
            msg = (
                f"MISSING FILE group={group_name} tier={tier_name} "
                f"check={check_label} year={y} staging_dir={staging_dir}"
            )
            if require:
                failures = SpotCheckFailureCounts(
                    count_failures=failures.count_failures,
                    required_missing=failures.required_missing + 1,
                    required_open_failures=failures.required_open_failures,
                )
                log.error(msg)
            else:
                warnings += 1
                log.warning(msg)
            continue

        chosen = _choose_best_candidate(year_matches)
        log.debug(
            "FOUND group=%s tier=%s check=%s year=%d chosen=%s (n_candidates=%d)",
            group_name,
            tier_name,
            check_label,
            y,
            chosen,
            len(year_matches),
        )

        if not do_open:
            continue

        try:
            _ = read_ts(chosen)
            log.info(
                "OPEN PASS group=%s tier=%s check=%s year=%d file=%s",
                group_name,
                tier_name,
                check_label,
                y,
                chosen,
            )
        except Exception as e:
            msg = (
                f"OPEN FAIL group={group_name} tier={tier_name} check={check_label} "
                f"year={y} file={chosen} err={type(e).__name__}: {e}"
            )
            if require:
                failures = SpotCheckFailureCounts(
                    count_failures=failures.count_failures,
                    required_missing=failures.required_missing,
                    required_open_failures=failures.required_open_failures + 1,
                )
                log.error(msg)
            else:
                warnings += 1
                log.warning(msg)

    return failures, warnings


# ----------------------------
# Internals (fail-fast)
# ----------------------------

def _load_config(config_path: Path) -> Dict[str, Any]:
    config_path = dstore_config.config_file(config_path)
    if not os.path.exists(config_path):
        raise FileNotFoundError(str(config_path))
    cfg = OmegaConf.load(str(config_path))
    out = OmegaConf.to_container(cfg, resolve=True)
    if not isinstance(out, dict):
        raise ValueError("Top-level YAML must be a mapping/dict.")
    return out


def _expect_mapping(d: Dict[str, Any], key: str) -> Dict[str, Any]:
    if key not in d:
        raise ValueError(f"Missing required key: {key}")
    v = d[key]
    if not isinstance(v, dict):
        raise TypeError(f"Expected mapping for key '{key}', got {type(v).__name__}")
    return v


def _expect_list(d: Dict[str, Any], key: str) -> List[Any]:
    if key not in d:
        raise ValueError(f"Missing required key: {key}")
    v = d[key]
    if not isinstance(v, list):
        raise TypeError(f"Expected list for key '{key}', got {type(v).__name__}")
    return v


def _expect_str(d: Dict[str, Any], key: str) -> str:
    if key not in d:
        raise ValueError(f"Missing required key: {key}")
    v = d[key]
    if not isinstance(v, str) or not v:
        raise TypeError(f"Expected non-empty string for key '{key}', got {type(v).__name__}")
    return v


def _expect_string_list(value: Any, *, context: str) -> List[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list) and all(isinstance(x, str) and x for x in value):
        return value
    raise TypeError(f"Expected string or list[str] for {context}, got {type(value).__name__}")


def _patterns_for_tier(chk: Dict[str, Any], tier_name: str) -> List[str]:
    if "tier_patterns" in chk:
        tier_patterns = _expect_mapping(chk, "tier_patterns")
        if tier_name not in tier_patterns:
            raise ValueError(
                f"Check {chk.get('name', '<unnamed>')!r} missing tier_patterns entry for tier {tier_name!r}"
            )
        return _expect_string_list(tier_patterns[tier_name], context=f"tier_patterns[{tier_name!r}]")

    if "patterns" in chk:
        patterns_map = _expect_mapping(chk, "patterns")
        if tier_name not in patterns_map:
            raise ValueError(
                f"Check {chk.get('name', '<unnamed>')!r} missing patterns entry for tier {tier_name!r}"
            )
        return _expect_string_list(patterns_map[tier_name], context=f"patterns[{tier_name!r}]")

    if "name" in chk:
        return [_expect_str(chk, "name")]

    raise ValueError("Each file/stream check must define 'name' or tier-specific patterns")


def _dedupe_paths(paths: Sequence[Path]) -> List[Path]:
    seen = set()
    out: List[Path] = []
    for p in paths:
        rp = p.resolve()
        if rp in seen:
            continue
        seen.add(rp)
        out.append(p)
    return out


def _glob_count(root: Path, pattern: str) -> int:
    if not root.exists():
        raise FileNotFoundError(str(root))
    return sum(1 for _ in root.glob(pattern))


_YEAR_RE = re.compile(r"(?<!\d)(19\d{2}|20\d{2})(?!\d)")

# Includes syear and eyear, with 9999 as a special "open" sentinel for convenience
_RANGE_RE = re.compile(
    r"(?<!\d)(19\d{2}|20\d{2})_(19\d{2}|20\d{2}|9999)(?!\d)"
)


def _filter_files_covering_year(paths: Sequence[Path], year: int) -> List[Path]:
    """Return files whose filenames explicitly mention or cover ``year``."""
    out: List[Path] = []
    for p in paths:
        name = p.name
        m = _RANGE_RE.search(name)
        if m:
            y0 = int(m.group(1))
            y1 = int(m.group(2))
            if y0 <= year <= y1:
                out.append(p)
                continue

        ys = [int(x) for x in _YEAR_RE.findall(name)]
        if year in ys:
            out.append(p)

    return out


def _choose_best_candidate(paths: Sequence[Path]) -> Path:
    """Prefer newest mtime, then lexicographic name, for deterministic selection."""
    return sorted(paths, key=lambda p: (p.stat().st_mtime, p.name))[-1]


@click.command(
    name="spot_check",
    context_settings={"help_option_names": ["-h", "--help"]},
)
@click.option(
    "--config",
    "config_path",
    type=str,
    required=True,
    help="Path to spot-check YAML spec or key in the package config system that evaluates to the name of this spec.",
)
@click.option(
    "--group",
    "groups",
    multiple=True,
    help="Group(s) to run. Repeatable: --group des --group ncro. If omitted, runs all groups.",
)
@click.option(
    "--tier",
    "tiers",
    multiple=True,
    help="Tier(s) to run. Repeatable: --tier raw --tier formatted. If omitted, runs all tiers declared per group.",
)
@click.option(
    "--logdir",
    type=click.Path(file_okay=False, path_type=Path),
    default=None,
    help="Optional directory to write a logfile in addition to console output.",
)
@click.option(
    "--logfile-prefix",
    default="spot_check",
    show_default=True,
    help="Prefix for logfile name when --logdir is provided.",
)
@click.option("--debug", is_flag=True, help="Set loglevel to DEBUG.")
@click.option("--verbose", is_flag=True, help="Set loglevel to INFO (unless --quiet).")
@click.option("--quiet", is_flag=True, help="Set loglevel to WARNING.")
@click.option(
    "--loglevel",
    default=None,
    help="Explicit loglevel (e.g., DEBUG, INFO, WARNING). Overrides --debug/--verbose/--quiet precedence per resolve_loglevel().",
)
def spot_check_cli(
    config_path: Path,
    groups: tuple[str, ...],
    tiers: tuple[str, ...],
    logdir: Path | None,
    logfile_prefix: str,
    debug: bool,
    verbose: bool,
    quiet: bool,
    loglevel: str | None,
) -> None:
    """Spot-check staged nightly downloads against repo expectations."""
    level = resolve_loglevel(
        debug=debug,
        verbose=verbose,
        quiet=quiet,
        loglevel=loglevel,
    )
    if isinstance(level, tuple):
        level = level[0]

    configure_logging_config(
        LoggingConfig(
            package_name="dms_datastore",
            level=level,
            console=True,
            logdir=logdir,
            logfile_prefix=logfile_prefix,
            include_pid_in_filename=True,
        )
    )

    result = run_spot_check(
        config_path,
        groups=list(groups) if groups else None,
        tiers=list(tiers) if tiers else None,
    )

    raise SystemExit(result.failures.exit_code())


if __name__ == "__main__":
    spot_check_cli()
