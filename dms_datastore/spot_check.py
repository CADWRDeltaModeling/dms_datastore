from __future__ import annotations

import fnmatch
import logging
import math
import os.path
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence

import click
from omegaconf import OmegaConf

from dms_datastore import dstore_config, read_ts
from dms_datastore.filename import interpret_fname
from dms_datastore.logging_config import (
    LoggingConfig,
    configure_logging_config,
    resolve_loglevel,
)

log = logging.getLogger("dms_datastore.spot_check")

# Integrity checks recognized in a stream's `integrity:` list. Kept short and
# filename-only (no file I/O) so they stay affordable to run on every match.
KNOWN_INTEGRITY_CHECKS = (
    "filename_correct",
    "station_in_registry",
    "agency_id_matches_registry",
)


@dataclass(frozen=True)
class SpotCheckFailureCounts:
    count_failures: int = 0
    required_missing: int = 0
    required_open_failures: int = 0
    integrity_failures: int = 0

    def exit_code(self) -> int:
        code = 0
        if self.count_failures:
            code |= 1
        if self.required_missing:
            code |= 2
        if self.required_open_failures:
            code |= 4
        if self.integrity_failures:
            code |= 8
        return code


@dataclass(frozen=True)
class SpotCheckResult:
    failures: SpotCheckFailureCounts
    warnings: int
    groups_checked: int
    streams_checked: int


@dataclass(frozen=True)
class IdentityPolicy:
    """How loosely a filename token must match a registry value to "identify" it.

    Only consumed by the `agency_id_matches_registry` integrity check via a
    stream's `agency_id_policy`, where `candidate` is the `agency_id` parsed
    from the filename and `expected` is the registry's `agency_id` for that
    `station_id`. `agency_id` and `station_id` are different identifiers and
    generally do not resemble each other -- the check exists to confirm the
    filename's `agency_id` is the one the registry actually has on file for
    that `station_id`, not to compare it against `station_id` itself. How
    exact that comparison should be varies by agency, hence the modes:

    - "exact": `candidate == expected`.
    - "contains" (default): true if `expected` is a substring of `candidate`,
      or vice versa (`expected in candidate or candidate in expected`).
      NCRO's registry `agency_id` is a base id but the filename token can
      carry an additional subprogram suffix (e.g. registry `"b91470"` vs.
      filename token `"b91470q"` or `"b91470q00"`, see dedup_ncro_suffix.py)
      -- neither string equals the other, but one contains the other.
    - "prefix": either string is a prefix of the other.

    Does not affect filename `pattern` glob matching or per-year repo/staging
    coverage checks -- those always match exactly on the configured glob.
    """

    mode: str = "contains"

    def matches(self, candidate: str, expected: str) -> bool:
        if self.mode == "exact":
            return candidate == expected
        if self.mode == "contains":
            return expected in candidate or candidate in expected
        if self.mode == "prefix":
            return candidate.startswith(expected) or expected.startswith(candidate)
        raise ValueError(f"Unsupported identity_policy.mode={self.mode!r}")

    @classmethod
    def from_mapping(cls, value: Any) -> "IdentityPolicy":
        if value is None:
            return cls()
        if isinstance(value, str):
            return cls(mode=value)
        if isinstance(value, Mapping):
            mode = str(value.get("mode", "contains"))
            return cls(mode=mode)
        raise TypeError(f"Unsupported identity_policy value: {type(value).__name__}")


# Default agency_id_matches_registry comparison mode, keyed by the `agency`
# token parsed from the filename ("*" is the fallback for anything unlisted).
# agency_id and station_id are unrelated identifiers whose correspondence
# varies by agency: USGS and DES keep one fixed agency_id per station, but
# NCRO's agency_id also carries a subprogram suffix that varies across a
# station's file variants (see IdentityPolicy docstring), so it needs the
# looser "contains" comparison, as does everything not listed here.
DEFAULT_AGENCY_ID_POLICY_BY_AGENCY: dict[str, str] = {
    "usgs": "exact",
    "des": "exact",
    "ncro": "contains",
    "*": "contains",
}


def _default_agency_id_policy(agency: str) -> "IdentityPolicy":
    mode = DEFAULT_AGENCY_ID_POLICY_BY_AGENCY.get(agency, DEFAULT_AGENCY_ID_POLICY_BY_AGENCY["*"])
    return IdentityPolicy(mode=mode)


@dataclass(frozen=True)
class Subset:
    include: tuple[str, ...] = ()
    exclude: tuple[str, ...] = ()

    @classmethod
    def from_mapping(cls, value: Any) -> "Subset":
        if value is None:
            return cls()
        if not isinstance(value, Mapping):
            raise TypeError(f"Subset must be a mapping, got {type(value).__name__}")
        include = tuple(_expect_string_list(value.get("include", ()), context="subset.include"))
        exclude = tuple(_expect_string_list(value.get("exclude", ()), context="subset.exclude"))
        return cls(include=include, exclude=exclude)

    def matches(self, path: Path) -> bool:
        name = path.name
        if self.include and not any(fnmatch.fnmatch(name, pat) for pat in self.include):
            return False
        if self.exclude and any(fnmatch.fnmatch(name, pat) for pat in self.exclude):
            return False
        return True


@dataclass(frozen=True)
class StreamCheck:
    name: str
    years: tuple[int, ...] = ()
    # Glob matched against both repo_root and staging_root. A single field
    # because a check already fixes one filename convention (repo_label) for
    # every stream in it -- repo and staging can't legitimately disagree.
    pattern: Optional[str] = None
    require: bool = True
    open_file: bool = False
    # Optional override for the `agency_id_matches_registry` integrity
    # check: when set, forces this comparison mode for every file in the
    # stream. When unset (the normal case), each file gets the per-agency
    # default from DEFAULT_AGENCY_ID_POLICY_BY_AGENCY instead, since a
    # stream's matched files usually span more than one agency.
    agency_id_policy: Optional[IdentityPolicy] = None

    # Absolute-count check fields. A stream carries these when it validates a
    # raw file count against fixed thresholds instead of (or in addition to)
    # per-year coverage. Named `count_pattern` (rather than reusing `pattern`)
    # because it scopes the count check independently of the coverage glob --
    # a stream can count a narrower subset than it covers (e.g. noaa_predictions
    # counts only `*_predictions_*.csv` while the group's plain `pattern` would
    # match every noaa file). `count_pattern=None` means "every file in
    # spot_target matched by the group's subset", per data source rather than
    # per year.
    count_pattern: Optional[str] = None
    min_count: Optional[int] = None
    max_count: Optional[int] = None

    # `partial` marks a data source that is only ever partially populated
    # (e.g. NOAA predictions). It selects the partial_* thresholds below
    # instead of min_count/max_count. Left per-stream because partial-ness
    # varies by data source, not by tier. When partial is True and no
    # partial_* threshold is set, the count check is skipped (logged, not a
    # failure) until real numbers are learned.
    partial: bool = False
    partial_min_count: Optional[int] = None
    partial_max_count: Optional[int] = None

    # When true, an under/over-threshold count result logs at WARNING and
    # increments `warnings` instead of failing the run. For a count gap that's
    # real but known non-fatal (e.g. a source lagging behind expectations
    # without blocking downstream use), rather than something that should
    # gate update_repo/update_flagged_data.
    warn_only: bool = False

    # Filename-only integrity checks to run over this stream's matched files.
    # See KNOWN_INTEGRITY_CHECKS. Resolved against the group's `repo_label`
    # (there is one filename convention per group, not per stream).
    integrity: tuple[str, ...] = ()

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "StreamCheck":
        name = _expect_str(value, "name")
        years = tuple(int(y) for y in _expect_list(value.get("years", []), key="years"))
        pattern = value.get("pattern")
        require = bool(value.get("require", True))
        open_file = bool(value.get("open_file", value.get("open", False)))
        raw_agency_id_policy = value.get("agency_id_policy")
        agency_id_policy = (
            IdentityPolicy.from_mapping(raw_agency_id_policy) if raw_agency_id_policy is not None else None
        )

        count_pattern = value.get("count_pattern")
        min_count = value.get("min_count")
        max_count = value.get("max_count")
        partial = bool(value.get("partial", False))
        partial_min_count = value.get("partial_min_count")
        partial_max_count = value.get("partial_max_count")
        warn_only = bool(value.get("warn_only", False))

        integrity = tuple(_expect_string_list(value.get("integrity"), context=f"stream '{name}'.integrity"))
        unknown = set(integrity) - set(KNOWN_INTEGRITY_CHECKS)
        if unknown:
            raise ValueError(f"Stream '{name}' has unknown integrity check(s): {sorted(unknown)}")

        has_years = bool(years)
        # `partial: true` alone is enough (thresholds may be learned later,
        # see partial-skip logging in run_spot_check).
        has_count = partial or any(
            v is not None for v in (min_count, max_count, partial_min_count, partial_max_count)
        )
        if not has_years and not has_count:
            raise ValueError(f"Stream '{name}' must define either 'years' coverage or count thresholds")
        if has_years and pattern is None:
            raise ValueError(f"Stream '{name}' has 'years' but is missing 'pattern'")

        return cls(
            name=name,
            years=years,
            pattern=pattern,
            require=require,
            open_file=open_file,
            agency_id_policy=agency_id_policy,
            count_pattern=count_pattern,
            min_count=int(min_count) if min_count is not None else None,
            max_count=int(max_count) if max_count is not None else None,
            partial=partial,
            partial_min_count=int(partial_min_count) if partial_min_count is not None else None,
            partial_max_count=int(partial_max_count) if partial_max_count is not None else None,
            warn_only=warn_only,
            integrity=integrity,
        )


@dataclass(frozen=True)
class GroupSection:
    name: str
    subset: Subset = field(default_factory=Subset)
    streams: tuple[StreamCheck, ...] = ()

    # Names the dstore_config repo (e.g. "raw", "formatted", "screened") whose
    # filename_templates/registry govern every file this group looks at. A
    # group validates one physical tier, so this is a single group-level
    # label, not a per-stream one. Required whenever any stream in the group
    # declares `integrity` checks.
    repo_label: Optional[str] = None

    # Restricts the group's bulk repo-vs-staging relative count check
    # (n_count_fract) to files covering cutoff_year or later. Tiers without
    # year-sharded filenames (processed, structures) or with multi-decade
    # shards (raw) should set ignore_cutoff_year instead of relying on this.
    cutoff_year: Optional[int] = None
    ignore_cutoff_year: bool = False

    # The "bulk relative" repo-vs-staging count check is opt-in per group:
    # it only runs when bulk_relative_pattern is set (e.g. "*.csv"), matched
    # against both repo and staging via the group's `subset`. n_count_fract/
    # min_count_abs override the spec-level `defaults` of the same name for
    # this group only; set explicitly to e.g. `${defaults.n_count_fract}` to
    # inherit the default visibly rather than silently, or to a different
    # number to tune this group independently.
    bulk_relative_pattern: Optional[str] = None
    n_count_fract: Optional[float] = None
    min_count_abs: Optional[int] = None

    # Optional per-group override of the spec-level `locations`. Each tier
    # (raw/formatted/screened/...) normally has its own physical spot_target;
    # set these when one `locations` block can't serve every group section.
    repo_dir: Optional[str] = None
    staging_dir: Optional[str] = None

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "GroupSection":
        name = _expect_str(value, "name")
        subset = Subset.from_mapping(value.get("subset"))
        raw_streams = value.get("streams", []) or []
        if not isinstance(raw_streams, list):
            raise TypeError(f"Group '{name}' streams must be a list")
        streams = tuple(StreamCheck.from_mapping(item) for item in raw_streams)
        cutoff_year = value.get("cutoff_year")
        repo_dir = value.get("repo")
        staging_dir = value.get("staging", value.get("spot_target"))
        repo_label = value.get("repo_label")
        bulk_relative_pattern = value.get("bulk_relative_pattern")
        n_count_fract = value.get("n_count_fract")
        min_count_abs = value.get("min_count_abs")
        if any(stream.integrity for stream in streams) and not repo_label:
            raise ValueError(f"Group '{name}' has stream(s) with integrity checks but no repo_label")
        return cls(
            name=name,
            subset=subset,
            streams=streams,
            repo_label=str(repo_label) if repo_label is not None else None,
            cutoff_year=int(cutoff_year) if cutoff_year is not None else None,
            ignore_cutoff_year=bool(value.get("ignore_cutoff_year", False)),
            bulk_relative_pattern=str(bulk_relative_pattern) if bulk_relative_pattern is not None else None,
            n_count_fract=float(n_count_fract) if n_count_fract is not None else None,
            min_count_abs=int(min_count_abs) if min_count_abs is not None else None,
            repo_dir=str(repo_dir) if repo_dir is not None else None,
            staging_dir=str(staging_dir) if staging_dir is not None else None,
        )


@dataclass(frozen=True)
class SpotCheckSpec:
    locations: dict[str, str]
    defaults: dict[str, Any]
    groups: tuple[GroupSection, ...]

    @classmethod
    def from_yaml(cls, config_path: str | os.PathLike[str]) -> "SpotCheckSpec":
        cfg = _load_config(config_path)

        if any(key in cfg for key in ("repo_dirs", "staging_dirs", "tiers")):
            raise ValueError(
                "Legacy spot_check YAML format (groups/tiers/repo_dirs/staging_dirs) is no longer supported. "
                "Use the repo-first schema with `locations` and `groups`."
            )

        locations = cfg.get("locations", {})
        if not isinstance(locations, Mapping):
            raise TypeError("Expected a mapping at the top-level `locations` key")

        raw_groups = cfg.get("groups", []) or []
        if not isinstance(raw_groups, list):
            raise TypeError("Expected a list at the top-level `groups` key")

        return cls(
            locations={str(k): str(v) for k, v in dict(locations).items()},
            defaults=dict(cfg.get("defaults", {}) or {}),
            groups=tuple(GroupSection.from_mapping(item) for item in raw_groups),
        )


def run_spot_check(
    config_path: str,
    *,
    groups: Optional[Sequence[str]] = None,
    streams: Optional[Sequence[str]] = None,
    cutoff_year: Optional[int] = None,
) -> SpotCheckResult:
    """Run repo-first spot checks defined in YAML.

    The proof-of-concept legacy `groups`/`tiers` layout (top-level `repo_dirs`/
    `staging_dirs`/`tiers`) is intentionally rejected here; the active model is
    a repo-vs-staging validation built around `locations` and a list of named
    `groups`, each containing `streams`.

    ``cutoff_year`` overrides each group's configured ``cutoff_year``
    for the bulk relative (repo-vs-staging) count comparison; a group with
    ``ignore_cutoff_year: true`` ignores it regardless (tiers without a
    per-year filename shard, or with multi-decade shards, should set that).
    That comparison itself is opt-in per group via ``bulk_relative_pattern``
    (see ``GroupSection``) -- a group without it runs no repo-vs-staging
    ratio check at all, only its declared ``streams``.

    Failures are accumulated and logged as they are found, but this function
    never raises or exits early on a failed check — every group and
    stream runs and is logged (with a final summary line) before returning,
    so a single unsupervised run yields a complete log for triage.
    """
    spec = SpotCheckSpec.from_yaml(config_path)

    selected_groups = set(groups) if groups else None
    selected_streams = set(streams) if streams else None

    failures = SpotCheckFailureCounts()
    warnings = 0
    groups_checked = 0
    streams_checked = 0

    default_repo_root = Path(spec.locations.get("repo", spec.locations.get("repo_dir", "")))
    default_staging_root = Path(spec.locations.get("staging", spec.locations.get("staging_dir", "")))

    for group in spec.groups:
        if selected_groups is not None and group.name not in selected_groups:
            continue
        groups_checked += 1

        repo_root = Path(group.repo_dir) if group.repo_dir else default_repo_root
        staging_root = Path(group.staging_dir) if group.staging_dir else default_staging_root
        if not repo_root or not repo_root.exists():
            raise FileNotFoundError(f"Repo root does not exist for group '{group.name}': {repo_root}")
        if not staging_root or not staging_root.exists():
            raise FileNotFoundError(f"Staging root (spot_target) does not exist for group '{group.name}': {staging_root}")

        if group.bulk_relative_pattern:
            repo_matches = _collect_matches(repo_root, group.subset, pattern=group.bulk_relative_pattern)
            staging_matches = _collect_matches(staging_root, group.subset, pattern=group.bulk_relative_pattern)

            effective_cutoff = None if group.ignore_cutoff_year else (
                cutoff_year if cutoff_year is not None else group.cutoff_year
            )
            if effective_cutoff is not None:
                repo_matches = _filter_files_from_year(repo_matches, effective_cutoff)
                staging_matches = _filter_files_from_year(staging_matches, effective_cutoff)

            n_count_fract = (
                group.n_count_fract if group.n_count_fract is not None else float(spec.defaults.get("n_count_fract", 0.9))
            )
            min_count_abs = group.min_count_abs if group.min_count_abs is not None else spec.defaults.get("min_count_abs")
            required_min = _required_min_count(n_count_fract, min_count_abs, repo_matches)

            streams_checked += 1
            staging_count = len(staging_matches)
            if staging_count < required_min:
                failures = SpotCheckFailureCounts(
                    count_failures=failures.count_failures + 1,
                    required_missing=failures.required_missing,
                    required_open_failures=failures.required_open_failures,
                    integrity_failures=failures.integrity_failures,
                )
                log.error(
                    "COUNT FAIL (bulk relative) group=%s pattern=%s repo=%d staging=%d required_min=%d "
                    "(=ceil(repo*n_count_fract=%.2f), floor min_count_abs=%s) cutoff_year=%s",
                    group.name,
                    group.bulk_relative_pattern,
                    len(repo_matches),
                    staging_count,
                    required_min,
                    n_count_fract,
                    min_count_abs,
                    effective_cutoff,
                )
            else:
                log.info(
                    "COUNT PASS (bulk relative) group=%s pattern=%s repo=%d staging=%d required_min=%d "
                    "(=ceil(repo*n_count_fract=%.2f), floor min_count_abs=%s) cutoff_year=%s",
                    group.name,
                    group.bulk_relative_pattern,
                    len(repo_matches),
                    staging_count,
                    required_min,
                    n_count_fract,
                    min_count_abs,
                    effective_cutoff,
                )

        for stream in group.streams:
            if selected_streams is not None and stream.name not in selected_streams:
                continue
            streams_checked += 1

            has_count = stream.partial or any(
                v is not None
                for v in (stream.min_count, stream.max_count, stream.partial_min_count, stream.partial_max_count)
            )
            count_candidates: Optional[list[Path]] = None

            if has_count:
                count_pattern = stream.count_pattern or stream.pattern or "*"
                count_candidates = _collect_matches(staging_root, group.subset, pattern=count_pattern)
                found = len(count_candidates)
                lo, hi = (stream.partial_min_count, stream.partial_max_count) if stream.partial else (
                    stream.min_count,
                    stream.max_count,
                )
                if stream.partial and lo is None and hi is None:
                    log.info(
                        "COUNT SKIP (partial, no thresholds yet) group=%s stream=%s found=%d",
                        group.name,
                        stream.name,
                        found,
                    )
                elif (lo is not None and found < lo) or (hi is not None and found > hi):
                    if stream.warn_only:
                        warnings += 1
                        log.warning(
                            "COUNT WARN group=%s stream=%s found=%d min=%s max=%s partial=%s",
                            group.name,
                            stream.name,
                            found,
                            lo,
                            hi,
                            stream.partial,
                        )
                    else:
                        failures = SpotCheckFailureCounts(
                            count_failures=failures.count_failures + 1,
                            required_missing=failures.required_missing,
                            required_open_failures=failures.required_open_failures,
                            integrity_failures=failures.integrity_failures,
                        )
                        log.error(
                            "COUNT FAIL group=%s stream=%s found=%d min=%s max=%s partial=%s",
                            group.name,
                            stream.name,
                            found,
                            lo,
                            hi,
                            stream.partial,
                        )
                else:
                    log.info(
                        "COUNT PASS group=%s stream=%s found=%d min=%s max=%s partial=%s",
                        group.name,
                        stream.name,
                        found,
                        lo,
                        hi,
                        stream.partial,
                    )

            if stream.integrity:
                integrity_paths = count_candidates
                if integrity_paths is None:
                    integrity_paths = _collect_matches(staging_root, group.subset, pattern=stream.pattern or "*")
                n_fail, n_checked, messages = _run_integrity_checks(
                    integrity_paths, group.repo_label, stream.integrity, stream.agency_id_policy
                )
                if n_fail:
                    failures = SpotCheckFailureCounts(
                        count_failures=failures.count_failures,
                        required_missing=failures.required_missing,
                        required_open_failures=failures.required_open_failures,
                        integrity_failures=failures.integrity_failures + n_fail,
                    )
                    for msg in messages:
                        log.error("INTEGRITY FAIL group=%s stream=%s %s", group.name, stream.name, msg)
                log.info(
                    "INTEGRITY SUMMARY group=%s stream=%s checked=%d failed=%d",
                    group.name,
                    stream.name,
                    n_checked,
                    n_fail,
                )

            if not stream.years:
                continue

            repo_candidates = _collect_matches(repo_root, group.subset, pattern=stream.pattern)
            staging_candidates = _collect_matches(staging_root, group.subset, pattern=stream.pattern)

            for year in stream.years:
                matched_repo = _filter_files_covering_year(repo_candidates, year)
                matched_stage = _filter_files_covering_year(staging_candidates, year)
                if not matched_stage:
                    if stream.require:
                        failures = SpotCheckFailureCounts(
                            count_failures=failures.count_failures,
                            required_missing=failures.required_missing + 1,
                            required_open_failures=failures.required_open_failures,
                            integrity_failures=failures.integrity_failures,
                        )
                        log.error(
                            "MISSING FILE group=%s stream=%s year=%d repo_dir=%s staging_dir=%s",
                            group.name,
                            stream.name,
                            year,
                            repo_root,
                            staging_root,
                        )
                    else:
                        warnings += 1
                        log.warning(
                            "OPTIONAL MISSING group=%s stream=%s year=%d repo_dir=%s staging_dir=%s",
                            group.name,
                            stream.name,
                            year,
                            repo_root,
                            staging_root,
                        )
                    continue

                chosen = _choose_best_candidate(matched_stage)
                if not stream.open_file:
                    continue
                try:
                    _ = read_ts(chosen)
                    log.info(
                        "OPEN PASS group=%s stream=%s year=%d file=%s",
                        group.name,
                        stream.name,
                        year,
                        chosen,
                    )
                except Exception as exc:  # pragma: no cover - exercised in integration-like usage
                    if stream.require:
                        failures = SpotCheckFailureCounts(
                            count_failures=failures.count_failures,
                            required_missing=failures.required_missing,
                            required_open_failures=failures.required_open_failures + 1,
                            integrity_failures=failures.integrity_failures,
                        )
                        log.error(
                            "OPEN FAIL group=%s stream=%s year=%d file=%s err=%s: %s",
                            group.name,
                            stream.name,
                            year,
                            chosen,
                            type(exc).__name__,
                            exc,
                        )
                    else:
                        warnings += 1
                        log.warning(
                            "OPTIONAL OPEN FAIL group=%s stream=%s year=%d file=%s err=%s: %s",
                            group.name,
                            stream.name,
                            year,
                            chosen,
                            type(exc).__name__,
                            exc,
                        )

    log.info(
        "SPOT CHECK SUMMARY groups_checked=%d streams_checked=%d count_failures=%d "
        "required_missing=%d required_open_failures=%d integrity_failures=%d warnings=%d exit_code=%d",
        groups_checked,
        streams_checked,
        failures.count_failures,
        failures.required_missing,
        failures.required_open_failures,
        failures.integrity_failures,
        warnings,
        failures.exit_code(),
    )

    return SpotCheckResult(
        failures=failures,
        warnings=warnings,
        groups_checked=groups_checked,
        streams_checked=streams_checked,
    )


def _collect_matches(root: Path, subset: Subset, pattern: str) -> list[Path]:
    if not root.exists():
        return []
    out: list[Path] = []
    for candidate in root.glob(pattern):
        if candidate.is_file() and subset.matches(candidate):
            out.append(candidate)
    return sorted(_dedupe_paths(out), key=lambda p: p.name)


def _required_min_count(n_count_fract: float, min_count_abs: Optional[int], repo_matches: Sequence[Path]) -> int:
    repo_count = len(repo_matches)
    if repo_count <= 0:
        return int(min_count_abs) if min_count_abs is not None else 0
    return max(int(math.ceil(repo_count * n_count_fract)), int(min_count_abs) if min_count_abs is not None else 0)


def _dedupe_paths(paths: Iterable[Path]) -> list[Path]:
    seen: set[Path] = set()
    out: list[Path] = []
    for p in paths:
        rp = p.resolve()
        if rp in seen:
            continue
        seen.add(rp)
        out.append(p)
    return out


_YEAR_RE = re.compile(r"(?<!\d)(19\d{2}|20\d{2})(?!\d)")
_RANGE_RE = re.compile(r"(?<!\d)(19\d{2}|20\d{2}|9999)_(19\d{2}|20\d{2}|9999)(?!\d)")


def _filter_files_covering_year(paths: Sequence[Path], year: int) -> list[Path]:
    out: list[Path] = []
    for p in paths:
        name = p.name
        m = _RANGE_RE.search(name)
        if m:
            y0 = int(m.group(1))
            y1 = int(m.group(2))
            if y0 <= year <= y1:
                out.append(p)
                continue
        years_found = [int(x) for x in _YEAR_RE.findall(name)]
        if year in years_found:
            out.append(p)
    return out


def _filter_files_from_year(paths: Sequence[Path], min_year: int) -> list[Path]:
    """Keep files whose name indicates data at or after ``min_year``.

    A ranged shard (``syear_eyear``) is kept if its span reaches ``min_year``;
    a single-year file is kept if its year is ``>= min_year``.
    """
    out: list[Path] = []
    for p in paths:
        name = p.name
        m = _RANGE_RE.search(name)
        if m:
            y1 = int(m.group(2))
            if y1 >= min_year:
                out.append(p)
            continue
        years_found = [int(x) for x in _YEAR_RE.findall(name)]
        if years_found and max(years_found) >= min_year:
            out.append(p)
    return out


def _run_integrity_checks(
    paths: Sequence[Path],
    repo_label: str,
    checks: Sequence[str],
    agency_id_policy_override: Optional[IdentityPolicy],
) -> tuple[int, int, list[str]]:
    """Run cheap, filename-only integrity checks over ``paths``.

    Filename parsing uses ``repo_label``'s configured ``filename_templates``;
    registry lookups use its configured registry. No file contents are read,
    so this is affordable to run over every matched file rather than a sample.

    Returns
    -------
    tuple[int, int, list[str]]
        ``(failure_count, checked_count, failure_messages)``.
    """
    failures = 0
    checked = 0
    messages: list[str] = []

    needs_registry = "station_in_registry" in checks or "agency_id_matches_registry" in checks
    registry_by_lower: dict[str, str] = {}
    if needs_registry:
        registry = dstore_config.repo_registry(repo=repo_label)
        registry_by_lower = {str(idx).lower(): idx for idx in registry.index}

    for path in paths:
        checked += 1
        try:
            meta = interpret_fname(path.name, repo=repo_label)
        except Exception as exc:
            if "filename_correct" in checks:
                failures += 1
                messages.append(f"filename_correct file={path.name} err={exc}")
            continue

        station_id = str(meta.get("station_id", "")).strip().lower()

        if "station_in_registry" in checks and station_id not in registry_by_lower:
            failures += 1
            messages.append(f"station_in_registry file={path.name} station_id={station_id!r}")
            continue

        if "agency_id_matches_registry" in checks and station_id in registry_by_lower:
            registry_idx = registry_by_lower[station_id]
            # Repo convention lowercases filename tokens; registry values are
            # not guaranteed to be, so normalize case before comparing.
            expected = str(registry.loc[registry_idx, "agency_id"]).lower()
            actual = str(meta.get("agency_id", "")).lower()
            policy = agency_id_policy_override or _default_agency_id_policy(str(meta.get("agency", "")))
            if not policy.matches(actual, expected):
                failures += 1
                messages.append(
                    f"agency_id_matches_registry file={path.name} agency_id={actual!r} "
                    f"registry_agency_id={expected!r}"
                )

    return failures, checked, messages


def _choose_best_candidate(paths: Sequence[Path]) -> Path:
    return sorted(paths, key=lambda p: (p.stat().st_mtime, p.name))[-1]


def _expect_mapping(value: Any, key: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"Expected mapping for {key!r}, got {type(value).__name__}")
    return value


def _expect_list(value: Any, key: str) -> list[Any]:
    if not isinstance(value, list):
        raise TypeError(f"Expected list for {key!r}, got {type(value).__name__}")
    return value


def _expect_str(value: Mapping[str, Any], key: str) -> str:
    if key not in value:
        raise ValueError(f"Missing required key: {key!r}")
    v = value[key]
    if not isinstance(v, str) or not v:
        raise TypeError(f"Expected non-empty string for key {key!r}, got {type(v).__name__}")
    return v


def _expect_string_list(value: Any, *, context: str) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, (list, tuple)) and all(isinstance(item, str) and item for item in value):
        return list(value)
    raise TypeError(f"Expected string or list[str] for {context}, got {type(value).__name__}")


def _load_config(config_path: str | os.PathLike[str]) -> dict[str, Any]:
    value = str(config_path)
    if os.path.exists(value):
        resolved = value
    else:
        try:
            resolved = dstore_config.config_file(value)
        except ValueError:
            raise FileNotFoundError(f"Configuration file not found: {value}") from None

    if not os.path.exists(resolved):
        raise FileNotFoundError(str(resolved))
    cfg = OmegaConf.load(str(resolved))
    out = OmegaConf.to_container(cfg, resolve=True)
    if not isinstance(out, dict):
        raise ValueError("Top-level YAML must be a mapping/dict.")
    return out


@click.command(
    name="spot_check",
    context_settings={"help_option_names": ["-h", "--help"]},
)
@click.option(
    "--config",
    "config_path",
    type=str,
    required=True,
    help="Path to a repo-first spot-check YAML spec or a configured spec label.",
)
@click.option(
    "--group",
    "groups",
    multiple=True,
    help="Group(s) to run (spec `groups[].name`). Repeatable: --group foo --group bar.",
)
@click.option(
    "--stream",
    "streams",
    multiple=True,
    help="Optional stream name(s) to run within the selected group(s) (spec `streams[].name`).",
)
@click.option(
    "--cutoff-year",
    "cutoff_year",
    type=int,
    default=None,
    help=(
        "Override each group's configured cutoff_year for the bulk relative "
        "repo-vs-staging count comparison. Ignored by groups with ignore_cutoff_year: true."
    ),
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
    config_path: str,
    groups: tuple[str, ...],
    streams: tuple[str, ...],
    cutoff_year: int | None,
    logdir: Path | None,
    logfile_prefix: str,
    debug: bool,
    verbose: bool,
    quiet: bool,
    loglevel: str | None,
) -> None:
    """Run repo-first spot checks for staged downloads versus the repo."""
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
        streams=list(streams) if streams else None,
        cutoff_year=cutoff_year,
    )
    raise SystemExit(result.failures.exit_code())


if __name__ == "__main__":
    spot_check_cli()
