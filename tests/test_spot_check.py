from __future__ import annotations

from pathlib import Path

import pytest

import dms_datastore.dstore_config as cfgmod
from dms_datastore.spot_check import (
    GroupSection,
    SpotCheckSpec,
    StreamCheck,
    Subset,
    run_spot_check,
)


def test_spot_check_spec_parses_repo_first_yaml(tmp_path: Path) -> None:
    repo_dir = tmp_path / "repo"
    staging_dir = tmp_path / "staging"
    repo_dir.mkdir()
    staging_dir.mkdir()
    (repo_dir / "usgs_anh_11303500_flow_2024.csv").write_text("x\n")
    (staging_dir / "usgs_anh_11303500_flow_2024.csv").write_text("x\n")

    yaml_path = tmp_path / "spot_check_spec.yaml"
    yaml_path.write_text(
        """
locations:
  repo: '{repo_dir}'
  staging: '{staging_dir}'
groups:
  - name: demo
    subset:
      include: ["usgs_*.csv"]
    streams:
      - name: flow_2024
        years: [2024]
        pattern: "usgs_*.csv"
""".format(repo_dir=str(repo_dir).replace("\\", "/"), staging_dir=str(staging_dir).replace("\\", "/"))
    )

    spec = SpotCheckSpec.from_yaml(yaml_path)
    assert isinstance(spec, SpotCheckSpec)
    assert len(spec.groups) == 1
    assert isinstance(spec.groups[0], GroupSection)
    assert isinstance(spec.groups[0].subset, Subset)
    assert isinstance(spec.groups[0].streams[0], StreamCheck)
    assert spec.groups[0].streams[0].agency_id_policy is None

    result = run_spot_check(str(yaml_path))
    assert result.failures.count_failures == 0
    assert result.failures.required_missing == 0


def test_legacy_group_tier_yaml_is_rejected(tmp_path: Path) -> None:
    cfg = tmp_path / "legacy.yaml"
    cfg.write_text(
        """
repo_dirs:
  screened: /tmp/repo
staging_dirs:
  screened: /tmp/staging
groups:
  - name: demo
    tiers:
      screened:
        pattern: "*.csv"
"""
    )

    with pytest.raises(ValueError, match="legacy|group.*tier"):
        run_spot_check(str(cfg))


# The tests below exercise the count-check, integrity-check, and cutoff_year
# additions entirely against tmp_path fixtures (and, where a registry is
# needed, an isolated dstore_config), so none require network/shared-repo
# access and none carry @pytest.mark.integration.


def _write_yaml(tmp_path: Path, repo_dir: Path, staging_dir: Path, groups_yaml: str) -> Path:
    yaml_path = tmp_path / "spec.yaml"
    yaml_path.write_text(
        """
locations:
  repo: '{repo_dir}'
  staging: '{staging_dir}'
groups:
{groups_yaml}
""".format(
            repo_dir=str(repo_dir).replace("\\", "/"),
            staging_dir=str(staging_dir).replace("\\", "/"),
            groups_yaml=groups_yaml,
        )
    )
    return yaml_path


def test_absolute_count_check_pass_and_fail(tmp_path: Path) -> None:
    repo_dir = tmp_path / "repo"
    staging_dir = tmp_path / "staging"
    repo_dir.mkdir()
    staging_dir.mkdir()
    for i in range(5):
        (staging_dir / f"noaa_st{i}_a{i}_predictions_2024.csv").write_text("x\n")

    yaml_path = _write_yaml(
        tmp_path,
        repo_dir,
        staging_dir,
        """\
  - name: demo
    streams:
      - name: total_count
        min_count: 10
        max_count: 100
""",
    )

    result = run_spot_check(str(yaml_path))
    assert result.failures.count_failures == 1

    yaml_path_pass = _write_yaml(
        tmp_path,
        repo_dir,
        staging_dir,
        """\
  - name: demo
    streams:
      - name: total_count
        min_count: 1
        max_count: 100
""",
    )
    result = run_spot_check(str(yaml_path_pass))
    assert result.failures.count_failures == 0


def test_warn_only_count_check_warns_instead_of_failing(tmp_path: Path) -> None:
    repo_dir = tmp_path / "repo"
    staging_dir = tmp_path / "staging"
    repo_dir.mkdir()
    staging_dir.mkdir()
    for i in range(2):
        (staging_dir / f"des_st{i}_a{i}_flow_2024.csv").write_text("x\n")

    yaml_path = _write_yaml(
        tmp_path,
        repo_dir,
        staging_dir,
        """\
  - name: demo
    streams:
      - name: des_count
        count_pattern: "des_*.csv"
        min_count: 10
        warn_only: true
""",
    )

    result = run_spot_check(str(yaml_path))
    assert result.failures.count_failures == 0
    assert result.warnings == 1


def test_partial_stream_without_thresholds_is_skipped(tmp_path: Path) -> None:
    repo_dir = tmp_path / "repo"
    staging_dir = tmp_path / "staging"
    repo_dir.mkdir()
    staging_dir.mkdir()
    (staging_dir / "noaa_st1_a1_predictions_2024.csv").write_text("x\n")

    yaml_path = _write_yaml(
        tmp_path,
        repo_dir,
        staging_dir,
        """\
  - name: demo
    streams:
      - name: noaa_predictions
        partial: true
        min_count: 1000000
""",
    )

    result = run_spot_check(str(yaml_path))
    assert result.failures.count_failures == 0


def test_cutoff_year_filters_relative_count_check(tmp_path: Path) -> None:
    repo_dir = tmp_path / "repo"
    staging_dir = tmp_path / "staging"
    repo_dir.mkdir()
    staging_dir.mkdir()
    # Old years present in repo but never re-staged; recent year present in both.
    for year in (2005, 2010, 2024):
        (repo_dir / f"usgs_anh_11303500_flow_{year}.csv").write_text("x\n")
    (staging_dir / "usgs_anh_11303500_flow_2024.csv").write_text("x\n")

    yaml_path = _write_yaml(
        tmp_path,
        repo_dir,
        staging_dir,
        """\
  - name: no_cutoff
    cutoff_year: 2024
    bulk_relative_pattern: "*.csv"
  - name: with_cutoff
    bulk_relative_pattern: "*.csv"
""",
    )
    # Without a cutoff, staging (1 file) falls well short of repo (3 files).
    result = run_spot_check(str(yaml_path), groups=["with_cutoff"])
    assert result.failures.count_failures == 1

    # cutoff_year=2024 restricts the comparison to the recent shard only.
    result = run_spot_check(str(yaml_path), groups=["no_cutoff"])
    assert result.failures.count_failures == 0


def test_bulk_relative_check_is_opt_in(tmp_path: Path) -> None:
    repo_dir = tmp_path / "repo"
    staging_dir = tmp_path / "staging"
    repo_dir.mkdir()
    staging_dir.mkdir()
    for year in (2020, 2021, 2022, 2023):
        (repo_dir / f"usgs_anh_11303500_flow_{year}.csv").write_text("x\n")
    # Staging is far short of repo, but no bulk_relative_pattern is set.

    yaml_path = _write_yaml(
        tmp_path,
        repo_dir,
        staging_dir,
        """\
  - name: no_bulk_check
""",
    )
    result = run_spot_check(str(yaml_path), groups=["no_bulk_check"])
    assert result.failures.count_failures == 0


def test_bulk_relative_check_group_override_of_n_count_fract(tmp_path: Path) -> None:
    repo_dir = tmp_path / "repo"
    staging_dir = tmp_path / "staging"
    repo_dir.mkdir()
    staging_dir.mkdir()
    for year in (2020, 2021, 2022, 2023):
        (repo_dir / f"usgs_anh_11303500_flow_{year}.csv").write_text("x\n")
    (staging_dir / "usgs_anh_11303500_flow_2020.csv").write_text("x\n")
    # staging=1 of repo=4 (25%): fails the spec-level default (0.9) but passes
    # a group-level override loose enough to allow it.

    yaml_path = _write_yaml(
        tmp_path,
        repo_dir,
        staging_dir,
        """\
  - name: strict
    bulk_relative_pattern: "*.csv"
  - name: loose
    bulk_relative_pattern: "*.csv"
    n_count_fract: 0.2
""",
    )
    result = run_spot_check(str(yaml_path), groups=["strict"])
    assert result.failures.count_failures == 1

    result = run_spot_check(str(yaml_path), groups=["loose"])
    assert result.failures.count_failures == 0


def test_stream_requires_years_or_count_thresholds(tmp_path: Path) -> None:
    repo_dir = tmp_path / "repo"
    staging_dir = tmp_path / "staging"
    repo_dir.mkdir()
    staging_dir.mkdir()

    yaml_path = _write_yaml(
        tmp_path,
        repo_dir,
        staging_dir,
        """\
  - name: demo
    streams:
      - name: underspecified
""",
    )
    with pytest.raises(ValueError, match="years.*count thresholds"):
        run_spot_check(str(yaml_path))


def test_check_integrity_requires_repo_label(tmp_path: Path) -> None:
    repo_dir = tmp_path / "repo"
    staging_dir = tmp_path / "staging"
    repo_dir.mkdir()
    staging_dir.mkdir()

    yaml_path = _write_yaml(
        tmp_path,
        repo_dir,
        staging_dir,
        """\
  - name: demo
    streams:
      - name: needs_label
        min_count: 1
        integrity: [filename_correct]
""",
    )
    with pytest.raises(ValueError, match="repo_label"):
        run_spot_check(str(yaml_path))


@pytest.fixture
def isolated_registry_config(monkeypatch, tmp_path):
    """Isolated dstore_config with a tiny 'formatted' repo + registry for integrity tests."""
    config_dir = tmp_path / "config_data"
    config_dir.mkdir()

    registry = config_dir / "stations.csv"
    registry.write_text(
        "station_id,agency_id,agency\n"
        "anh,11303500,usgs\n"
        "wdcut,B95225,ncro\n",
        encoding="utf-8",
    )

    unused_root = tmp_path / "unused_root"
    unused_root.mkdir()

    config = {
        "registries": {"stations": "stations.csv"},
        "repos": {
            "formatted": {
                "root": str(unused_root),
                "registry": "stations",
                "provider_key": "source",
                "provider_resolution_mode": "assume_unique",
                "filename_templates": ["{source}_{station_id@subloc}_{agency_id}_{param}_{year}.csv"],
            },
        },
    }

    monkeypatch.setattr(cfgmod, "config", config)
    monkeypatch.setattr(cfgmod, "localdir", str(config_dir))
    monkeypatch.setattr(cfgmod, "_registry_cache", {})
    monkeypatch.setattr(cfgmod, "_repo_cache", None)
    return config_dir


def test_integrity_checks_flag_bad_filename_and_agency_mismatch(
    tmp_path: Path, isolated_registry_config: Path
) -> None:
    repo_dir = tmp_path / "repo"
    staging_dir = tmp_path / "staging"
    repo_dir.mkdir()
    staging_dir.mkdir()

    (staging_dir / "usgs_anh_11303500_flow_2024.csv").write_text("x\n")  # correct
    (staging_dir / "usgs_anh_99999999_flow_2024.csv").write_text("x\n")  # agency_id mismatch
    (staging_dir / "not_a_valid_name.csv").write_text("x\n")  # unparsable

    yaml_path = _write_yaml(
        tmp_path,
        repo_dir,
        staging_dir,
        """\
  - name: demo
    repo_label: formatted
    streams:
      - name: full_check
        min_count: 1
        integrity: [filename_correct, station_in_registry, agency_id_matches_registry]
""",
    )

    result = run_spot_check(str(yaml_path))
    # 1 unparsable filename + 1 agency_id mismatch
    assert result.failures.integrity_failures == 2


def test_agency_id_matches_registry_is_case_insensitive(
    tmp_path: Path, isolated_registry_config: Path
) -> None:
    """Registry agency_id case must not cause a false failure (registry has 'B95225',
    repo convention lowercases filenames to 'b9522500' -- the numeric suffix variant)."""
    repo_dir = tmp_path / "repo"
    staging_dir = tmp_path / "staging"
    repo_dir.mkdir()
    staging_dir.mkdir()

    (staging_dir / "ncro_wdcut_b9522500_ec_2020_2025.csv").write_text("x\n")

    yaml_path = _write_yaml(
        tmp_path,
        repo_dir,
        staging_dir,
        """\
  - name: demo
    repo_label: formatted
    streams:
      - name: full_check
        min_count: 1
        integrity: [agency_id_matches_registry]
""",
    )

    result = run_spot_check(str(yaml_path))
    assert result.failures.integrity_failures == 0


def test_group_location_override(tmp_path: Path) -> None:
    shared_repo = tmp_path / "shared_repo"
    shared_staging = tmp_path / "shared_staging"
    shared_repo.mkdir()
    shared_staging.mkdir()

    other_staging = tmp_path / "other_staging"
    other_staging.mkdir()
    (other_staging / "usgs_anh_11303500_flow_2024.csv").write_text("x\n")

    yaml_path = tmp_path / "spec.yaml"
    yaml_path.write_text(
        """
locations:
  repo: '{repo_dir}'
  staging: '{staging_dir}'
groups:
  - name: override_target
    staging: '{other_staging}'
    streams:
      - name: total_count
        min_count: 1
""".format(
            repo_dir=str(shared_repo).replace("\\", "/"),
            staging_dir=str(shared_staging).replace("\\", "/"),
            other_staging=str(other_staging).replace("\\", "/"),
        )
    )

    result = run_spot_check(str(yaml_path))
    assert result.failures.count_failures == 0

