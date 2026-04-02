"""Tests that auto_screen() catches per-station failures and writes a CSV."""

import os
import yaml
import pandas as pd
import pytest
from pathlib import Path

import dms_datastore.auto_screen as auto_screen_mod


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FAILURE_KEYS = {"station_id", "subloc", "param", "step", "exc_type", "message"}

# Minimal protocol dict that screener/context_config would return.
_MINIMAL_PROTO = {"inherits_global": False, "steps": []}

# Minimal YAML config written to a file; load_config() parses it from disk.
_MINIMAL_SCREEN_YAML = """
global:
  inherits_global: false
  steps: []
"""


def _write_config(path: Path) -> str:
    """Write a minimal screen config YAML and return the path as string."""
    path.write_text(_MINIMAL_SCREEN_YAML)
    return str(path)


def _fake_inventory():
    return pd.DataFrame([
        {
            "station_id": "sta1",
            "subloc": "default",
            "param": "flow",
            "agency": "usgs",
            "agency_id": "11455420",
        }
    ])


def _fake_station_db():
    return pd.DataFrame(
        {"name": ["Station One"]},
        index=pd.Index(["sta1"], name="station_id"),
    )


def _minimal_ts():
    idx = pd.date_range("2020-01-01", periods=100, freq="15min")
    return pd.DataFrame({"value": range(100), "user_flag": 0}, index=idx)


def _setup_common_patches(monkeypatch, *, fetcher_fn=None):
    """Apply monkeypatches that are common to all auto_screen tests."""
    monkeypatch.setattr(auto_screen_mod, "repo_data_inventory",
                        lambda repo=None, in_path=None: _fake_inventory())
    monkeypatch.setattr(auto_screen_mod, "station_dbase", _fake_station_db)
    # Bypass the complex context_config logic (requires region files etc.).
    monkeypatch.setattr(auto_screen_mod, "context_config",
                        lambda cfg, station_id, subloc, param: _MINIMAL_PROTO)
    # meta_to_filename needs the screened repo config file on disk; stub it out.
    monkeypatch.setattr(auto_screen_mod, "meta_to_filename",
                        lambda meta, **kw: "usgs_sta1_11455420_flow.csv")

    if fetcher_fn is None:
        # Default fetcher: returns a valid (metas, ts) tuple
        def _default_fetcher(source_repo, station_id, param, subloc=None, data_path=None):
            meta = {
                "agency": "usgs",
                "station_id": station_id,
                "subloc": subloc or "default",
                "sublocation": subloc or "default",
                "param": param,
            }
            return ([meta], _minimal_ts())

        fetcher_fn = _default_fetcher

    monkeypatch.setattr(
        auto_screen_mod, "custom_fetcher", lambda agency: fetcher_fn
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_auto_screen_catches_screener_failure(tmp_path, monkeypatch):
    """When screener() raises, the failure is recorded and processing does not abort."""
    failures_file = tmp_path / "screen_failures.csv"
    config_file = _write_config(tmp_path / "screen.yaml")

    _setup_common_patches(monkeypatch)

    def _bad_screener(ts, station_id, subloc, param, protocol, *args, **kwargs):
        raise RuntimeError("screener exploded")

    monkeypatch.setattr(auto_screen_mod, "screener", _bad_screener)
    monkeypatch.setattr(auto_screen_mod, "write_ts_csv", lambda *a, **kw: None)

    auto_screen_mod.auto_screen(
        fpath=str(tmp_path),
        config=config_file,
        dest=str(tmp_path / "screened"),
        failures_file=str(failures_file),
    )

    assert failures_file.exists()
    df = pd.read_csv(failures_file)
    assert len(df) == 1
    row = df.iloc[0]
    assert row["station_id"] == "sta1"
    assert row["step"] == "screen"
    assert row["exc_type"] == "RuntimeError"


def test_auto_screen_catches_write_failure(tmp_path, monkeypatch):
    """When write_ts_csv() raises, the failure is recorded (step='write')."""
    failures_file = tmp_path / "write_failures.csv"
    config_file = _write_config(tmp_path / "screen.yaml")

    _setup_common_patches(monkeypatch)

    # screener returns the input ts unchanged
    monkeypatch.setattr(
        auto_screen_mod, "screener",
        lambda ts, *a, **kw: ts[["value", "user_flag"]]
    )

    def _bad_write(*args, **kwargs):
        raise IOError("disk full")

    monkeypatch.setattr(auto_screen_mod, "write_ts_csv", _bad_write)

    auto_screen_mod.auto_screen(
        fpath=str(tmp_path),
        config=config_file,
        dest=str(tmp_path / "screened"),
        failures_file=str(failures_file),
    )

    assert failures_file.exists()
    df = pd.read_csv(failures_file)
    assert len(df) == 1
    assert df.iloc[0]["step"] == "write"


def test_auto_screen_writes_failures_csv(tmp_path, monkeypatch):
    """Regardless of failure source, auto_screen always writes a CSV."""
    failures_file = tmp_path / "any_failures.csv"
    config_file = _write_config(tmp_path / "screen.yaml")

    _setup_common_patches(monkeypatch)

    monkeypatch.setattr(auto_screen_mod, "screener",
                        lambda *a, **kw: (_ for _ in ()).throw(ValueError("bad proto")))
    monkeypatch.setattr(auto_screen_mod, "write_ts_csv", lambda *a, **kw: None)

    auto_screen_mod.auto_screen(
        fpath=str(tmp_path),
        config=config_file,
        dest=str(tmp_path / "screened"),
        failures_file=str(failures_file),
    )

    assert failures_file.exists()


def test_auto_screen_writes_empty_csv_on_clean_run(tmp_path, monkeypatch):
    """When every station processes cleanly the CSV still exists (header-only)."""
    failures_file = tmp_path / "empty_failures.csv"
    config_file = _write_config(tmp_path / "screen.yaml")

    _setup_common_patches(monkeypatch)

    monkeypatch.setattr(
        auto_screen_mod, "screener",
        lambda ts, *a, **kw: ts[["value", "user_flag"]]
    )
    monkeypatch.setattr(auto_screen_mod, "write_ts_csv", lambda *a, **kw: None)

    auto_screen_mod.auto_screen(
        fpath=str(tmp_path),
        config=config_file,
        dest=str(tmp_path / "screened"),
        failures_file=str(failures_file),
    )

    assert failures_file.exists()
    df = pd.read_csv(failures_file)
    assert len(df) == 0
    assert list(df.columns) == ["station_id", "subloc", "param", "step", "exc_type", "message"]
