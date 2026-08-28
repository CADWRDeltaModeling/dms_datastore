"""Tests for the NCRO agency_id suffix-selection fix in download_ncro.py.

Two layers:

1. Fast, no-network unit tests of `_select_preferred_site()` covering every
   hierarchy branch (default/other params, flow/velocity, elev) across all
   permutations of candidate site ordering, plus the zero-coverage fallback.

2. A real-network integration test (skipped by default) that downloads a
   handful of known "tough" NCRO stations -- verified against
   ncro_inventory_full.csv to genuinely report the same station/param under
   more than one agency_id suffix variant -- and asserts that exactly one
   file is written per station/param, with the expected preferred variant.
   Enable with:

       RUN_NCRO_SUFFIX_HARNESS=1 pytest tests/test_ncro_suffix_selection.py -m integration -v
"""

import glob
import itertools
import os
import threading

import pandas as pd
import pytest

from dms_datastore import download_ncro


def _env_true(name):
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


# ---------------------------------------------------------------------------
# Fast, no-network unit tests for the a priori suffix preference logic.
# ---------------------------------------------------------------------------

# Realistic-length agency ids (6-char base, matching real NCRO codes like
# "B95338"). This matters: _split_agency_suffix() only recognizes a trailing
# "00" as a suffix when len(site_id) > 6, so short synthetic ids like "B123"
# would misclassify "B12300" as a base id instead of a "00" variant.
BASE = "B95338"
CANDIDATES = [BASE, BASE + "Q", BASE + "00"]  # base, q, 00


@pytest.mark.parametrize("paramname", ["temp", "ec", "do", "cla", "unmapped_param"])
def test_select_preferred_site_default_prefers_00_over_q_over_base(paramname):
    for perm in itertools.permutations(CANDIDATES):
        assert download_ncro._select_preferred_site(paramname, list(perm)) == BASE + "00"


@pytest.mark.parametrize("paramname", ["temp", "ec"])
def test_select_preferred_site_falls_back_to_q_when_00_absent(paramname):
    for perm in itertools.permutations([BASE, BASE + "Q"]):
        assert download_ncro._select_preferred_site(paramname, list(perm)) == BASE + "Q"


@pytest.mark.parametrize("paramname", ["temp", "ec"])
def test_select_preferred_site_falls_back_to_base_when_00_and_q_absent(paramname):
    assert download_ncro._select_preferred_site(paramname, [BASE]) == BASE


@pytest.mark.parametrize("paramname", ["flow", "velocity"])
def test_select_preferred_site_flow_velocity_prefer_q(paramname):
    for perm in itertools.permutations(CANDIDATES):
        assert download_ncro._select_preferred_site(paramname, list(perm)) == BASE + "Q"


def test_select_preferred_site_elev_prefers_base():
    for perm in itertools.permutations(CANDIDATES):
        assert download_ncro._select_preferred_site("elev", list(perm)) == BASE


def test_select_preferred_site_single_candidate_returned_regardless_of_param():
    assert download_ncro._select_preferred_site("temp", [BASE + "00"]) == BASE + "00"
    assert download_ncro._select_preferred_site("flow", [BASE + "Q"]) == BASE + "Q"
    assert download_ncro._select_preferred_site("elev", [BASE]) == BASE


def test_split_agency_suffix():
    assert download_ncro._split_agency_suffix("B95338") == ("B95338", "")
    assert download_ncro._split_agency_suffix("b95338q") == ("B95338", "q")
    assert download_ncro._split_agency_suffix("B9533800") == ("B95338", "00")


# ---------------------------------------------------------------------------
# Real-network integration test: a few known tough stations with genuine
# multi-variant WaterTemp coverage (verified against ncro_inventory_full.csv):
#   B95338  -> [q, 00]         both currently active
#   B95410  -> [base, q, 00]   all three variants active
#   B95380  -> [base, q]       no 00 variant at all -- exercises fallback
# ---------------------------------------------------------------------------

TOUGH_AGENCY_IDS = ["B95338", "B95410", "B95380"]


@pytest.mark.integration
@pytest.mark.skipif(
    not _env_true("RUN_NCRO_SUFFIX_HARNESS"),
    reason="Set RUN_NCRO_SUFFIX_HARNESS=1 to run real NCRO suffix-selection downloads",
)
def test_ncro_download_resolves_tough_suffix_permutations(monkeypatch, tmp_path):
    abort_event = threading.Event()
    dfs = []
    for agency_id in TOUGH_AGENCY_IDS:
        df = download_ncro._fetch_inventory_for_station(agency_id, abort_event)
        assert df is not None and not df.empty, f"No live inventory returned for {agency_id}"
        dfs.append(df)
    inventory = pd.concat(dfs, axis=0, ignore_index=True)
    inventory = inventory.loc[inventory["param"] == "WaterTemp", :].reset_index(drop=True)

    monkeypatch.setattr(download_ncro, "load_inventory", lambda *a, **k: inventory)
    monkeypatch.setattr(download_ncro.dstore_config, "station_dbase", lambda: pd.DataFrame())

    # Fixed historical window (not "now") chosen to fall inside the confirmed
    # overlap of *all* tough stations' variant coverage as of this writing:
    #   B95338: 00 [2018-04-10, 2026-06-16], q [2016-10-18, 2026-08-17]
    #   B95410: base [2008-01-01, 2026-07-22], 00 [2014-04-30, 2026-08-18], q [2016-10-07, 2026-08-10]
    #   B95380: base [1988-10-01, 2026-07-22], q [2016-10-07, 2026-08-10]
    # A window near "now" would miss this since several base/00 variants have
    # already stopped reporting relative to the system clock in this sandbox.
    stime = pd.Timestamp("2020-06-01")
    etime = pd.Timestamp("2020-06-03")

    stations = pd.DataFrame(
        [
            {
                "agency_id": agency_id,
                "station_id": f"sta_{agency_id.lower()}",
                "src_var_id": "WaterTemp",
                "param": "temp",
            }
            for agency_id in TOUGH_AGENCY_IDS
        ]
    )

    failures = download_ncro.ncro_download(
        stations, str(tmp_path), start=stime, end=etime, overwrite=True,
    )
    assert failures == []

    for agency_id in TOUGH_AGENCY_IDS:
        station_id = f"sta_{agency_id.lower()}"
        matches = glob.glob(os.path.join(str(tmp_path), f"ncro_{station_id}_*_temp_*.csv"))
        assert len(matches) == 1, (
            f"Expected exactly one temp file for {station_id} (root cause of the "
            f"phantom-duplicate bug), got: {matches}"
        )

        candidate_sites = (
            inventory.loc[
                inventory["site"].isin(download_ncro.similar_ncro_station_names(agency_id))
                & (inventory["start_time"] <= etime)
                & (inventory["end_time"] >= stime),
                "site",
            ]
            .unique()
            .tolist()
        )
        expected_site = download_ncro._select_preferred_site("temp", candidate_sites)
        assert expected_site.lower() in os.path.basename(matches[0])
