import pandas as pd
import pytest

from dms_datastore import dstore_config
from dms_datastore.populate_repo import (
    _apply_source_availability,
    _load_source_availability,
)


@pytest.mark.integration
def test_usgs_usbr_transitions_have_cdec_availability_bounds():
    """All approved USGS-to-USBR stations suppress pre-handoff CDEC requests."""
    transitions = pd.read_csv(
        dstore_config.config_file("usgs_usbr_transition_list"), comment="#", dtype=str
    ).fillna("")
    registry = dstore_config.repo_registry("formatted")
    availability = _load_source_availability(
        dstore_config.config_file("source_availability")
    )

    excluded_cdec_ids = {"CM49A", "MIT/MIB"}
    transition_cdec_ids = set(
        transitions.loc[
            ~transitions["CDEC Station ID"].str.upper().isin(excluded_cdec_ids),
            "CDEC Station ID",
        ].str.upper()
    )
    transitioned = registry.loc[
        registry["agency"].str.lower().eq("usbr")
        & registry["cdec_id"].str.upper().isin(transition_cdec_ids),
        ["station_id", "cdec_id"],
    ]

    assert len(transitioned) == len(transition_cdec_ids)
    policy = availability.loc[
        availability["source"].eq("cdec") & availability["variable"].eq(""),
        ["station_id", "available_from"],
    ].set_index("station_id")["available_from"]
    transitioned_ids = set(transitioned["station_id"].str.lower())

    assert transitioned_ids.issubset(set(policy.index))
    assert (policy.loc[sorted(transitioned_ids)] == pd.Timestamp("2025-01-01")).all()

    requests = pd.DataFrame(
        {"station_id": sorted(transitioned_ids), "param": "flow"}
    )
    groups = _apply_source_availability(
        requests,
        "cdec",
        pd.Timestamp("2000-01-01"),
        pd.Timestamp("2019-12-31"),
        availability,
    )

    assert groups == []
