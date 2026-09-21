import pandas as pd

from dms_datastore.prune_cdec_ncro_shadows import plan_cdec_shadow_deletions


def _registry():
    return pd.DataFrame(
        {
            "station_id": ["wci", "other"],
            "agency": ["dwr_ncro", "dwr"],
        }
    )


def test_plan_cdec_shadow_deletions_requires_matching_ncro_and_registry(tmp_path):
    cdec_candidate = tmp_path / "cdec_wci_b95338q_velocity_2024.csv"
    ncro_match = tmp_path / "ncro_wci_b95338q_velocity_2024.csv"
    cdec_not_ncro = tmp_path / "cdec_other_abc_velocity_2024.csv"
    ncro_not_ncro = tmp_path / "ncro_other_abc_velocity_2024.csv"
    cdec_without_match = tmp_path / "cdec_wci_b95338q_flow_2024.csv"
    for path in [
        cdec_candidate,
        ncro_match,
        cdec_not_ncro,
        ncro_not_ncro,
        cdec_without_match,
    ]:
        path.write_text("placeholder")

    candidates, ambiguous, unparseable = plan_cdec_shadow_deletions(
        tmp_path, _registry()
    )

    assert candidates == [(str(cdec_candidate), str(ncro_match))]
    assert ambiguous == [
        (str(cdec_not_ncro), str(ncro_not_ncro), "registry agency is dwr")
    ]
    assert unparseable == []


def test_plan_cdec_shadow_deletions_uses_base_station_with_sublocation(tmp_path):
    cdec_candidate = tmp_path / "cdec_wci@upper_b95338q_velocity_2024.csv"
    ncro_match = tmp_path / "ncro_wci@upper_b95338q_velocity_2024.csv"
    cdec_candidate.write_text("placeholder")
    ncro_match.write_text("placeholder")

    candidates, ambiguous, unparseable = plan_cdec_shadow_deletions(
        tmp_path, _registry()
    )

    assert candidates == [(str(cdec_candidate), str(ncro_match))]
    assert ambiguous == []
    assert unparseable == []