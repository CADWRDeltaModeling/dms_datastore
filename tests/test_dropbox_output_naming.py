import pytest

from dms_datastore.dropbox_data import _validated_output_filename


@pytest.fixture
def esmr_metadata():
    return {
        "source": "esmr",
        "station_id": "bwwtp",
        "agency_id": "bwwtp",
        "param": "ec",
    }


def test_formatted_output_rejects_unsharded_filename(esmr_metadata) -> None:
    with pytest.raises(ValueError, match="potw_ec: output naming is incompatible"):
        _validated_output_filename(
            esmr_metadata,
            "formatted",
            {"chunk_years": False},
            "potw_ec",
        )


def test_daily_formatted_output_accepts_unsharded_filename(esmr_metadata) -> None:
    result = _validated_output_filename(
        esmr_metadata,
        "daily_formatted",
        {"chunk_years": False},
        "potw_ec",
    )

    assert result == "esmr_bwwtp_bwwtp_ec.csv"
