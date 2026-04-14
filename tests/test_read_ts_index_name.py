import logging
from pathlib import Path

import pandas as pd
import pytest

from dms_datastore.read_ts import read_dms1, read_ts


DMS1_HEADER = """# format: dwr-dms-1.0
# date_formatted: '2026-04-14T09:15:32'
# param: elev
# agency: dwr_des
# source: des
# station_id: cse
# subloc: upper
# agency_id: '22'
# station_name: Sacramento River at Collinsville
# latitude: 38.07394957
# longitude: -121.8500958
# projection_x_coordinate: 600859.1
# projection_y_coordinate: 4214643.6
# projection_authority_id: epsg:26910
# crs_note: Reported lat-lon are agency provided. Projected coordinates may have been
#   revised based on additional information.
# agency_unit: ft
# unit: feet
# original_header: |-
#   # provider : DWR-DES
#   # station_id : cse
#   # agency_station_id : 22
#   # agency_result_id : 7
#   # agency_analyte_name : Stage
#   # agency_probe_depth : depth=0
#   # agency_unit_name : ft
#   # agency_equipment_name : Shaft Encoder
#   # agency aggregate_name : Inst
#   # agency_interval_name : 15 min
#   # agency_station_name : (C-2B)  Collinsville B
#   # source : https://dwrmsweb0263.ad.water.ca.gov/TelemetryDirect/api/Results/ReadingDates
#   # subloc : upper
#   # param : elev
#   # unit : feet
# metadata_time_precision_caveat: Transitions metadata (such as instruments, sublocations,
#   and agency-specific details) over the lifetime of a series may not be resolved in
#   time. Metadata from the last original raw file covering a year is used for that
#   year's shard.
#
"""

DATA_ROWS = """2001-10-01T08:30:00,2.47
2001-10-01T08:45:00,2.46
2001-10-01T09:00:00,2.48
2001-10-01T09:15:00,2.57
2001-10-01T09:30:00,2.70
2001-10-01T09:45:00,2.88
2001-10-01T10:00:00,3.09
2001-10-01T10:15:00,3.33
2001-10-01T10:30:00,3.58
2001-10-01T10:45:00,3.83
2001-10-01T11:00:00,4.07
2001-10-01T11:15:00,4.29
2001-10-01T11:30:00,4.49
"""


def _write_dms1_file(tmp_path: Path, *, include_index_name: bool) -> Path:
    body_header = "datetime,value\n" if include_index_name else ",value\n"
    text = DMS1_HEADER + body_header + DATA_ROWS
    fpath = tmp_path / (
        "dms1_with_index_name.csv" if include_index_name else "dms1_without_index_name.csv"
    )
    fpath.write_text(text, encoding="utf-8", newline="\n")
    return fpath


@pytest.mark.parametrize("include_index_name", [True, False])
def test_read_dms1_preserves_or_repairs_datetime_index_name(
    tmp_path, caplog, include_index_name
):
    fpath = _write_dms1_file(tmp_path, include_index_name=include_index_name)

    with caplog.at_level(logging.WARNING):
        ts = read_dms1(fpath, force_regular=False)

    assert isinstance(ts, pd.DataFrame)
    assert list(ts.columns) == ["value"]
    assert ts.index.name == "datetime"
    assert ts.index[0] == pd.Timestamp("2001-10-01T08:30:00")
    assert ts.iloc[0, 0] == pytest.approx(2.47)
    assert ts.iloc[-1, 0] == pytest.approx(4.49)
    assert len(ts) == 13

    if include_index_name:
        assert "no index name" not in caplog.text.lower()
    else:
        assert "no index name" in caplog.text.lower()
        assert "coercing to 'datetime'" in caplog.text.lower()


@pytest.mark.parametrize("include_index_name", [True, False])
def test_read_ts_dms1_preserves_or_repairs_datetime_index_name(
    tmp_path, caplog, include_index_name
):
    fpath = _write_dms1_file(tmp_path, include_index_name=include_index_name)

    with caplog.at_level(logging.WARNING):
        ts = read_ts(fpath, hint="dms1", force_regular=False)

    assert isinstance(ts, pd.DataFrame)
    assert list(ts.columns) == ["value"]
    assert ts.index.name == "datetime"
    assert ts.index[0] == pd.Timestamp("2001-10-01T08:30:00")
    assert ts.iloc[0, 0] == pytest.approx(2.47)
    assert ts.iloc[-1, 0] == pytest.approx(4.49)
    assert len(ts) == 13

    if include_index_name:
        assert "no index name" not in caplog.text.lower()
    else:
        assert "no index name" in caplog.text.lower()
        assert "coercing to 'datetime'" in caplog.text.lower()