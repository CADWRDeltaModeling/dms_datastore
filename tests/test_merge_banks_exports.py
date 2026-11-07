import logging

import pandas as pd
import pytest

from dms_datastore.processed import merge_banks_exports as merge_mod
from dms_datastore.processed.merge_banks_exports import (
    TZ_COMMENT,
    _read_text_csv,
    merge_banks_exports,
)

ARCHIVE_HDR = "Time,Field Div.,Plant,Unit,Status,Date,AVR,PSS,MWs,CFS,Notes,Exclude\n"
EXPORT_HDR = "If ,Field Div.,Plant,Unit,Status,Date,AVR,PSS,MWs,CFS,Notes,Exclude,Cause Code,kV\n"


def _arc(time, unit, status, date, cfs="0000000000", notes="", excl=""):
    return f'{time},WRDT,WRBA,UNIT {unit:02d},"{status}",{date},O/S,,,{cfs},{notes},{excl}\n'


def _exp(time, unit, status, date, notes=""):
    return f'{time},WRDT,WRBA,UNIT {unit:02d},"{status}",{date},O/S,,,,{notes},,0,0.00\n'


@pytest.fixture
def files(tmp_path):
    arc = tmp_path / "archive.csv"
    arc.write_text(
        ARCHIVE_HDR
        + _arc("0001", 1, "S/D,L/M", "6/1/2026")
        + _arc("0001", 9, "I/S,R/A", "6/1/2026", excl="X")
        + _arc("0001", 9, "T/IS", "6/1/2026", notes="sir#8500 ok")
        + _arc("0900", 2, "FOP", "6/1/2026")
        + _arc("0930", 5, "FOP", "6/1/2026")
        + _arc("0930", 5, "S/D,R/A", "6/1/2026", excl="X"),
        encoding="utf-8",
    )
    exp = tmp_path / "EXPORT_1.csv"
    exp.write_text(
        EXPORT_HDR
        + _exp("0900", 2, "FOP", "6/1/2026", notes="different note")  # already in archive
        + _exp("801", 11, "I/S,R/A", "6/1/2026")  # new, Excel stripped zero
        + _exp("0000", 3, "S/D,R/A", "6/1/2026")  # new, sorts before archive rows
        + _exp("0000", 3, "SO/S", "6/1/2026"),  # new, same key as previous: both kept
        encoding="utf-8",
    )
    return arc, exp


def test_merge_appends_new_sorts_and_archive_wins(files, tmp_path):
    arc, exp = files
    out = tmp_path / "out.csv"
    merge_banks_exports(arc, [exp], outfile=out)

    assert out.read_text(encoding="utf-8").splitlines()[0] == TZ_COMMENT
    df = _read_text_csv(out)
    keys = list(zip(df["Time"], df["Unit"]))
    assert keys == [
        ("0000", "UNIT 03"),
        ("0000", "UNIT 03"),
        ("0001", "UNIT 01"),
        ("0001", "UNIT 09"),
        ("0001", "UNIT 09"),
        ("0801", "UNIT 11"),
        ("0900", "UNIT 02"),
        ("0930", "UNIT 05"),
        ("0930", "UNIT 05"),
    ]
    # ties keep archive order, including an X row that follows a non-X row
    assert list(df.loc[df["Unit"] == "UNIT 05", "Exclude"]) == ["", "X"]
    assert list(df.loc[df["Unit"] == "UNIT 05", "Status"]) == ["FOP", "S/D,R/A"]
    # same-key rows keep their original relative order
    assert list(df.loc[df["Unit"] == "UNIT 09", "Status"]) == ["I/S,R/A", "T/IS"]
    assert list(df.loc[df["Unit"] == "UNIT 03", "Status"]) == ["S/D,R/A", "SO/S"]
    # archive row wins over the export row with a different note; Exclude preserved
    assert df.loc[df["Unit"] == "UNIT 02", "Notes"].item() == ""
    assert list(df.loc[df["Unit"] == "UNIT 09", "Exclude"]) == ["X", ""]
    assert "sir#8500 ok" in set(df["Notes"])
    assert (df["CFS"].loc[df["Unit"] == "UNIT 01"] == "0000000000").all()


def test_merge_is_idempotent(files, tmp_path):
    arc, exp = files
    out = tmp_path / "out.csv"
    merge_banks_exports(arc, [exp], outfile=out)
    first = out.read_bytes()
    merge_banks_exports(out, [exp], outfile=out)
    assert out.read_bytes() == first


def test_dry_run_writes_nothing(files, tmp_path):
    arc, exp = files
    before = arc.read_bytes()
    merge_banks_exports(arc, [exp], dry_run=True)
    assert arc.read_bytes() == before


def test_short_export_warns(files, tmp_path):
    arc, exp = files  # the fixture export has only 4 rows
    messages = []

    class _Capture(logging.Handler):
        def emit(self, record):
            messages.append(record.getMessage())

    # configure_logging in other tests turns off propagation, so caplog is not reliable here
    handler = _Capture(level=logging.WARNING)
    merge_mod.logger.addHandler(handler)
    try:
        merge_banks_exports(arc, [exp], outfile=tmp_path / "o.csv")
    finally:
        merge_mod.logger.removeHandler(handler)
    assert any("may be incomplete" in m for m in messages)


def test_bad_date_raises(files, tmp_path):
    arc, _ = files
    bad = tmp_path / "bad.csv"
    bad.write_text(EXPORT_HDR + _exp("0000", 1, "FOP", "not-a-date"), encoding="utf-8")
    with pytest.raises(ValueError):
        merge_banks_exports(arc, [bad], outfile=tmp_path / "o.csv")
