import numpy as np
import pandas as pd
import pytest
from click.testing import CliRunner

from dms_datastore.processed.process_banks import (
    DFD,
    SRR,
    parse_banks,
    process_banks,
    process_banks_cli,
    read_banks_log,
    regularize_pst,
)
from dms_datastore.read_ts import read_ts

HDR = "Time,Field Div.,Plant,Unit,Status,Date,AVR,PSS,MWs,CFS,Notes,Exclude\n"


def _row(time, unit, status, date, cfs="", excl=""):
    return f'{time},WRDT,WRBA,UNIT {unit:02d},"{status}",{date},O/S,,,{cfs},,{excl}\n'


def _write(path, rows, comment=True):
    path.write_text(("# Time zone: PDT\n" if comment else "") + HDR + "".join(rows), encoding="utf-8")
    return path


def test_flow_from_status_changes(tmp_path):
    f = _write(
        tmp_path / "log.csv",
        [
            _row("0001", 1, "S/D,L/M", "1/1/2020"),  # initial: not pumping, no change
            _row("0001", 3, "I/S,R/A", "1/1/2020"),  # initial: pumping counts
            _row("0600", 4, "I/S,R/A", "1/1/2020"),
            _row("1200", 3, "S/D,R/A", "1/1/2020"),
            _row("1300", 4, "T/IS", "1/1/2020"),  # test start is not a change
        ],
    )
    events, last = parse_banks(read_banks_log(f), DFD)
    assert list(events.values) == [1129.0, 2258.0, 1129.0]
    assert list(events.index.strftime("%H%M")) == ["0001", "0600", "1200"]
    assert last == pd.Timestamp("2020-01-01 13:00")


def test_srr_rating_differs(tmp_path):
    f = _write(tmp_path / "log.csv", [_row("0001", 4, "I/S,R/A", "1/1/2020")])
    srr, _ = parse_banks(read_banks_log(f), SRR)
    dfd, _ = parse_banks(read_banks_log(f), DFD)
    assert srr.iloc[0] == 1130.0 and dfd.iloc[0] == 1129.0


def test_excluded_rows_dropped_and_ties_keep_file_order(tmp_path):
    f = _write(
        tmp_path / "log.csv",
        [
            _row("0001", 3, "S/D,R/A", "1/1/2020"),
            _row("0600", 3, "I/S,R/A", "1/1/2020", excl="X"),  # excluded
            _row("0900", 3, "I/S,R/A", "1/1/2020"),
            _row("0900", 3, "S/D,R/A", "1/1/2020"),  # same minute: later row wins
        ],
    )
    events, _ = parse_banks(read_banks_log(f), DFD)
    # the unit goes on then off at 09:00; the net change at 09:00 is zero flow
    assert events.loc["2020-01-01 09:00"] == 0.0
    assert "2020-01-01 06:00" not in [str(t) for t in events.index]


def test_bad_exclude_and_unknown_op_raise(tmp_path):
    f = _write(tmp_path / "a.csv", [_row("0001", 3, "I/S,R/A", "1/1/2020", excl="Y")])
    with pytest.raises(ValueError, match="Exclude"):
        parse_banks(read_banks_log(f))
    f = _write(tmp_path / "b.csv", [_row("0001", 3, "ZZZ", "1/1/2020")])
    with pytest.raises(ValueError, match="Op unknown"):
        parse_banks(read_banks_log(f))


def test_reads_with_and_without_comment_line(tmp_path):
    rows = [_row("0001", 3, "I/S,R/A", "1/1/2020")]
    a = read_banks_log(_write(tmp_path / "a.csv", rows, comment=True))
    b = read_banks_log(_write(tmp_path / "b.csv", rows, comment=False))
    assert a["Datetime"].equals(b["Datetime"])


def test_regularize_pst_through_dst_changes():
    # Spring forward 2021-03-14 and fall back 2021-11-07 (local clock); 15-min grid
    idx = pd.to_datetime(
        ["2021-03-13 12:00", "2021-03-14 01:30", "2021-03-14 03:10", "2021-11-06 12:00", "2021-11-07 02:20"]
    )
    events = pd.Series([1000.0, 2000.0, 3000.0, 1000.0, 0.0], index=idx, name="value")
    out = regularize_pst(events, pd.Timestamp("2021-11-07 05:00"), 15)

    assert out.index.is_unique and out.index.is_monotonic_increasing
    assert (out.index.to_series().diff().dropna() == pd.Timedelta("15min")).all()
    assert not out.isna().any()
    # local 12:00 PST day before spring forward is the same wall time in PST
    assert out.loc["2021-03-13 12:00"] == 1000.0
    # 03:10 PDT on 3/14 is 02:10 PST; the 3000 change is in effect from the 02:15 PST grid point
    assert out.loc["2021-03-14 02:15"] == 3000.0
    assert out.loc["2021-03-14 01:45"] == 2000.0
    # 02:20 local on 11/7 is 02:20 PST (second pass); 02:30 PST is 0
    assert out.loc["2021-11-07 02:30"] == 0.0
    # repeated hour is filled, not missing
    assert out.loc["2021-11-07 01:00":"2021-11-07 01:45"].notna().all()


def test_process_banks_end_is_last_row_and_smoothing_off(tmp_path):
    f = _write(
        tmp_path / "log.csv",
        [
            _row("0001", 3, "I/S,R/A", "1/1/2020"),
            _row("1159", 3, "S/D,R/A", "1/1/2020"),
        ],
    )
    flow = process_banks(f, sigma=0)
    # converted to PST: 00:01 PST local is the same wall time in January
    assert flow.index[0] == pd.Timestamp("2020-01-01 00:15")
    assert flow.index[-1] == pd.Timestamp("2020-01-01 12:00")
    assert flow.iloc[0] == 1129.0 and flow.iloc[-1] == 0.0


def test_cli_writes_pst_header_and_compare(tmp_path):
    f = _write(
        tmp_path / "log.csv",
        [
            _row("0001", 3, "I/S,R/A", "1/1/2020"),
            _row("0001", 4, "I/S,R/A", "1/1/2020"),
            _row("0001", 5, "I/S,R/A", "1/2/2020"),
            _row("1200", 3, "S/D,R/A", "1/3/2020"),
        ],
    )
    hro = tmp_path / "hro.csv"
    days = pd.date_range("2020-01-01", periods=3, freq="D")
    hro.write_text(
        "datetime,value\n" + "".join(f"{d:%Y-%m-%d},{2000 + i}\n" for i, d in enumerate(days)),
        encoding="utf-8",
    )
    out = tmp_path / "dms_hro_flow.csv"
    res = CliRunner().invoke(
        process_banks_cli,
        ["--infile", str(f), "--outfile", str(out), "--sigma", "0", "--compare", str(hro),
         "--logdir", str(tmp_path / "logs"), "--quiet"],
    )
    assert res.exit_code == 0, res.output
    text = out.read_text(encoding="utf-8")
    assert "timezone: PST" in text.split("datetime")[0]
    flow = read_ts(str(out))
    assert len(flow) > 100
    comp = pd.read_csv(tmp_path / "dms_hro_flow_vs_hro_daily.csv", index_col=0)
    assert list(comp.columns) == ["processed", "hro", "diff"]
    assert not comp.empty
