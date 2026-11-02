"""Default partition for raw instrument files without an explicit rule."""

from pathlib import Path

import numpy as np
import pandas as pd

from dms_datastore.read_ts import read_ts
from dms_datastore.write_ts import write_ts_csv
from dms_datastore.rationalize_time_partitions import (
    _default_partition_paths,
    _filename_engulfed,
    _monotone_partition_plan,
)

META = "station_id: xyz\nparam: ph\nunit: pH\n"


def _write(path: Path, start, end, freq, nan_slice=None):
    idx = pd.date_range(start, end, freq=freq)
    df = pd.DataFrame({"value": np.linspace(7.0, 8.0, len(idx)).round(3)}, index=idx)
    if nan_slice is not None:
        df.loc[nan_slice, "value"] = np.nan
    df.index.name = "datetime"
    write_ts_csv(df, path, metadata=META, chunk_years=False)
    return df


def _names(d: Path):
    return sorted(p.name for p in d.glob("*.csv"))


def _meta(name, sy, ey):
    return {"filename": name, "syear": sy, "eyear": ey}


def test_filename_engulfed_uses_year_spans_and_keeps_one_of_identical():
    metas = [
        _meta("a_2004_2019.csv", 2004, 2019),
        _meta("b_2004_2007.csv", 2004, 2007),  # engulfed even if its data start earlier
        _meta("c_2003_2007.csv", 2003, 2007),  # not engulfed
        _meta("d_2020_2026.csv", 2020, 2026),
        _meta("e_2020_2026.csv", 2020, 2026),  # identical span: later name kept
    ]
    assert _filename_engulfed(metas) == {"b_2004_2007.csv", "d_2020_2026.csv"}


def test_monotone_plan_and_anomaly():
    t = pd.Timestamp
    spans = [
        (Path("a.csv"), t("2000-01-01"), t("2008-01-18")),
        (Path("b.csv"), t("2004-09-23"), t("2019-12-31")),
    ]
    include, anomalies, overlapping = _monotone_partition_plan(spans)
    assert [(p.name, s) for p, s in include] == [("a.csv", None), ("b.csv", t("2004-09-23"))]
    assert anomalies == [] and overlapping

    spans.append((Path("c.csv"), t("2007-10-01"), t("2009-01-01")))  # data inside b
    _, anomalies, _ = _monotone_partition_plan(spans)
    assert [p.name for p in anomalies] == ["c.csv"]


def test_overlap_sliced_at_first_valid_of_newer_file(tmp_path):
    old = tmp_path / "des_xyz_99_ph_2000_2005.csv"
    new = tmp_path / "des_xyz_99_ph_2004_2006.csv"
    _write(old, "2000-01-01", "2005-12-31 23:00", "h")
    # newer file has leading filler rows; takeover is at its first valid value
    new_df = _write(new, "2004-01-01", "2006-12-31 23:45", "15min",
                    nan_slice=slice("2004-01-01", "2004-03-31 23:45"))

    _default_partition_paths([old, new], dry_run=False)

    assert _names(tmp_path) == ["des_xyz_99_ph_2000_2004.csv", "des_xyz_99_ph_2004_2006.csv"]
    o = read_ts(str(tmp_path / "des_xyz_99_ph_2000_2004.csv"), force_regular=True)
    n = read_ts(str(new), force_regular=True)
    assert pd.Timestamp("2004-03-31 23:00") <= o.index.max() < pd.Timestamp("2004-04-01")
    assert n.dropna().index.min() == pd.Timestamp("2004-04-01")
    assert np.allclose(n["value"].dropna().to_numpy(), new_df["value"].dropna().to_numpy())


def test_filename_engulfed_file_deleted_even_if_data_protrude(tmp_path):
    outer = tmp_path / "des_xyz_99_ph_2000_2010.csv"
    inner = tmp_path / "des_xyz_99_ph_2003_2004.csv"
    _write(outer, "2000-06-01", "2010-12-31", "h")
    _write(inner, "2003-01-01", "2004-12-31", "15min")

    _default_partition_paths([outer, inner], dry_run=False)

    assert _names(tmp_path) == ["des_xyz_99_ph_2000_2010.csv"]


def test_handback_needed_leaves_survivors_untouched(tmp_path):
    a = tmp_path / "des_xyz_99_ph_2000_2005.csv"
    b = tmp_path / "des_xyz_99_ph_2003_2006.csv"  # name not engulfed, data inside a
    _write(a, "2000-01-01", "2005-12-31", "h")
    _write(b, "2003-01-01", "2004-12-31", "h")
    before = {p.name: p.stat().st_mtime_ns for p in tmp_path.glob("*.csv")}

    _default_partition_paths([a, b], dry_run=False)

    assert {p.name: p.stat().st_mtime_ns for p in tmp_path.glob("*.csv")} == before


def test_non_overlapping_untouched_and_dry_run_noop(tmp_path):
    a = tmp_path / "des_xyz_99_ph_2000_2001.csv"
    b = tmp_path / "des_xyz_99_ph_2002_2003.csv"
    _write(a, "2000-01-01", "2001-12-31", "h")
    _write(b, "2002-01-01", "2003-12-31", "h")
    before = {p.name: p.stat().st_mtime_ns for p in tmp_path.glob("*.csv")}
    _default_partition_paths([a, b], dry_run=False)
    assert {p.name: p.stat().st_mtime_ns for p in tmp_path.glob("*.csv")} == before

    c = tmp_path / "des_xyz_99_ph_2001_2004.csv"
    inner = tmp_path / "des_xyz_99_ph_2002_2002.csv"
    _write(c, "2001-06-01", "2004-12-31", "h")
    _write(inner, "2002-03-01", "2002-04-01", "h")
    before = _names(tmp_path)
    _default_partition_paths([a, b, c, inner], dry_run=True)
    assert _names(tmp_path) == before
