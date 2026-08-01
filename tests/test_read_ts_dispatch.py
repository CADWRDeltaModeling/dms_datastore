from pathlib import Path

import importlib

import pandas as pd
import pytest

# NOTE: ``import dms_datastore.read_ts as rt`` would bind ``rt`` to the
# re-exported ``read_ts`` *function* (the package __init__ does
# ``from dms_datastore.read_ts import *``, which shadows the submodule
# attribute). Use importlib to obtain the module object itself.
rt = importlib.import_module("dms_datastore.read_ts")


def test_read_usgs1_rejects_caller_dtypes(tmp_path: Path) -> None:
    fpath = tmp_path / "usgs_like.csv"
    fpath.write_text(
        "USGS\n"
        "tz_cd\n"
        "datetime,value\n"
        "2018-01-01 00:00:00,1\n",
        encoding="utf-8",
        newline="\n",
    )

    with pytest.raises(IOError, match="caller-supplied dtypes"):
        rt.read_usgs1(fpath, dtypes={"value": float})


def test_read_ts_hint_resort_skips_usgs_reader(tmp_path: Path, monkeypatch) -> None:
    fpath = tmp_path / "smscg_log.csv"
    fpath.write_text(
        "datetime,value\n"
        "2018-01-01 00:00:00,1\n"
        "2018-01-02 00:00:00,2\n",
        encoding="utf-8",
        newline="\n",
    )

    def bomb(*args, **kwargs):
        raise AssertionError("read_usgs1 should not be called when hint='resort'")

    monkeypatch.setattr(rt, "read_usgs1", bomb)

    ts = rt.read_ts(str(fpath), force_regular=False, hint="resort")

    assert isinstance(ts, pd.DataFrame)
    assert ts.index.name == "datetime"
    assert list(ts.columns) == ["value"]
    assert len(ts) == 2