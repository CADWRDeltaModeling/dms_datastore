import importlib

import pandas as pd
import pytest

rm = importlib.import_module("dms_datastore.read_multi")


def _df(start, periods, freq="15min"):
    idx = pd.date_range(start=start, periods=periods, freq=freq)
    return pd.DataFrame({"value": range(periods)}, index=idx)


def test_ts_multifile_single_shard_bypasses_merge(monkeypatch):
    calls = {"read_ts": 0}

    def fake_glob(pattern):
        return ["single.csv"]

    def fake_detect(_fname):
        return None, None

    def fake_header(_fname):
        return {}

    def fake_interpret(_base, repo=None):
        return {"year": 2024}

    def fake_filter(_meta, _start, _end):
        return False

    def fake_read_ts(_path, force_regular=True, dtypes=None):
        calls["read_ts"] += 1
        return _df("2024-01-01", 3)

    def bomb_merge(*args, **kwargs):
        raise AssertionError("ts_merge should not be called for a single shard")

    monkeypatch.setattr(rm.glob, "glob", fake_glob)
    monkeypatch.setattr(rm, "detect_dms_unit", fake_detect)
    monkeypatch.setattr(rm, "read_yaml_header", fake_header)
    monkeypatch.setattr(rm, "interpret_fname", fake_interpret)
    monkeypatch.setattr(rm, "filter_date", fake_filter)
    monkeypatch.setattr(rm, "read_ts", fake_read_ts)
    monkeypatch.setattr(rm, "ts_merge", bomb_merge)

    out = rm.ts_multifile("unused_pattern", force_regular=False)

    assert isinstance(out, pd.DataFrame)
    assert len(out) == 3
    assert calls["read_ts"] == 1


def test_ts_multifile_raises_for_overlapping_shards(monkeypatch):
    files = ["a.csv", "b.csv"]

    def fake_glob(pattern):
        return files

    def fake_detect(_fname):
        return None, None

    def fake_header(_fname):
        return {}

    def fake_interpret(base, repo=None):
        return {"year": 2024 if base == "a.csv" else 2025}

    def fake_filter(_meta, _start, _end):
        return False

    def fake_read_ts(path, force_regular=True, dtypes=None):
        if path == "a.csv":
            return _df("2024-01-01 00:00", 5)
        return _df("2024-01-01 00:45", 5)

    monkeypatch.setattr(rm.glob, "glob", fake_glob)
    monkeypatch.setattr(rm, "detect_dms_unit", fake_detect)
    monkeypatch.setattr(rm, "read_yaml_header", fake_header)
    monkeypatch.setattr(rm, "interpret_fname", fake_interpret)
    monkeypatch.setattr(rm, "filter_date", fake_filter)
    monkeypatch.setattr(rm, "read_ts", fake_read_ts)

    with pytest.raises(ValueError, match="Overlapping shard windows"):
        rm.ts_multifile("unused_pattern", force_regular=False)
