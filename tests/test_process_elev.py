import pandas as pd
import pytest
from click.testing import CliRunner

from dms_datastore.processed import process_elev_data as pe


class _FakeModel:
    def __init__(self, intercept, slope):
        self.params = pd.Series([intercept, slope])


def _fake_result():
    idx = pd.date_range("2020-01-01", periods=48, freq="6min")
    ts = pd.DataFrame({"sf": 1.0, "ptr": 2.0, "mtr": 3.0}, index=idx)
    models = {
        "ptr_mtr": _FakeModel(0.1, 0.9),
        "mtr_ptr": _FakeModel(0.2, 0.8),
        "sf_ptr": _FakeModel(0.3, 0.7),
    }
    return ts, models


def test_write_elev_outputs_names_and_metadata(tmp_path):
    ts, models = _fake_result()
    outdir = tmp_path / "scratch"
    written = pe.write_elev_outputs(ts, models, outdir, "2004-03-01", "2022-10-01")

    assert sorted(p.name for p in written) == sorted(pe.OUTPUT_FILES.values())
    text = (outdir / "pryc1_elev.csv").read_text()
    assert "ols_predictor: mtyc1_subtidal" in text
    assert "ols_slope: '0.900000'" in text
    assert "datum: NAVD88" in text
    assert "datetime,value" in text
    assert "2004-03-01 to 2022-10-01" in text
    assert "ols_predictor: pryc1_subtidal" in (outdir / "sffpx_elev.csv").read_text()


def test_cli_help_lists_outdir():
    result = CliRunner().invoke(pe.process_elev_cli, ["-h"])
    assert result.exit_code == 0
    assert "--outdir" in result.output


def test_cli_writes_to_outdir_and_no_save(tmp_path, monkeypatch):
    monkeypatch.setattr(pe, "process_elev_data", lambda **kw: _fake_result())
    out = tmp_path / "out"
    args = ["--start", "2020-01-01", "--end", "2020-01-02", "--logdir", str(tmp_path / "logs")]

    result = CliRunner().invoke(pe.process_elev_cli, args + ["--outdir", str(out)])
    assert result.exit_code == 0, result.output
    assert {p.name for p in out.iterdir()} == set(pe.OUTPUT_FILES.values())

    out2 = tmp_path / "out2"
    result = CliRunner().invoke(pe.process_elev_cli, args + ["--outdir", str(out2), "--no-save"])
    assert result.exit_code == 0, result.output
    assert not out2.exists()


def test_reads_pin_screened_repo(monkeypatch):
    calls = []

    def fake_read(station, param, **kw):
        calls.append(kw.get("repo"))
        raise RuntimeError("stop")

    monkeypatch.setattr(pe, "read_ts_repo", fake_read)
    with pytest.raises(RuntimeError):
        pe.process_elev_data()
    assert calls == [pe.SOURCE_REPO]
