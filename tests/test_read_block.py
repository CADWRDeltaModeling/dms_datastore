import pandas as pd
import pytest
from click.testing import CliRunner

from dms_datastore import dstore_config
from dms_datastore.read_block import (
    DIM_ALIASES,
    parse_dim,
    read_block_cli,
    read_request_csv,
    read_ts_block,
    ts_block,
)


def _series(start, periods, base, freq="15min"):
    idx = pd.date_range(start=start, periods=periods, freq=freq)
    return pd.Series([base + i for i in range(periods)], index=idx, name="value")


@pytest.fixture
def sample_series():
    return [
        ("sjw", "ec", _series("2026-02-05", 2, 1000.0)),
        ("sjw", "temp", _series("2026-02-05", 2, 12.0)),
        ("emm2", "ec", _series("2026-02-05", 2, 3000.0)),
        ("emm2", "temp", _series("2026-02-05", 2, 14.0)),
    ]


# ---------------------------------------------------------------------------
# dim parsing
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "dim,expected",
    [
        ("tidy", (["datetime", "station"], [["param"]])),
        ("[[t,s],[p]]", (["datetime", "station"], [["param"]])),
        ("long", (["datetime", "station", "param"], [])),
        ("sd_tidy", (["station", "datetime"], [["param"]])),
        ("flat", (["datetime"], [["station", "param"]])),
        ("wide", (["datetime"], [["station"]])),
        ("[[t], [s_p]]", (["datetime"], [["station", "param"]])),
    ],
)
def test_parse_dim(dim, expected):
    assert parse_dim(dim) == expected


def test_all_aliases_parse():
    for alias in DIM_ALIASES:
        parse_dim(alias)


@pytest.mark.parametrize(
    "dim",
    ["[[t,s]]", "[[t,x],[p]]", "[[t,s],[s]]", "[[],[p]]", "nonsense"],
)
def test_parse_dim_rejects_bad_spec(dim):
    with pytest.raises(ValueError):
        parse_dim(dim)


def test_parse_dim_rejects_composite_index():
    with pytest.raises(ValueError, match="Composite token"):
        parse_dim("[[t_s],[p]]")


# ---------------------------------------------------------------------------
# block reshaping
# ---------------------------------------------------------------------------


def test_ts_block_tidy(sample_series):
    block = ts_block(sample_series, dim="tidy")
    assert list(block.index.names) == ["datetime", "station"]
    assert list(block.columns) == ["ec", "temp"]
    assert block.loc[(pd.Timestamp("2026-02-05"), "sjw"), "ec"] == 1000.0
    assert block.loc[(pd.Timestamp("2026-02-05"), "emm2"), "temp"] == 14.0


def test_ts_block_long(sample_series):
    block = ts_block(sample_series, dim="long")
    assert list(block.index.names) == ["datetime", "station", "param"]
    assert list(block.columns) == ["value"]
    assert len(block) == 8


def test_ts_block_sd_tidy_preserves_request_order(sample_series):
    block = ts_block(sample_series, dim="sd_tidy")
    assert list(block.index.names) == ["station", "datetime"]
    assert list(block.index.get_level_values("station").unique()) == ["sjw", "emm2"]


def test_ts_block_flat_column_labels(sample_series):
    block = ts_block(sample_series, dim="flat")
    assert list(block.index.names) == ["datetime"]
    assert list(block.columns) == ["sjw_ec", "sjw_temp", "emm2_ec", "emm2_temp"]


def test_ts_block_wide_single_param():
    series = [
        ("sjw", "ec", _series("2026-02-05", 2, 1000.0)),
        ("emm2", "ec", _series("2026-02-05", 2, 1200.0)),
    ]
    block = ts_block(series, dim="wide")
    assert list(block.columns) == ["sjw", "emm2"]
    assert block.iloc[0].tolist() == [1000.0, 1200.0]


def test_ts_block_rejects_omitted_multivalued_dim(sample_series):
    with pytest.raises(ValueError, match="omits the 'param' dimension"):
        ts_block(sample_series, dim="wide")


def test_ts_block_aligns_mismatched_timestamps():
    series = [
        ("sjw", "ec", _series("2026-02-05 00:00", 2, 1000.0, freq="15min")),
        ("emm2", "ec", _series("2026-02-05 00:00", 2, 1200.0, freq="h")),
    ]
    block = ts_block(series, dim="wide")
    assert list(block.index) == [
        pd.Timestamp("2026-02-05 00:00"),
        pd.Timestamp("2026-02-05 00:15"),
        pd.Timestamp("2026-02-05 01:00"),
    ]
    assert pd.isna(block.loc[pd.Timestamp("2026-02-05 00:15"), "emm2"])


def test_ts_block_keeps_all_nan_series():
    """A window that is entirely flagged/missing must still appear as a column."""
    series = [
        ("sjw", "ec", _series("2026-02-05", 2, 1000.0)),
        ("emm2", "ec", pd.Series(
            [float("nan")] * 2,
            index=pd.date_range("2026-02-05", periods=2, freq="15min"),
        )),
    ]
    block = ts_block(series, dim="wide")
    assert list(block.columns) == ["sjw", "emm2"]
    assert block["emm2"].isna().all()


def test_ts_block_requires_series():
    with pytest.raises(ValueError, match="No series"):
        ts_block([], dim="tidy")


# ---------------------------------------------------------------------------
# request file
# ---------------------------------------------------------------------------


def test_read_request_csv(tmp_path):
    path = tmp_path / "station_list.csv"
    path.write_text("station,param\nsjw,ec\nemm2,ec\nsjw,temp\nemm2,temp\n")
    assert read_request_csv(path) == [
        ("sjw", "ec"),
        ("emm2", "ec"),
        ("sjw", "temp"),
        ("emm2", "temp"),
    ]


def test_read_request_csv_subloc_column(tmp_path):
    path = tmp_path / "station_list.csv"
    path.write_text("station,param,subloc\nanh,ec,bottom\nanh,ec,\n")
    assert read_request_csv(path) == [("anh@bottom", "ec"), ("anh", "ec")]


def test_read_request_csv_missing_column(tmp_path):
    path = tmp_path / "station_list.csv"
    path.write_text("station\nsjw\n")
    with pytest.raises(ValueError, match="missing required column"):
        read_request_csv(path)


# ---------------------------------------------------------------------------
# repo search order
# ---------------------------------------------------------------------------


def test_repo_search_order_default():
    assert dstore_config.repo_search_order() == ["processed", "screened"]


def test_repo_search_order_explicit():
    assert dstore_config.repo_search_order("screened") == ["screened"]
    assert dstore_config.repo_search_order(["screened", "processed"]) == [
        "screened",
        "processed",
    ]


def test_repo_search_order_rejects_unknown():
    with pytest.raises(ValueError, match="unconfigured repos"):
        dstore_config.repo_search_order("no_such_repo")


# ---------------------------------------------------------------------------
# read_ts_block orchestration (repository reads stubbed)
# ---------------------------------------------------------------------------


@pytest.fixture
def stub_repo(monkeypatch):
    """Stub read_ts_repo with a controllable per-repo store of series."""
    store = {}
    calls = []

    def fake_read_ts_repo(station_id, variable, repo=None, start=None, end=None,
                          force_regular=True, freq_resolver=None,
                          provider_priority="infer"):
        calls.append((station_id, variable, repo, force_regular))
        values = store.get((repo, station_id, variable))
        return None if values is None else values.to_frame("value")

    monkeypatch.setattr(
        "dms_datastore.read_block.read_ts_repo", fake_read_ts_repo
    )
    return store, calls


def test_read_ts_block_cross_product(stub_repo):
    store, _ = stub_repo
    for station, base in (("sjw", 1000.0), ("emm2", 3000.0)):
        store[("screened", station, "ec")] = _series("2026-02-05", 2, base)
        store[("screened", station, "temp")] = _series("2026-02-05", 2, base / 100)

    block = read_ts_block(
        station=["sjw", "emm2"],
        param=["ec", "temp"],
        repo="screened",
        dim="tidy",
        start="2026-02-05",
    )
    assert list(block.columns) == ["ec", "temp"]
    assert list(block.index.get_level_values("station").unique()) == ["sjw", "emm2"]


def test_read_ts_block_prefers_earlier_repo(stub_repo):
    store, calls = stub_repo
    store[("processed", "sjw", "ec")] = _series("2026-02-05", 2, 1.0)
    store[("screened", "sjw", "ec")] = _series("2026-02-05", 2, 999.0)

    block = read_ts_block(station=["sjw"], param=["ec"], dim="wide")
    assert block["sjw"].iloc[0] == 1.0
    assert [c[2] for c in calls] == ["processed"]


def test_read_ts_block_falls_through_to_next_repo(stub_repo):
    store, calls = stub_repo
    store[("screened", "sjw", "ec")] = _series("2026-02-05", 2, 999.0)

    block = read_ts_block(station=["sjw"], param=["ec"], dim="wide")
    assert block["sjw"].iloc[0] == 999.0
    assert [c[2] for c in calls] == ["processed", "screened"]


def test_read_ts_block_force_regular_default_true(stub_repo):
    store, calls = stub_repo
    store[("screened", "sjw", "ec")] = _series("2026-02-05", 2, 1.0)

    read_ts_block(station=["sjw"], param=["ec"], repo="screened", dim="wide")
    assert calls[0][3] is True


def test_read_ts_block_omits_missing_pairs(stub_repo):
    store, _ = stub_repo
    store[("screened", "sjw", "ec")] = _series("2026-02-05", 2, 1000.0)
    store[("screened", "emm2", "ec")] = _series("2026-02-05", 2, 3000.0)

    block = read_ts_block(
        station=["sjw", "emm2", "vns"],
        param=["ec"],
        repo="screened",
        dim="wide",
    )
    assert list(block.columns) == ["sjw", "emm2"]


def test_read_ts_block_raises_when_nothing_found(stub_repo):
    with pytest.raises(ValueError, match="No data found"):
        read_ts_block(station=["sjw"], param=["ec"], repo="screened")


def test_read_ts_block_rejects_request_with_station(stub_repo, tmp_path):
    path = tmp_path / "station_list.csv"
    path.write_text("station,param\nsjw,ec\n")
    with pytest.raises(ValueError, match="mutually exclusive"):
        read_ts_block(station=["sjw"], param=["ec"], request=str(path))


def test_read_ts_block_requires_a_request(stub_repo):
    with pytest.raises(ValueError, match="Supply either request"):
        read_ts_block(station=["sjw"])


def test_read_ts_block_from_request_file(stub_repo, tmp_path):
    store, _ = stub_repo
    store[("screened", "sjw", "ec")] = _series("2026-02-05", 2, 1000.0)
    store[("screened", "emm2", "ec")] = _series("2026-02-05", 2, 1200.0)

    path = tmp_path / "station_list.csv"
    path.write_text("station,param\nsjw,ec\nemm2,ec\n")

    block = read_ts_block(request=str(path), repo="screened", dim="flat")
    assert list(block.columns) == ["sjw_ec", "emm2_ec"]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def test_cli_help_exits_cleanly():
    result = CliRunner().invoke(read_block_cli, ["-h"])
    assert result.exit_code == 0
    assert "--output" in result.output


def test_cli_requires_output():
    result = CliRunner().invoke(
        read_block_cli, ["--station", "sjw", "--param", "ec"]
    )
    assert result.exit_code != 0
    assert "--output" in result.output


def test_cli_writes_csv(stub_repo, tmp_path):
    store, _ = stub_repo
    store[("screened", "sjw", "ec")] = _series("2026-02-05", 2, 1000.0)
    store[("screened", "emm2", "ec")] = _series("2026-02-05", 2, 1200.0)

    out = tmp_path / "block.csv"
    result = CliRunner().invoke(
        read_block_cli,
        [
            "--station", "sjw",
            "--station", "emm2",
            "--param", "ec",
            "--repo", "screened",
            "--dim", "flat",
            "--start", "2026-02-05",
            "--end", "2026-02-06",
            "--output", str(out),
        ],
    )
    assert result.exit_code == 0, result.output
    text = out.read_text()
    assert text.splitlines()[0] == "datetime,sjw_ec,emm2_ec"


# ---------------------------------------------------------------------------
# Integration against the real repository
# ---------------------------------------------------------------------------

# emm2 has complete 15-min ec and temp coverage in this window; many other
# Delta stations currently trip the repo-wide overlapping-shard defect.
INTEG_STATION = "emm2"
INTEG_START = "2024-06-01"
INTEG_END = "2024-06-08"


@pytest.mark.integration
def test_read_ts_block_wide_against_repo():
    block = read_ts_block(
        station=[INTEG_STATION],
        param=["ec"],
        dim="wide",
        start=INTEG_START,
        end=INTEG_END,
    )
    assert list(block.columns) == [INTEG_STATION]
    assert block[INTEG_STATION].notna().any()


@pytest.mark.integration
def test_read_ts_block_tidy_against_repo():
    block = read_ts_block(
        station=[INTEG_STATION],
        param=["ec", "temp"],
        dim="tidy",
        start=INTEG_START,
        end=INTEG_END,
    )
    assert list(block.index.names) == ["datetime", "station"]
    assert list(block.columns) == ["ec", "temp"]
    assert block.notna().any().all()


@pytest.mark.integration
def test_read_ts_block_long_against_repo():
    block = read_ts_block(
        station=[INTEG_STATION],
        param=["ec", "temp"],
        dim="long",
        start=INTEG_START,
        end=INTEG_END,
    )
    assert list(block.index.names) == ["datetime", "station", "param"]
    assert list(block.columns) == ["value"]


@pytest.mark.integration
def test_read_block_cli_against_repo(tmp_path):
    out = tmp_path / "block.csv"
    result = CliRunner().invoke(
        read_block_cli,
        [
            "--station", INTEG_STATION,
            "--param", "ec",
            "--param", "temp",
            "--dim", "flat",
            "--start", INTEG_START,
            "--end", INTEG_END,
            "--output", str(out),
        ],
    )
    assert result.exit_code == 0, result.output
    assert out.read_text().splitlines()[0] == "datetime,emm2_ec,emm2_temp"
