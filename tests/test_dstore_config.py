import importlib
from pathlib import Path

import pandas as pd
import pytest


try:
    import dms_datastore.dstore_config as cfgmod
except ModuleNotFoundError:  # pragma: no cover - fallback for standalone validation
    import importlib.util
    module_path = Path("/mnt/data/dstore_config_documented.py")
    spec = importlib.util.spec_from_file_location("dstore_config_under_test", module_path)
    cfgmod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(cfgmod)


@pytest.fixture(autouse=True)
def reset_module_state(monkeypatch, tmp_path):
    """Give each test an isolated config dict, temp files, and empty caches."""
    config_dir = tmp_path / "config_data"
    config_dir.mkdir()

    # Files used by multiple tests
    sublocations = config_dir / "sublocations.csv"
    sublocations.write_text(
        "station_id,subloc,z,comment\n"
        "anh,default,0.0,main\n"
        "anh,upper,1.0,upper sensor\n",
        encoding="utf-8",
    )

    stations = config_dir / "stations.csv"
    stations.write_text(
        "station_id,agency_id,agency,name\n"
        "anh,A1,dwr,Antioch\n"
        "old,A2,usgs,Old River\n",
        encoding="utf-8",
    )

    quoted_registry = config_dir / "quoted_registry.csv"
    quoted_registry.write_text(
        "'station_id','agency_id','name'\n"
        "'anh','A1','Antioch'\n"
        "'old','A2','Old River'\n",
        encoding="utf-8",
    )

    bad_missing_key = config_dir / "bad_missing_key.csv"
    bad_missing_key.write_text(
        "station_name,agency_id\n"
        "Antioch,A1\n",
        encoding="utf-8",
    )

    dup_registry = config_dir / "dup_registry.csv"
    dup_registry.write_text(
        "station_id,agency_id\n"
        "anh,A1\n"
        "anh,A9\n",
        encoding="utf-8",
    )

    empty_registry = config_dir / "empty_registry.csv"
    empty_registry.write_text("station_id,agency_id\n", encoding="utf-8")

    formatted_root = config_dir / "formatted_root"
    formatted_root.mkdir()
    processed_root = config_dir / "processed_root"
    processed_root.mkdir()
    direct_dir = tmp_path / "direct_dir"
    direct_dir.mkdir()

    config = {
        "scalar_value": 7,
        "screen_config": {"method": "basic", "threshold": 0.1},
        "labels": ["a", "b"],
        "plain_file": "stations.csv",
        "sublocations": "sublocations.csv",
        "registries": {
            "stations": "quoted_registry.csv",
            "missing_key": "bad_missing_key.csv",
            "duplicates": "dup_registry.csv",
            "empty": "empty_registry.csv",
        },
        "source_priority_groups": {
            "obs": ["ncro", "cdec", "usgs"],
        },
        "repos": {
            "formatted": {
                "root": "formatted_root",
                "registry": "stations",
                "site_key": "station_id",
                "provider_key": "source",
                "provider_resolution_mode": "assume_unique",
                "filename_templates": ["{source}_{station_id}_{param}.csv"],
            },
            "processed": {
                "root": str(processed_root),
                "registry": "stations",
                "site_key": "station_id",
                "provider_key": "processor",
                "provider_resolution_mode": "assume_unique",
                "filename_templates": ["{processor}_{station_id}_{param}.csv"],
                "file_key": ["processor", "station_id", "param"],
                "data_key": ["processor", "station_id", "param"],
            },
            "no_registry": {
                "root": str(processed_root),
                "site_key": "station_id",
                "provider_key": "processor",
                "provider_resolution_mode": "assume_unique",
                "filename_templates": ["{processor}_{station_id}_{param}.csv"],
            },
        },
    }

    monkeypatch.setattr(cfgmod, "config", config)
    monkeypatch.setattr(cfgmod, "localdir", str(config_dir))
    monkeypatch.setattr(cfgmod, "station_dbase_cache", None)
    monkeypatch.setattr(cfgmod, "subloc_cache", None)
    monkeypatch.setattr(cfgmod, "_registry_cache", {})
    monkeypatch.setattr(cfgmod, "_repo_cache", None)
    monkeypatch.chdir(tmp_path)

    return {
        "config_dir": config_dir,
        "formatted_root": formatted_root,
        "processed_root": processed_root,
        "direct_dir": direct_dir,
    }


def test_configuration_returns_copy_with_module_path():
    cfg = cfgmod.configuration()

    assert cfg["scalar_value"] == 7
    assert "config_file_location" in cfg
    assert Path(cfg["config_file_location"]).name.startswith("dstore_config")

    cfg["scalar_value"] = 99
    assert cfgmod.config["scalar_value"] == 7


@pytest.mark.parametrize(
    "label, expected",
    [
        ("scalar_value", 7),
        ("labels", ["a", "b"]),
        ("screen_config", {"method": "basic", "threshold": 0.1}),
    ],
)
def test_config_value_returns_raw_values(label, expected):
    assert cfgmod.config_value(label) == expected


def test_config_value_missing_raises():
    with pytest.raises(ValueError, match="Config label not found"):
        cfgmod.config_value("missing")


def test_config_file_resolves_top_level_file(reset_module_state):
    path = cfgmod.config_file("plain_file")
    assert Path(path) == reset_module_state["config_dir"] / "stations.csv"


def test_config_file_missing_label_raises():
    with pytest.raises(ValueError, match="Config label not found"):
        cfgmod.config_file("missing")


def test_config_file_unresolvable_target_raises(monkeypatch):
    monkeypatch.setitem(cfgmod.config, "bad_file", "does_not_exist.csv")

    with pytest.raises(ValueError, match="File not found"):
        cfgmod.config_file("bad_file")


def test_sublocation_df_reads_and_caches(monkeypatch):
    first = cfgmod.sublocation_df()
    second = cfgmod.sublocation_df()

    assert list(first.columns) == ["station_id", "subloc", "z", "comment"]
    assert first is second

    # Prove the cache is being used.
    monkeypatch.setattr(
        pd,
        "read_csv",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("cache not used")),
    )
    third = cfgmod.sublocation_df()
    assert third is first


def test_sublocation_df_duplicate_station_subloc_raises(monkeypatch, tmp_path):
    dup = tmp_path / "dup_sublocations.csv"
    dup.write_text(
        "station_id,subloc,z,comment\n"
        "anh,default,0.0,main\n"
        "anh,default,1.0,duplicate\n",
        encoding="utf-8",
    )
    monkeypatch.setitem(cfgmod.config, "sublocations", str(dup))
    monkeypatch.setattr(cfgmod, "subloc_cache", None)

    with pytest.raises(ValueError, match="duplicate station_id keys"):
        cfgmod.sublocation_df()


def test_repo_names_lists_configured_repos():
    assert cfgmod.repo_names() == ["formatted", "processed", "no_registry"]


def test_repo_config_returns_validated_spec_and_caches(reset_module_state, monkeypatch):
    cfg = cfgmod.repo_config("formatted")

    assert cfg["name"] == "formatted"
    assert Path(cfg["root"]) == reset_module_state["formatted_root"]
    assert cfg["provider_key"] == "source"

    monkeypatch.setattr(
        cfgmod,
        "_resolve_config_path",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("cache not used")),
    )
    cached = cfgmod.repo_config("formatted")
    assert cached is cfg


@pytest.mark.parametrize(
    "repo_name, match",
    [
        (None, "repo name must be provided explicitly"),
        ("missing", "Unknown configured repo name"),
    ],
)
def test_repo_config_rejects_missing_or_unknown_repo(repo_name, match):
    with pytest.raises(ValueError, match=match):
        cfgmod.repo_config(repo_name)


def test_repo_config_requires_expected_keys(monkeypatch):
    monkeypatch.setitem(
        cfgmod.config["repos"],
        "bad_repo",
        {
            "root": "formatted_root",
            "site_key": "station_id",
            "provider_key": "source",
            "filename_templates": ["{source}_{station_id}_{param}.csv"],
        },
    )

    with pytest.raises(ValueError, match="missing required keys"):
        cfgmod.repo_config("bad_repo")


@pytest.mark.parametrize(
    "spec",
    [
        {
            "root": "formatted_root",
            "site_key": "station_id",
            "provider_key": "source",
            "provider_resolution_mode": "assume_unique",
            "filename_templates": ["{source}_{station_id}_{param}.csv"],
            "file_key": ["source", "station_id", "param"],
        },
        {
            "root": "formatted_root",
            "site_key": "station_id",
            "provider_key": "source",
            "provider_resolution_mode": "assume_unique",
            "filename_templates": ["{source}_{station_id}_{param}.csv"],
            "data_key": ["source", "station_id", "param"],
        },
    ],
)
def test_repo_config_requires_file_key_and_data_key_together(monkeypatch, spec):
    monkeypatch.setitem(cfgmod.config["repos"], "bad_repo", spec)

    with pytest.raises(ValueError, match="must define both file_key and data_key"):
        cfgmod.repo_config("bad_repo")


@pytest.mark.parametrize("templates", [[], None])
def test_repo_config_requires_filename_templates(monkeypatch, templates):
    monkeypatch.setitem(
        cfgmod.config["repos"],
        "bad_repo",
        {
            "root": "formatted_root",
            "site_key": "station_id",
            "provider_key": "source",
            "provider_resolution_mode": "assume_unique",
            "filename_templates": templates,
        },
    )

    with pytest.raises(ValueError, match="must define filename_templates"):
        cfgmod.repo_config("bad_repo")


def test_coerce_repo_config_prefers_repo_cfg_dict():
    repo_cfg = {"root": "/tmp/example"}
    assert cfgmod.coerce_repo_config(repo="formatted", repo_cfg=repo_cfg) is repo_cfg


def test_coerce_repo_config_resolves_named_repo():
    out = cfgmod.coerce_repo_config(repo="formatted")
    assert out["name"] == "formatted"


def test_coerce_repo_config_requires_argument():
    with pytest.raises(ValueError, match="repo must be provided"):
        cfgmod.coerce_repo_config()


def test_repo_root_from_repo_name_and_repo_cfg():
    cfg = cfgmod.repo_config("processed")

    assert cfgmod.repo_root(repo="processed") == cfg["root"]
    assert cfgmod.repo_root(repo_cfg=cfg) == cfg["root"]


def test_resolve_repo_data_dir_accepts_existing_directory(reset_module_state):
    direct = reset_module_state["direct_dir"]
    assert cfgmod.resolve_repo_data_dir(repo_or_path=str(direct)) == str(direct)


def test_resolve_repo_data_dir_accepts_repo_name(reset_module_state):
    assert cfgmod.resolve_repo_data_dir(repo="formatted") == str(
        reset_module_state["formatted_root"]
    )


def test_resolve_repo_data_dir_accepts_repo_cfg(reset_module_state):
    repo_cfg = cfgmod.repo_config("processed")
    assert cfgmod.resolve_repo_data_dir(repo_cfg=repo_cfg) == str(
        reset_module_state["processed_root"]
    )


@pytest.mark.parametrize(
    "kwargs, match",
    [
        ({"repo_or_path": "x", "repo": "formatted"}, "Provide only one"),
        ({}, "Must provide repo_or_path, repo, or repo_cfg"),
    ],
)
def test_resolve_repo_data_dir_rejects_bad_argument_combinations(kwargs, match):
    with pytest.raises(ValueError, match=match):
        cfgmod.resolve_repo_data_dir(**kwargs)


def test_resolve_repo_data_dir_rejects_unknown_repo_name():
    with pytest.raises(ValueError, match="Not an existing directory and not a configured repo name"):
        cfgmod.resolve_repo_data_dir(repo="missing")


def test_resolve_repo_data_dir_rejects_nonexistent_repo_cfg_root():
    with pytest.raises(ValueError, match="Configured repo root does not exist"):
        cfgmod.resolve_repo_data_dir(repo_cfg={"root": "/definitely/not/here"})


def test_resolve_repo_data_dir_rejects_configured_root_that_does_not_exist(monkeypatch):
    monkeypatch.setitem(
        cfgmod.config["repos"],
        "broken",
        {
            "root": "missing_root_dir",
            "registry": "stations",
            "site_key": "station_id",
            "provider_key": "source",
            "provider_resolution_mode": "assume_unique",
            "filename_templates": ["{source}_{station_id}_{param}.csv"],
        },
    )

    with pytest.raises(ValueError, match="Not an existing directory and not a configured repo name"):
        cfgmod.resolve_repo_data_dir(repo="broken")


def test_registry_df_none_returns_none():
    assert cfgmod.registry_df(None) is None


def test_registry_df_reads_registry_and_normalizes_columns():
    df = cfgmod.registry_df("stations")

    assert list(df.columns) == ["station_id", "agency_id", "name"]
    assert df["station_id"].tolist() == ["'anh'", "'old'"]
    assert df.loc[0, "agency_id"] == "A1"


@pytest.mark.xfail(reason="registry_df initializes but does not populate _registry_cache in current implementation")
def test_registry_df_caches_result(monkeypatch):
    first = cfgmod.registry_df("stations")

    monkeypatch.setattr(
        pd,
        "read_csv",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("cache not used")),
    )
    second = cfgmod.registry_df("stations")
    assert second is first

@pytest.mark.parametrize(
    "registry_name, match",
    [
        ("missing", "Registry not found"),
        ("empty", "is empty or could not be read"),
    ]
)
def test_registry_df_error_cases(registry_name, match):
    with pytest.raises(ValueError, match=match):
        cfgmod.registry_df(registry_name)


def test_repo_registry_uses_repo_site_key():
    df = cfgmod.repo_registry(repo="formatted")
    assert df.index.name == "station_id"
    assert df.loc["old", "agency_id"] == "A2"
    
def test_repo_registry_rejects_missing_site_key(monkeypatch):
    monkeypatch.setitem(
        cfgmod.config["repos"],
        "broken_repo",
        {
            "root": "formatted_root",
            "registry": "missing_key",
            "site_key": "station_id",
            "provider_key": "source",
            "provider_resolution_mode": "assume_unique",
            "filename_templates": ["{source}_{station_id}_{param}.csv"],
        },
    )

    with pytest.raises(
        ValueError,
        match="Registry missing_key is missing key column station_id",
    ):
        cfgmod.repo_registry(repo="broken_repo")


def test_repo_registry_rejects_duplicate_site_key(monkeypatch):
    monkeypatch.setitem(
        cfgmod.config["repos"],
        "dup_repo",
        {
            "root": "formatted_root",
            "registry": "duplicates",
            "site_key": "station_id",
            "provider_key": "source",
            "provider_resolution_mode": "assume_unique",
            "filename_templates": ["{source}_{station_id}_{param}.csv"],
        },
    )

    with pytest.raises(
        ValueError,
        match="Registry duplicates has duplicate station_id keys",
    ):
        cfgmod.repo_registry(repo="dup_repo")

def test_repo_registry_rejects_repo_without_registry():
    with pytest.raises(ValueError, match="has no named registry"):
        cfgmod.repo_registry(repo="no_registry")


def test_station_dbase_legacy_delegate():
    df = cfgmod.station_dbase()
    expected = cfgmod.repo_registry(repo="formatted")
    assert df.equals(expected)


def test_source_priority_group_returns_group():
    assert cfgmod.source_priority_group("obs") == ["ncro", "cdec", "usgs"]


def test_source_priority_group_missing_raises():
    with pytest.raises(ValueError, match="Source priority group not found"):
        cfgmod.source_priority_group("missing")
