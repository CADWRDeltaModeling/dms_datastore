from dms_datastore import dstore_config
def test_repo_config_from_named_repo(monkeypatch, tmp_path):
    root = tmp_path / "processed"
    root.mkdir()

    monkeypatch.setattr(
        dstore_config,
        "config",
        {
            "repos": {
                "processed": {
                    "root": str(root),
                    "registry": "processed",
                    "provider_key": "processor",
                    "provider_resolution_mode": "assume_unique",
                    "filename_templates": [
                        "{processor}_{station_id@subloc}_{param@modifier}.csv",
                        "{processor}_{station_id@subloc}_{param@modifier}_{year}.csv",
                        "{processor}_{station_id@subloc}_{param}_{year}.csv",
                        "{processor}_{station_id@subloc}_{param}.csv",
                    ],
                    "file_key": ["processor", "station_id", "subloc", "param", "modifier", "shard"],
                    "data_key": ["station_id", "subloc", "param", "modifier", "shard"],
                }
            }
        },
    )
    monkeypatch.setattr(dstore_config, "_repo_cache", None)

    cfg = dstore_config.repo_config("processed")

    assert cfg["name"] == "processed"
    assert cfg["root"] == str(root)
    assert cfg["provider_key"] == "processor"
    assert cfg["provider_resolution_mode"] == "assume_unique"
    assert cfg["filename_templates"] == [
        "{processor}_{station_id@subloc}_{param@modifier}.csv",
        "{processor}_{station_id@subloc}_{param@modifier}_{year}.csv",
        "{processor}_{station_id@subloc}_{param}_{year}.csv",
        "{processor}_{station_id@subloc}_{param}.csv",
    ]

