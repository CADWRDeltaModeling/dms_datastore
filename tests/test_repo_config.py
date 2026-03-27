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
                    "filename_templates": [
                        "{source}_{key@subloc}_{param@modifier}_{year}.csv"
                    ],
                    "parse": {"style": "template"},
                    "source_priority_mode": "none",
                }
            }
        },
    )
    monkeypatch.setattr(dstore_config, "_repo_cache", None)

    cfg = dstore_config.repo_config("processed")

    assert cfg["name"] == "processed"
    assert cfg["root"] == str(root)
    assert cfg["parse"]["style"] == "template"
    assert cfg["filename_templates"] == [
        "{source}_{key@subloc}_{param@modifier}_{year}.csv"
    ]

