import pandas as pd
from dms_datastore import inventory


def test_repo_file_inventory_groups_by_file_pattern(monkeypatch):
    repo_cfg = {"name": "formatted", "root": "/dummy"}

    parsed = [
        {
            "filename": "usgs_anh@north_11303500_flow_2023.csv",
            "agency": "usgs",
            "station_id": "anh",
            "subloc": "north",
            "agency_id": "11303500",
            "param": "flow",
            "year": "2023",
        },
        {
            "filename": "usgs_anh@north_11303500_flow_2024.csv",
            "agency": "usgs",
            "station_id": "anh",
            "subloc": "north",
            "agency_id": "11303500",
            "param": "flow",
            "year": "2024",
        },
    ]

    registry = pd.DataFrame(
        [{"station_id": "anh", "name": "Antioch", "x": 1.0, "y": 2.0}]
    ).set_index("station_id")

    monkeypatch.setattr(inventory, "coerce_repo_config", lambda repo=None, repo_cfg=None: repo_cfg or repo)
    monkeypatch.setattr(inventory, "repo_registry", lambda repo=None, repo_cfg=None: registry)
    monkeypatch.setattr(inventory, "_inventory_files", lambda root: ["a", "b"])
    monkeypatch.setattr(
      inventory,
      "_parse_inventory_meta",
      lambda allfiles, repo_cfg=None: parsed,
    )
    monkeypatch.setattr(inventory, "scrape_header_metadata", lambda fname: "cfs")

    out = inventory.repo_file_inventory(repo_cfg=repo_cfg)

    assert list(out.index) == ["usgs_anh@north_11303500_flow_*.csv"]
    row = out.iloc[0]
    assert row["series_id"] == "usgs|anh|north|flow"
    assert row["source"] == "usgs"
    assert row["min_year"] == "2023"
    assert row["max_year"] == "2024"
    assert row["unit"] == "cfs"
    assert row["name"] == "Antioch"



def test_repo_data_inventory_groups_by_series_id(monkeypatch):
    repo_cfg = {"name": "formatted", "root": "/dummy"}

    parsed = [
        {
            "filename": "usgs_anh@north_11303500_flow_2023.csv",
            "agency": "usgs",
            "source": "usgs",
            "station_id": "anh",
            "subloc": "north",
            "agency_id": "11303500",
            "param": "flow",
            "year": "2023",
        },
        {
            "filename": "cdec_anh@north_B12345_flow_2024.csv",
            "agency": "cdec",
            "source": "cdec",
            "station_id": "anh",
            "subloc": "north",
            "agency_id": "B12345",
            "param": "flow",
            "year": "2024",
        },
    ]
    registry = pd.DataFrame(
        [{"station_id": "anh", "name": "Antioch"}]
    ).set_index("station_id",drop=False)

    monkeypatch.setattr(inventory, "coerce_repo_config", lambda repo=None, repo_cfg=None: repo_cfg or repo)
    monkeypatch.setattr(inventory, "repo_registry", lambda repo=None, repo_cfg=None: registry)
    monkeypatch.setattr(inventory, "_inventory_files", lambda root: ["a", "b"])
    monkeypatch.setattr(
      inventory,
      "_parse_inventory_meta",
      lambda allfiles, repo_cfg=None: parsed,
    )

    monkeypatch.setattr(inventory, "scrape_header_metadata", lambda fname: "cfs")

    out = inventory.repo_data_inventory(repo_cfg=repo_cfg)


    assert list(out.index) == ["anh|north|flow"]
    row = out.iloc[0]

    assert row["file_pattern"] == "*_anh@north_11303500_flow_*.csv" or row["file_pattern"] == "*_anh@north_B12345_flow_*.csv"
    assert row["station_id"] == "anh"
    assert row["param"] == "flow"
    assert row["min_year"] == "2023"
    assert row["max_year"] == "2024"