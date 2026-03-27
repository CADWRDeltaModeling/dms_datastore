import yaml
import os
import pandas as pd

__all__ = [
    "station_dbase",
    "configuration",
    "get_config",
    "config_file",
    "coerce_repo_config",
    "repo_config",
    "repo_root",
    "repo_names",
    "registry_df",
    "repo_registry",
    "source_priority_group",
]

config = None
localdir = os.path.join(os.path.split(__file__)[0], "config_data")

with open(os.path.join(localdir, "dstore_config.yaml"), "r") as stream:
    config = yaml.load(stream, Loader=yaml.FullLoader)

station_dbase_cache = None
subloc_cache = None
_registry_cache = {}
_repo_cache = None


def _resolve_config_path(fname_or_path):
    """
    Resolve a config value that may be either:
      - a filename living in config_data
      - a relative path
      - an absolute path
    """
    if fname_or_path is None:
        raise ValueError("Config path cannot be None")

    if os.path.isabs(fname_or_path) and os.path.exists(fname_or_path):
        return fname_or_path

    if os.path.exists(fname_or_path):
        return fname_or_path

    localpath = os.path.join(localdir, fname_or_path)
    if os.path.exists(localpath):
        return localpath

    assume_fname = os.path.join("config_data", fname_or_path)
    if os.path.exists(assume_fname):
        return assume_fname

    raise ValueError(
        f"File not found: {fname_or_path} either directly, in cwd, or in {localdir}"
    )


    
def coerce_repo_config(repo=None, repo_cfg=None):
    if repo_cfg is not None:
        return repo_cfg
    if repo is None:
        raise ValueError("repo must be provided")
    return repo_config(repo)


def repo_root(repo=None, repo_cfg=None):
    cfg = coerce_repo_config(repo=repo, repo_cfg=repo_cfg)
    return cfg["root"]


def repo_registry(repo=None, repo_cfg=None):
    cfg = coerce_repo_config(repo=repo, repo_cfg=repo_cfg)
    print(cfg)
    registry_name = cfg.get("registry")
    key_column = cfg.get("key_column", "id")
    if registry_name is None:
        raise ValueError(f"Repo {cfg['name']!r} has no named registry")
    return registry_df(registry_name, key_column=key_column)   
    
def station_dbase():
    """legacy. will disappear"""
    
    return repo_registry("formatted")

    
def blah():
    global station_dbase_cache
    if station_dbase_cache is None:
        if dbase_name is None:
            dbase_name = config_file("station_dbase")
        db = pd.read_csv(
            dbase_name,
            sep=",",
            comment="#",
            header=0,
            index_col="id",
            dtype={"agency_id": str},
        )
        db["agency_id"] = db["agency_id"].str.replace("'", "", regex=True)

        dup = db.index.duplicated()
        db.index = db.index.str.replace("'", "")
        if dup.sum(axis=0) > 0:
            print("Duplicates")
            print(db[dup])
            raise ValueError("Station database has duplicate id keys. See above")
        station_dbase_cache = db
    return station_dbase_cache


def sublocation_df(dbase_name=None):
    global subloc_cache
    if subloc_cache is None:
        subloc_name = config_file("sublocations")
        db = pd.read_csv(
            subloc_name,
            sep=",",
            comment="#",
            header=0,
            dtype={"station_id": str, "subloc": str, "z": float, "comment": str},
        )
        dup = db.duplicated(subset=["station_id", "subloc"], keep="first")
        if dup.sum(axis=0) > 0:
            print("Duplicates in subloc table")
            print(db[dup])
            raise ValueError("Station database has duplicate station_id keys. See above")
        subloc_cache = db
    return subloc_cache


def configuration():
    config_ret = config.copy()
    config_ret["config_file_location"] = __file__
    return config_ret


def get_config(label):
    return config_file(label)


def config_file(label):
    """
    Legacy config lookup for top-level labels.
    """
    if label not in config:
        raise ValueError(f"Config label not found: {label}")
    fname = config[label]
    return _resolve_config_path(fname)


def repo_names():
    return list(config.get("repos", {}).keys())

def repo_config(repo_name):
    global _repo_cache

    if repo_name is None:
        raise ValueError("repo name must be provided explicitly")

    repos = config.get("repos", {})
    if repo_name not in repos:
        raise ValueError(f"Unknown configured repo name: {repo_name}")

    if _repo_cache is None:
        _repo_cache = {}

    if repo_name in _repo_cache:
        return _repo_cache[repo_name]

    spec = dict(repos[repo_name])
    spec["name"] = repo_name
    spec["root"] = (
        _resolve_config_path(spec["root"])
        if not os.path.exists(spec["root"])
        else spec["root"]
    )
    spec.setdefault("key_column", "id")
    spec.setdefault("source_priority_mode", "none")
    spec.setdefault("parse", {"style": "legacy"})
    spec.setdefault("search", {"use_source_slot": True, "shard_style": "auto"})

    templates = spec.get("filename_templates", [])
    if not templates:
        raise ValueError(
            f"Configured repo {repo_name!r} must define filename_templates"
        )

    _repo_cache[repo_name] = spec
    return spec


def registry_df(registry_name, key_column="id"):
    """
    Load a named registry declared in config['registries'].
    """
    global _registry_cache

    if registry_name is None:
        return None

    if registry_name in _registry_cache:
        return _registry_cache[registry_name]

    registries = config.get("registries", {})
    if registry_name not in registries:
        raise ValueError(f"Registry not found: {registry_name}")

    reg_path = _resolve_config_path(registries[registry_name])
    if not os.path.exists(reg_path):
        raise ValueError(f"Registry file not found: {reg_path}")


    db = pd.read_csv(
      reg_path,
      sep=",",
      comment="#",
      header=0,
      dtype={"agency_id": str},
    )
    if db is None or db.empty:
        raise ValueError(f"Registry {registry_name} is empty or could not be read")


    db.columns = db.columns.str.replace("'", "", regex=True).str.strip()

    if key_column not in db.columns:
        raise ValueError(
            f"Registry {registry_name} is missing key column {key_column}"
        )

    db[key_column] = db[key_column].astype(str).str.replace("'", "", regex=True).str.strip()
    print(db)

    if "agency_id" in db.columns:
        db["agency_id"] = db["agency_id"].astype(str).str.replace("'", "", regex=True).str.strip()

    dup = db[key_column].duplicated()
    if dup.any():
        print("Duplicates")
        print(db.loc[dup, [key_column]])
        raise ValueError(f"Registry {registry_name} has duplicate {key_column} keys")

    db = db.set_index(key_column, drop=False)
    return db


def source_priority_group(name):
    groups = config.get("source_priority_groups", {})
    if name not in groups:
        raise ValueError(f"Source priority group not found: {name}")
    return groups[name]