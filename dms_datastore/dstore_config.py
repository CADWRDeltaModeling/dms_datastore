import yaml
import os
import pandas as pd

__all__ = [
    "station_dbase",
    "configuration",
    "get_config",
    "config_file",
    "repo_config",
    "repo_root",
    "repo_names",
    "registry_df",
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


def station_dbase(dbase_name=None):
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
            dtype={"id": str, "subloc": str, "z": float, "comment": str},
        )
        dup = db.duplicated(subset=["id", "subloc"], keep="first")
        if dup.sum(axis=0) > 0:
            print("Duplicates in subloc table")
            print(db[dup])
            raise ValueError("Station database has duplicate id keys. See above")
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


def repo_config(repo_name=None):
    """
    Return normalized repo configuration.

    If repo_name is None, fall back to legacy top-level 'repo'.
    If repo_name is an existing path, return an ad hoc repo config.
    """
    global _repo_cache

    if repo_name is None:
        repo_name = "repo"

    if os.path.exists(str(repo_name)):
        return {
            "name": str(repo_name),
            "root": str(repo_name),
            "registry": None,
            "key_column": "id",
            "source_priority_mode": "none",
            "filename_templates": [],
            "parse": {"style": "legacy"},
        }

    repos = config.get("repos", {})

    if repo_name in repos:
        if _repo_cache is None:
            _repo_cache = {}
        if repo_name in _repo_cache:
            return _repo_cache[repo_name]

        spec = dict(repos[repo_name])
        spec["name"] = repo_name
        spec["root"] = _resolve_config_path(spec["root"]) if not os.path.exists(spec["root"]) else spec["root"]
        spec.setdefault("key_column", "id")
        spec.setdefault("filename_templates", [])
        spec.setdefault("source_priority_mode", "none")
        spec.setdefault("parse", {"style": "legacy"})
        _repo_cache[repo_name] = spec
        return spec

    # compatibility with old top-level entries like screened / processed / repo
    if repo_name in config:
        root = config_file(repo_name)
        return {
            "name": repo_name,
            "root": root,
            "registry": "continuous",
            "key_column": "id",
            "source_priority_mode": "by_registry_column",
            "source_priority_column": "agency",
            "filename_templates": [],
            "parse": {"style": "legacy"},
        }

    raise ValueError(f"Unknown repo name: {repo_name}")


def repo_root(repo_name=None):
    return repo_config(repo_name)["root"]


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
    df = pd.read_csv(reg_path, sep=",", comment="#", header=0, dtype=str)
    if key_column not in df.columns:
        raise ValueError(
            f"Registry {registry_name} is missing key column {key_column}"
        )
    df[key_column] = df[key_column].str.replace("'", "", regex=True)
    dup = df[key_column].duplicated()
    if dup.any():
        raise ValueError(
            f"Registry {registry_name} has duplicate {key_column} values"
        )
    df = df.set_index(key_column, drop=False)
    _registry_cache[registry_name] = df
    return df


def source_priority_group(name):
    groups = config.get("source_priority_groups", {})
    if name not in groups:
        raise ValueError(f"Source priority group not found: {name}")
    return groups[name]