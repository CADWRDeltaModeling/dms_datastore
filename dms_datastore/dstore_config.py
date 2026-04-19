"""Configuration access utilities for :mod:`dms_datastore`.

This module provides two related public interfaces.

The first interface is for general configuration access. Use
:func:`configuration` to retrieve a copy of the loaded top-level configuration,
:func:`config_value` to retrieve a raw top-level value without additional
interpretation, and :func:`config_file` to resolve configured file resources.
This is the appropriate layer for shared resources such as variable mappings,
registries, and lookup tables declared in ``dstore_config.yaml``.

The second interface is for repository-aware access. Use :func:`repo_config`,
:func:`repo_root`, :func:`resolve_repo_data_dir`, :func:`repo_registry`, and
related helpers when working with configured repositories and their associated
identity, naming, and registry semantics.

Examples
--------
Retrieve a copy of the full loaded configuration and index into it directly::

    cfg = configuration()
    screen_cfg = cfg["screen_config"]

Retrieve a raw top-level value directly::

    screen_cfg = config_value("screen_config")

Resolve a configured file resource::

    variable_map = config_file("variable_mappings")

Resolve a repository configuration and its root directory::

    cfg = repo_config("formatted")
    root = repo_root(repo_cfg=cfg)

Notes
-----
``station_dbase()`` is retained only for legacy compatibility.
"""

import os

import pandas as pd
import yaml

__all__ = [
    "station_dbase",
    "configuration",
    "config_value",
    "config_file",
    "coerce_repo_config",
    "repo_config",
    "repo_root",
    "resolve_repo_data_dir",
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


# -----------------------------------------------------------------------------
# Configuration access
# -----------------------------------------------------------------------------


def _resolve_config_path(fname_or_path):
    """
    Resolve a configured path-like value to an existing filesystem path.

    Parameters
    ----------
    fname_or_path : str
        Configured filename or path.

    Returns
    -------
    str
        Existing filesystem path.

    Raises
    ------
    ValueError
        Raised if ``fname_or_path`` is ``None`` or cannot be resolved to an
        existing file.
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



def sublocation_df(dbase_name=None):
    """
    Load the configured sublocation table.

    Parameters
    ----------
    dbase_name : str, optional
        Unused legacy parameter.

    Returns
    -------
    pandas.DataFrame
        Sublocation table indexed by rows, with duplicate
        ``station_id``/``subloc`` combinations rejected.

    Raises
    ------
    ValueError
        Raised if duplicate ``station_id``/``subloc`` combinations are found.
    """
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
    """
    Return a copy of the loaded top-level configuration.

    Returns
    -------
    dict
        Copy of the loaded configuration with an added
        ``config_file_location`` entry identifying this module file.

    Notes
    -----
    This is the broadest public entry point for configuration inspection.
    Callers may index into the returned dictionary directly when they need raw
    top-level values without further interpretation.

    Examples
    --------
    ::

        cfg = configuration()
        repo_base = cfg["repo_base"]
    """
    config_ret = config.copy()
    config_ret["config_file_location"] = __file__
    return config_ret



def config_value(label):
    """
    Return a raw top-level configuration value.

    Parameters
    ----------
    label : str
        Top-level configuration key.

    Returns
    -------
    object
        Raw value stored under ``label`` in the loaded configuration.

    Raises
    ------
    ValueError
        Raised if ``label`` is not present in the configuration.

    Notes
    -----
    This function applies no file resolution, repo validation, or other
    interpretation. Use :func:`config_file` for configured file resources and
    :func:`repo_config` for repository specifications.
    """
    if label not in config:
        raise ValueError(f"Config label not found: {label}")
    return config[label]



def config_file(label):
    """
    Resolve a configured file resource by top-level label.

    Parameters
    ----------
    label : str
        Top-level configuration key whose value names a file resource.

    Returns
    -------
    str
        Resolved filesystem path.

    Raises
    ------
    ValueError
        Raised if ``label`` is not present in the configuration or the
        configured value cannot be resolved to an existing path.
    """
    if label not in config:
        raise ValueError(f"Config label not found: {label}")
    fname = config[label]
    return _resolve_config_path(fname)


# -----------------------------------------------------------------------------
# Repository access
# -----------------------------------------------------------------------------


def coerce_repo_config(repo=None, repo_cfg=None):
    """
    Normalize repository inputs to a repository configuration dictionary.

    Parameters
    ----------
    repo : str, optional
        Configured repository name.
    repo_cfg : dict, optional
        Repository configuration dictionary.

    Returns
    -------
    dict
        Repository configuration dictionary.

    Raises
    ------
    ValueError
        Raised if neither ``repo`` nor ``repo_cfg`` is provided.
    """
    if repo_cfg is not None:
        return repo_cfg
    if repo is None:
        raise ValueError("repo must be provided")
    return repo_config(repo)



def repo_root(repo=None, repo_cfg=None):
    """
    Return the configured root directory for a repository.

    Parameters
    ----------
    repo : str, optional
        Configured repository name.
    repo_cfg : dict, optional
        Repository configuration dictionary.

    Returns
    -------
    str
        Configured repository root.
    """
    cfg = coerce_repo_config(repo=repo, repo_cfg=repo_cfg)
    return cfg["root"]



def resolve_repo_data_dir(repo_or_path=None, repo=None, repo_cfg=None):
    """
    Resolve the physical data directory for a repository context.

    Parameters
    ----------
    repo_or_path : str, optional
        Either an existing directory path or a configured repository name.
    repo : str, optional
        Configured repository name. Mutually exclusive with ``repo_or_path``.
    repo_cfg : dict, optional
        Repository configuration dictionary.

    Returns
    -------
    str
        Existing directory path for repository data.

    Raises
    ------
    ValueError
        Raised if the inputs are inconsistent, insufficient, or do not resolve
        to an existing directory.

    Notes
    -----
    This is intended for callers that want repository semantics from config but
    may override the physical data location during testing or debugging.
    """
    if repo_cfg is not None:
        root = repo_root(repo_cfg=repo_cfg)
        if not os.path.isdir(root):
            raise ValueError(f"Configured repo root does not exist: {root}")
        return root

    if repo_or_path is not None and repo is not None:
        raise ValueError("Provide only one of repo_or_path or repo")

    target = repo if repo is not None else repo_or_path
    if target is None:
        raise ValueError("Must provide repo_or_path, repo, or repo_cfg")

    # Direct directory override wins
    if os.path.isdir(target):
        return target

    # Otherwise interpret as configured repo name
    try:
        root = repo_root(repo=target)
    except Exception as e:
        raise ValueError(
            f"Not an existing directory and not a configured repo name: {target}"
        ) from e

    if not os.path.isdir(root):
        raise ValueError(
            f"Configured repo root for {target!r} does not exist: {root}"
        )

    return root

def repo_registry(repo=None, repo_cfg=None):
    """
    Load and validate the registry associated with a configured repository.

    This function applies repository semantics to a raw registry table:
    - resolves the repository configuration
    - identifies the registry file and its ``site_key``
    - validates that the ``site_key`` column exists
    - enforces uniqueness of the ``site_key``
    - sets the DataFrame index to the ``site_key``

    Parameters
    ----------
    repo : str, optional
        Configured repository name.
    repo_cfg : dict, optional
        Repository configuration dictionary. One of ``repo`` or
        ``repo_cfg`` must be provided.

    Returns
    -------
    pandas.DataFrame
        Registry table indexed by the repository's ``site_key``.
        The key column is preserved as a column as well.

    Raises
    ------
    ValueError
        If the repository is not configured, has no registry,
        the registry is missing the required ``site_key`` column,
        or the key contains duplicate values.

    Notes
    -----
    This is the **preferred entry point** for accessing registry data
    in application code. It ensures that the registry is interpreted
    consistently with repository configuration.

    In contrast, :func:`registry_df` returns a raw table without
    any guarantees about identity or uniqueness.
    """
    cfg = coerce_repo_config(repo=repo, repo_cfg=repo_cfg)
    registry_name = cfg.get("registry")
    site_key = cfg["site_key"]

    if registry_name is None:
        raise ValueError(f"Repo {cfg['name']!r} has no named registry")

    db = registry_df(registry_name).copy()

    if site_key not in db.columns:
        raise ValueError(
            f"Registry {registry_name} is missing key column {site_key}"
        )

    db[site_key] = (
        db[site_key].astype(str).str.replace("'", "", regex=True).str.strip()
    )

    dup = db[site_key].duplicated()
    if dup.any():
        raise ValueError(f"Registry {registry_name} has duplicate {site_key} keys")

    db = db.set_index(site_key, drop=False)
    db.index = db.index.astype(str)
    db.index.name = "site_id"
    return db


def station_dbase():
    """Return the legacy formatted-repo station database."""
    return repo_registry("formatted")



def repo_names():
    """
    Return the configured repository names.

    Returns
    -------
    list of str
        Names under the top-level ``repos`` section of the configuration.
    """
    return list(config.get("repos", {}).keys())



def repo_config(repo_name):
    """
    Return a validated repository configuration by name.

    Parameters
    ----------
    repo_name : str
        Configured repository name.

    Returns
    -------
    dict
        Repository configuration dictionary with validated required keys,
        resolved root, and added ``name`` entry.

    Raises
    ------
    ValueError
        Raised if ``repo_name`` is missing, unknown, or has an invalid
        configuration.
    """
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

    required = ["site_key", "provider_key", "provider_resolution_mode", "filename_templates"]
    missing = [k for k in required if k not in spec]
    if missing:
        raise ValueError(f"Repo {repo_name!r} missing required keys: {missing}")

    if ("file_key" in spec) != ("data_key" in spec):
        raise ValueError(
            f"Repo {repo_name!r} must define both file_key and data_key, or neither"
        )

    spec["name"] = repo_name
    spec["root"] = (
        _resolve_config_path(spec["root"])
        if not os.path.exists(spec["root"])
        else spec["root"]
    )

    templates = spec.get("filename_templates", [])
    if not templates:
        raise ValueError(
            f"Configured repo {repo_name!r} must define filename_templates"
        )

    _repo_cache[repo_name] = spec
    return spec

def registry_df(registry_name):
    """
    Load a raw registry table by name.

    This function resolves a registry declared in the top-level
    ``registries`` section of the configuration and reads it into
    a :class:`pandas.DataFrame`.

    It performs only low-level loading and light normalization:
    - resolve the configured file path
    - read the CSV
    - strip quotes and surrounding whitespace from column names
    - normalize selected known columns such as ``agency_id``

    It does **not** apply repository semantics. In particular, it does not:
    - determine which column is the site key
    - validate uniqueness of any identifier column
    - set an index
    - enforce repository-specific identity rules

    Parameters
    ----------
    registry_name : str
        Name of a registry under the top-level ``registries`` section
        of the configuration.

    Returns
    -------
    pandas.DataFrame or None
        Raw registry table. Returns ``None`` if ``registry_name`` is None.

    Raises
    ------
    ValueError
        If the registry is not declared in configuration, cannot be
        resolved to a file, or is empty.

    Notes
    -----
    This is a low-level loader. Application code should usually prefer
    :func:`repo_registry`, which interprets the raw table using the
    configured repository ``site_key``.

    The loaded table is cached by ``registry_name``.
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

    if "agency_id" in db.columns:
        db["agency_id"] = (
            db["agency_id"].astype(str).str.replace("'", "", regex=True).str.strip()
        )

    if "agency_id" in db.columns:
        db["agency_id"] = (
            db["agency_id"].astype(str).str.replace("'", "", regex=True).str.strip()
        )

        missing_agency = db["agency_id"].isna() | (db["agency_id"] == "") | (db["agency_id"].str.lower() == "nan")
        if missing_agency.any():
            bad_rows = db.loc[missing_agency]
            raise ValueError(
                f"Registry {registry_name} has missing agency_id values in {len(bad_rows)} row(s)"
            )
    _registry_cache[registry_name] = db
    return db


def source_priority_group(name):
    """
    Return a named source-priority group from configuration.

    Parameters
    ----------
    name : str
        Source-priority group name.

    Returns
    -------
    object
        Configured source-priority group value.

    Raises
    ------
    ValueError
        Raised if ``name`` is not a configured source-priority group.
    """
    groups = config.get("source_priority_groups", {})
    if name not in groups:
        raise ValueError(f"Source priority group not found: {name}")
    return groups[name]
