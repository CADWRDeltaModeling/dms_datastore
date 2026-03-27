#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
from pathlib import Path


_TEMPLATE_TOKEN_RE = re.compile(r"\{([^{}]+)\}")


def naming_spec(repo=None, repo_cfg=None, templates=None, style=None):
    """
    Construct a minimal naming spec for filename parsing/rendering.

    Parameters
    ----------
    repo : str, optional
        Repo name understood by dstore_config.repo_config().
    repo_cfg : dict, optional
        Full repo configuration.
    templates : str or list[str], optional
        One or more filename templates.
    style : str, optional
        Parse style, typically "legacy" or "template".

    Returns
    -------
    dict
        Minimal naming spec with keys:
          - filename_templates
          - parse: {"style": ...}
    """
    if repo_cfg is not None:
        return {
            "filename_templates": list(repo_cfg.get("filename_templates", [])),
            "parse": dict(repo_cfg.get("parse", {"style": "legacy"})),
        }

    if repo is not None:
        from dms_datastore import dstore_config
        rcfg = dstore_config.repo_config(repo)
        return {
            "filename_templates": list(rcfg.get("filename_templates", [])),
            "parse": dict(rcfg.get("parse", {"style": "legacy"})),
        }

    if templates is not None:
        if isinstance(templates, str):
            templates = [templates]
        return {
            "filename_templates": list(templates),
            "parse": {"style": style or "template"},
        }

    return {
        "filename_templates": [],
        "parse": {"style": style or "legacy"},
    }



def extract_year_fname(fname):
    re1 = re.compile(r".*_(\d{4})(?:\..{3})")
    yr = int(re1.match(fname).group(1))
    return yr


def _station_key_from_parts(key, subloc):
    if "@" in key:
        return key
    if subloc is not None and subloc != "default":
        return f"{key}@{subloc}"
    return key


def _param_from_parts(param, modifier):
    if modifier is None:
        return param
    return f"{param}@{modifier}"


def _render_repo_template(template, values):
    """
    Render a configured repo filename template using literal values.

    Supported placeholders
    ----------------------
    {source}
    {key}
    {key@subloc}
    {param}
    {param@modifier}
    {agency_id}
    {year}
    {syear}
    {eyear}

    Notes
    -----
    This is used both for exact filename generation and wildcard discovery.
    The caller decides whether values like year/source are literal strings or '*'.
    """
    out = template

    source = values.get("source")
    if source is None:
        raise ValueError("Missing required template value: source")
    out = out.replace("{source}", source)

    key = values.get("key")
    if key is None:
        raise ValueError("Missing required template value: key")
    subloc = values.get("subloc")

    if "{key@subloc}" in out:
        out = out.replace("{key@subloc}", _station_key_from_parts(key, subloc))
    out = out.replace("{key}", key)

    param = values.get("param")
    if param is None:
        raise ValueError("Missing required template value: param")
    modifier = values.get("modifier")

    if "{param@modifier}" in out:
        out = out.replace("{param@modifier}", _param_from_parts(param, modifier))
    out = out.replace("{param}", param)

    agency_id = values.get("agency_id")
    if agency_id is None:
        raise ValueError("Missing required template value: agency_id")
    out = out.replace("{agency_id}", agency_id)

    year = values.get("year")
    syear = values.get("syear")
    eyear = values.get("eyear")
    if year is None or syear is None or eyear is None:
        raise ValueError("Missing one of required year-like template values")

    out = out.replace("{year}", year)
    out = out.replace("{syear}", syear)
    out = out.replace("{eyear}", eyear)

    return out


def build_repo_globs(
    repo_cfg,
    *,
    key,
    param,
    subloc=None,
    modifier=None,
    sources=None,
    agency_id="*",
    year="*",
    syear="*",
    eyear="*",
):
    """
    Build repo-relative glob patterns from a repo configuration.

    This function does not infer priorities or defaults beyond simple wildcard
    defaults. It only converts structured query parts into repo-relative globs.
    """
    templates = repo_cfg.get("filename_templates", [])
    if not templates:
        raise ValueError(
            f"Repo {repo_cfg.get('name')!r} has no filename_templates"
        )

    if sources is None:
        sources = ["*"]
    elif isinstance(sources, str):
        sources = [sources]
    else:
        sources = list(sources)

    values = {
        "key": key,
        "subloc": subloc,
        "param": param,
        "modifier": modifier,
        "agency_id": agency_id,
        "year": year,
        "syear": syear,
        "eyear": eyear,
    }

    out = []
    seen = set()

    for src in sources:
        values["source"] = src
        for tmpl in templates:
            pat = _render_repo_template(tmpl, values)
            if pat not in seen:
                out.append(pat)
                seen.add(pat)

    return out


def _interpret_fname_legacy(fname):
    """
    Legacy parser for filenames like

        source_station@subloc_agencyid_param_2001.csv
        source_station@subloc_agencyid_param_2001_2024.csv
    """
    fname = os.path.split(fname)[1]

    datere = re.compile(
        r"([a-z0-9]+)_([a-z0-9@]+)_([a-z0-9]+)_([a-z0-9]+).*_(\d{4})_(\d{4})(?:\..{3})"
    )
    datere1 = re.compile(
        r"([a-z0-9]+)_([a-z0-9@]+)_([a-z0-9]+)_([a-z0-9]+).*_(\d{4})(?:\..{3})"
    )

    m = datere.match(fname)
    if m is None:
        m = datere1.match(fname)
        single_date = True
    else:
        single_date = False

    if m is None:
        raise ValueError(f"Legacy naming convention not matched for {fname}")

    meta = {}
    meta["filename"] = m.group(0)
    meta["agency"] = m.group(1)

    station_id = m.group(2)
    if "@" in station_id:
        station_id, subloc = station_id.split("@", 1)
    else:
        subloc = None

    meta["station_id"] = station_id
    meta["subloc"] = subloc
    meta["agency_id"] = m.group(3)
    meta["param"] = m.group(4)

    if single_date:
        meta["year"] = m.group(5)
    else:
        meta["syear"] = m.group(5)
        meta["eyear"] = m.group(6)

    return meta


def _template_regex_from_template(template):
    """
    Convert a repo template into a regex with named groups.

    This is intentionally narrow and only supports the current placeholder set.
    """
    pieces = ["^"]
    last = 0
    token_map = {
        "source": r"(?P<agency>[a-z0-9]+)",
        "key": r"(?P<station_id>[a-z0-9]+)",
        "key@subloc": r"(?P<station_full>[a-z0-9]+(?:@[a-z0-9]+)?)",
        "param": r"(?P<param>[a-z0-9]+)",
        "param@modifier": r"(?P<param_full>[a-z0-9]+(?:@[a-z0-9_]+)?)",
        "agency_id": r"(?P<agency_id>[a-z0-9]+)",
        "year": r"(?P<year>\d{4})",
        "syear": r"(?P<syear>\d{4})",
        "eyear": r"(?P<eyear>\d{4})",
    }

    for m in _TEMPLATE_TOKEN_RE.finditer(template):
        pieces.append(re.escape(template[last:m.start()]))
        token = m.group(1)
        if token not in token_map:
            raise ValueError(f"Unsupported template token {token!r}")
        pieces.append(token_map[token])
        last = m.end()

    pieces.append(re.escape(template[last:]))
    pieces.append("$")
    return re.compile("".join(pieces))


def _interpret_fname_template(fname, repo_cfg):
    fname = os.path.split(fname)[1]
    templates = repo_cfg.get("filename_templates", [])
    if not templates:
        raise ValueError(
            f"Repo {repo_cfg.get('name')!r} has no filename_templates for template parse"
        )

    for tmpl in templates:
        rx = _template_regex_from_template(tmpl)
        m = rx.match(fname)
        if m is None:
            continue

        gd = m.groupdict()
        meta = {"filename": fname}

        if "agency" in gd and gd["agency"] is not None:
            meta["agency"] = gd["agency"]

        if "station_full" in gd and gd["station_full"] is not None:
            station_full = gd["station_full"]
            if "@" in station_full:
                station_id, subloc = station_full.split("@", 1)
            else:
                station_id, subloc = station_full, None
            meta["station_id"] = station_id
            meta["subloc"] = subloc
        elif "station_id" in gd and gd["station_id"] is not None:
            meta["station_id"] = gd["station_id"]
            meta["subloc"] = None

        if "agency_id" in gd and gd["agency_id"] is not None:
            meta["agency_id"] = gd["agency_id"]

        if "param_full" in gd and gd["param_full"] is not None:
            param_full = gd["param_full"]
            if "@" in param_full:
                param, modifier = param_full.split("@", 1)
                meta["param"] = param
                meta["modifier"] = modifier
            else:
                meta["param"] = param_full
        elif "param" in gd and gd["param"] is not None:
            meta["param"] = gd["param"]

        if "year" in gd and gd["year"] is not None:
            meta["year"] = gd["year"]
        if "syear" in gd and gd["syear"] is not None:
            meta["syear"] = gd["syear"]
        if "eyear" in gd and gd["eyear"] is not None:
            meta["eyear"] = gd["eyear"]

        return meta

    raise ValueError(
        f"Template naming convention not matched for {fname} in repo {repo_cfg.get('name')!r}"
    )


def _coerce_naming_spec(repo=None, repo_cfg=None, naming=None):
    if naming is not None:
        return naming
    return naming_spec(repo=repo, repo_cfg=repo_cfg)


def interpret_fname(fname, repo=None, repo_cfg=None, naming=None):
    spec = _coerce_naming_spec(repo=repo, repo_cfg=repo_cfg, naming=naming)
    style = spec.get("parse", {}).get("style", "legacy")

    if style == "legacy":
        return _interpret_fname_legacy(fname)

    if style == "template":
        return _interpret_fname_template(fname, spec)

    raise ValueError(f"Unsupported parse style {style!r}")


def _meta_to_filename_legacy(meta):
    if "station_id" not in meta:
        raise ValueError(f"station_id not in meta: {meta}")
    if "agency" not in meta:
        raise ValueError(f"agency not in meta: {meta}")
    if "param" not in meta:
        raise ValueError(f"param not in meta: {meta}")
    if "agency_id" not in meta:
        raise ValueError(f"agency_id not in meta: {meta}")

    sid = meta["station_id"]
    subloc = meta.get("subloc")
    station_id = sid if subloc is None else f"{sid}@{subloc}"

    if "syear" in meta and "eyear" in meta:
        year_part = f"{meta['syear']}_{meta['eyear']}"
    elif "year" in meta:
        year_part = f"{meta['year']}"
    else:
        raise ValueError(f"Missing year information in meta: {meta}")

    return (
        f"{meta['agency']}_{station_id}_{meta['agency_id']}"
        f"_{meta['param']}_{year_part}.csv"
    )


def _meta_to_filename_template(meta, spec):
    templates = spec.get("filename_templates", [])
    if not templates:
        raise ValueError("Naming spec has no filename_templates")

    if "station_id" not in meta:
        raise ValueError(f"station_id not in meta: {meta}")
    if "agency" not in meta:
        raise ValueError(f"agency not in meta: {meta}")
    if "param" not in meta:
        raise ValueError(f"param not in meta: {meta}")

    if "year" in meta:
        candidate_templates = [t for t in templates if "{year}" in t]
    elif "syear" in meta and "eyear" in meta:
        candidate_templates = [
            t for t in templates if "{syear}" in t and "{eyear}" in t
        ]
    else:
        candidate_templates = templates

    values = {
        "source": meta["agency"],
        "key": meta["station_id"],
        "subloc": meta.get("subloc"),
        "param": meta["param"],
        "modifier": meta.get("modifier"),
        "agency_id": str(meta.get("agency_id", "*")),
        "year": str(meta.get("year", "*")),
        "syear": str(meta.get("syear", "*")),
        "eyear": str(meta.get("eyear", "*")),
    }
    return _render_repo_template(candidate_templates[0], values)


def meta_to_filename(meta, repo=None, repo_cfg=None, naming=None):
    spec = _coerce_naming_spec(repo=repo, repo_cfg=repo_cfg, naming=naming)
    style = spec.get("parse", {}).get("style", "legacy")

    if style == "legacy":
        return _meta_to_filename_legacy(meta)
    if style == "template":
        return _meta_to_filename_template(meta, spec)

    raise ValueError(f"Unsupported parse style {style!r}")
    
def series_id_from_meta(row, remove_source=False):
    parts = [row["station_id"], row.get("subloc"), row["param"]]
    if not remove_source:
        parts.insert(0, row["agency"])
    return "_".join(str(p) for p in parts if p is not None)
