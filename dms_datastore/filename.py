#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re


_TEMPLATE_TOKEN_RE = re.compile(r"\{([^{}]+)\}")
_TOKEN_NAME_RE = re.compile(r"^[a-z0-9_@]+$")

def naming_spec(repo=None, repo_cfg=None, templates=None):
    if repo_cfg is not None:
        return {
            "filename_templates": list(repo_cfg.get("filename_templates", [])),
            "key_column": repo_cfg.get("key_column", "station_id"),
            "name": repo_cfg.get("name"),
        }

    if repo is not None:
        from dms_datastore import dstore_config
        rcfg = dstore_config.repo_config(repo)
        return {
            "filename_templates": list(rcfg.get("filename_templates", [])),
            "key_column": rcfg.get("key_column", "station_id"),
            "name": rcfg.get("name"),
        }

    if templates is not None:
        if isinstance(templates, str):
            templates = [templates]
        return {
            "filename_templates": list(templates),
            "key_column": "station_id",
            "name": None,
        }

    return {
        "filename_templates": [],
        "key_column": "station_id",
        "name": None,
    }

def extract_year_fname(fname):
    re1 = re.compile(r".*_(\d{4})(?:\..{3})")
    yr = int(re1.match(fname).group(1))
    return yr


def _station_key_from_parts(key, subloc):
    if "@" in key:
        return key
    if subloc is not None and str(subloc).lower() not in ("default", "none", ""):
        return f"{key}@{subloc}"
    return key


def _param_from_parts(param, modifier):
    if modifier is None or str(modifier).lower() in ("default", "none", ""):
        return param
    return f"{param}@{modifier}"


def _template_tokens(template):
    tokens = [m.group(1) for m in _TEMPLATE_TOKEN_RE.finditer(template)]
    for token in tokens:
        if not _TOKEN_NAME_RE.match(token):
            raise ValueError(f"Unsupported template token {token!r}")
    return tokens


def template_required_fields(template, *, key_column="station_id"):
    required = []
    for token in _template_tokens(template):
        base = token.split("@", 1)[0]
        if base == "key":
            base = key_column
        if base not in required:
            required.append(base)
    return required


def _render_repo_template(template, values):
    out = template
    tokens = _template_tokens(template)

    for token in tokens:
        if token == "key":
            val = values.get("key")
            if val is None:
                raise ValueError("Missing required template value: key")
            out = out.replace("{key}", str(val))
        elif token == "key@subloc":
            key = values.get("key")
            if key is None:
                raise ValueError("Missing required template value: key")
            out = out.replace("{key@subloc}", _station_key_from_parts(str(key), values.get("subloc")))
        elif token == "param":
            val = values.get("param")
            if val is None:
                raise ValueError("Missing required template value: param")
            out = out.replace("{param}", str(val))
        elif token == "param@modifier":
            param = values.get("param")
            if param is None:
                raise ValueError("Missing required template value: param")
            out = out.replace("{param@modifier}", _param_from_parts(str(param), values.get("modifier")))
        else:
            val = values.get(token)
            if val is None:
                raise ValueError(f"Missing required template value: {token}")
            out = out.replace("{" + token + "}", str(val))

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
    agency="*",
    year="*",
    syear="*",
    eyear="*",
):
    templates = repo_cfg.get("filename_templates", [])
    if not templates:
        raise ValueError(f"Repo {repo_cfg.get('name')!r} has no filename_templates")

    if sources is None:
        sources = ["*"]
    elif isinstance(sources, str):
        sources = [sources]
    else:
        sources = list(sources)

    out = []
    seen = set()
    for src in sources:
        values = {
            "source": src,
            "agency": agency,
            "key": key,
            "subloc": subloc,
            "param": param,
            "modifier": modifier,
            "agency_id": agency_id,
            "year": year,
            "syear": syear,
            "eyear": eyear,
        }
        for tmpl in templates:
            pat = _render_repo_template(tmpl, values)
            if pat not in seen:
                out.append(pat)
                seen.add(pat)
    return out


def _interpret_fname_legacy(fname):
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


def _template_regex_from_template(template, *, key_column="station_id"):
    pieces = ["^"]
    last = 0
    token_map = {
        "source": r"(?P<source>[a-z0-9]+)",
        "agency": r"(?P<agency>[a-z0-9]+)",
        "key": rf"(?P<{key_column}>[a-z0-9]+)",
        "key@subloc": rf"(?P<key_full>[a-z0-9]+(?:@[a-z0-9_]+)?)",
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
    key_column = repo_cfg.get("key_column", "station_id")
    if not templates:
        raise ValueError(
            f"Repo {repo_cfg.get('name')!r} has no filename_templates for template parse"
        )

    for tmpl in templates:
        rx = _template_regex_from_template(tmpl, key_column=key_column)
        m = rx.match(fname)
        if m is None:
            continue

        gd = m.groupdict()
        meta = {"filename": fname}

        if gd.get("source") is not None:
            meta["source"] = gd["source"]
        if gd.get("agency") is not None:
            meta["agency"] = gd["agency"]

        if gd.get("key_full") is not None:
            key_full = gd["key_full"]
            if "@" in key_full:
                key_val, subloc = key_full.split("@", 1)
            else:
                key_val, subloc = key_full, None
            meta[key_column] = key_val
            meta["subloc"] = subloc
        elif gd.get(key_column) is not None:
            meta[key_column] = gd[key_column]
            meta["subloc"] = None

        if gd.get("agency_id") is not None:
            meta["agency_id"] = gd["agency_id"]

        if gd.get("param_full") is not None:
            param_full = gd["param_full"]
            if "@" in param_full:
                param, modifier = param_full.split("@", 1)
                meta["param"] = param
                meta["modifier"] = modifier
            else:
                meta["param"] = param_full
        elif gd.get("param") is not None:
            meta["param"] = gd["param"]

        for ykey in ("year", "syear", "eyear"):
            if gd.get(ykey) is not None:
                meta[ykey] = gd[ykey]

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


def _template_score(template, meta, *, key_column="station_id"):
    score = 0
    for token in _template_tokens(template):
        if token == "key@subloc":
            score += 2 if meta.get("subloc") not in (None, "default", "none", "") else 1
        elif token == "param@modifier":
            score += 2 if meta.get("modifier") not in (None, "default", "none", "") else 1
        elif token in ("year", "syear", "eyear", key_column, "source", "agency", "agency_id", "param"):
            score += 1
        else:
            score += 1
    return score


def _meta_supports_template(meta, template, *, key_column="station_id"):
    for token in _template_tokens(template):
        if token == "key":
            if meta.get(key_column) is None:
                return False
        elif token == "key@subloc":
            if meta.get(key_column) is None:
                return False
        elif token == "param":
            if meta.get("param") is None:
                return False
        elif token == "param@modifier":
            if meta.get("param") is None:
                return False
        else:
            if meta.get(token) is None:
                return False
    return True

def meta_to_filename(
    meta,
    repo=None,
    repo_cfg=None,
    naming=None,
    include_shard=True,
):
    spec = _coerce_naming_spec(repo=repo, repo_cfg=repo_cfg, naming=naming)
    return _meta_to_filename_template(meta, spec, include_shard=include_shard)


def _meta_to_filename_template(meta, spec, include_shard=True):
    templates = spec.get("filename_templates", [])
    key_column = spec.get("key_column", "station_id")
    if not templates:
        raise ValueError("Naming spec has no filename_templates")

    template_pairs = []
    for tmpl in templates:
        effective = tmpl if include_shard else _drop_shard_tokens(tmpl)
        template_pairs.append((tmpl, effective))

    compatible = [
        (orig, eff)
        for (orig, eff) in template_pairs
        if _meta_supports_template(meta, eff, key_column=key_column)
    ]
    if not compatible:
        raise ValueError(f"No configured filename template is supported by metadata: {meta}")

    orig_template, effective_template = max(
        compatible,
        key=lambda pair: (
            _template_score(pair[1], meta, key_column=key_column),
            len(_template_tokens(pair[1])),
        ),
    )

    values = {
        "source": meta.get("source"),
        "agency": meta.get("agency"),
        "key": meta.get(key_column),
        "subloc": meta.get("subloc"),
        "param": meta.get("param"),
        "modifier": meta.get("modifier"),
        "agency_id": meta.get("agency_id"),
        "year": meta.get("year"),
        "syear": meta.get("syear"),
        "eyear": meta.get("eyear"),
    }
    return _render_repo_template(effective_template, values)


def _drop_shard_tokens(template):
    template = template.replace("_{syear}_{eyear}", "")
    template = template.replace("_{year}", "")
    return template

    raise ValueError(f"Unsupported parse style {style!r}")


def series_id_from_meta(row, remove_source=False):
    parts = [row.get("station_id"), row.get("subloc"), row.get("param")]
    if not remove_source:
        parts.insert(0, row.get("source", row.get("agency")))
    return "_".join(str(p) for p in parts if p is not None)
