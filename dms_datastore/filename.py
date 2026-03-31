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
            "site_key": repo_cfg["site_key"],
            "provider_key": repo_cfg.get("provider_key"),
            "name": repo_cfg.get("name"),
        }

    if repo is not None:
        from dms_datastore import dstore_config
        rcfg = dstore_config.repo_config(repo)
        return {
            "filename_templates": list(rcfg.get("filename_templates", [])),
            "site_key": rcfg["site_key"],
            "provider_key": rcfg.get("provider_key"),
            "name": rcfg.get("name"),
        }

    if templates is not None:
        if isinstance(templates, str):
            templates = [templates]

        return {
            "filename_templates": list(templates),
            "site_key": "station_id",   # default for template-only
            "provider_key": None,
            "name": "<templates>",
        }

    raise ValueError("Must provide one of repo, repo_cfg, or templates")

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


def template_required_fields(template, *, site_key="station_id"):
    required = []
    for token in _template_tokens(template):
        base = token.split("@", 1)[0]
        if base == "key":
            base = site_key
        if base not in required:
            required.append(base)
    return required


def _render_repo_template(template, values):
    out = template
    tokens = _template_tokens(template)

    for token in tokens:
        # Generic handling of optional "@"
        if "@" in token:
            base, suffix = token.split("@", 1)

            base_val = values.get(base)
            if base_val is None:
                raise ValueError(f"Missing required template value: {base}")

            suffix_val = values.get(suffix)

            if suffix_val is not None and str(suffix_val).lower() not in ("default", "none", ""):
                val = f"{base_val}@{suffix_val}"
            else:
                val = str(base_val)

            out = out.replace("{" + token + "}", val)

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
    providers=None,
    year="*",
    syear="*",
    eyear="*",
    **extra_fields,
):
    templates = repo_cfg.get("filename_templates", [])
    if not templates:
        raise ValueError(f"Repo {repo_cfg.get('name')!r} has no filename_templates")

    site_key = repo_cfg["site_key"]
    provider_key = repo_cfg.get("provider_key")

    if providers is None:
        provider_values = ["*"]
    elif isinstance(providers, str):
        provider_values = [providers]
    else:
        provider_values = list(providers)

    if "@" in str(key):
        key_base, key_subloc = str(key).split("@", 1)
        if subloc is None:
            subloc = key_subloc
    else:
        key_base = key

    out = []
    seen = set()

    for provider in provider_values:
        values = {
            site_key: key_base,
            "subloc": subloc,
            "param": param,
            "modifier": modifier,
            "year": year,
            "syear": syear,
            "eyear": eyear,
        }

        if provider_key is not None:
            values[provider_key] = provider

        values.update(extra_fields)

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


def _token_regex(token, key_column):
    if "@" in token:
        base, suffix = token.split("@", 1)
        if base == "key":
            base = key_column
        return rf"(?P<{base}_full>[a-z0-9]+(?:@[a-z0-9_]+)?)"
    else:
        if token == "key":
            token = key_column

        if token in ("year", "syear", "eyear"):
            return rf"(?P<{token}>\d{{4}})"

        return rf"(?P<{token}>[a-z0-9]+)"


def _template_regex_from_template(template, *, key_column="station_id"):
    pieces = ["^"]
    last = 0

    for m in _TEMPLATE_TOKEN_RE.finditer(template):
        pieces.append(re.escape(template[last:m.start()]))
        token = m.group(1)
        pieces.append(_token_regex(token, key_column))
        last = m.end()

    pieces.append(re.escape(template[last:]))
    pieces.append("$")
    return re.compile("".join(pieces))


def _template_regex_from_template(template, *, key_column="station_id"):
    pieces = ["^"]
    last = 0

    for m in _TEMPLATE_TOKEN_RE.finditer(template):
        pieces.append(re.escape(template[last:m.start()]))
        token = m.group(1)
        pieces.append(_token_regex(token, key_column))
        last = m.end()

    pieces.append(re.escape(template[last:]))
    pieces.append("$")
    return re.compile("".join(pieces))


def _interpret_fname_template(fname, repo_cfg):
    fname = os.path.split(fname)[1]
    templates = repo_cfg.get("filename_templates", [])
    site_key = repo_cfg.get("site_key", "station_id")
    if not templates:
        raise ValueError(
            f"Repo {repo_cfg.get('name')!r} has no filename_templates for template parse"
        )

    for tmpl in templates:
        rx = _template_regex_from_template(tmpl, key_column=site_key)
        m = rx.match(fname)
        if m is None:
            continue

        gd = m.groupdict()
        meta = {"filename": fname, "subloc": None}

        for k, v in gd.items():
            if v is None:
                continue

            if k.endswith("_full"):
                base = k[:-5]

                if "@" in v:
                    base_val, suffix_val = v.split("@", 1)
                else:
                    base_val, suffix_val = v, None

                if base == site_key:
                    meta[site_key] = base_val
                    meta["subloc"] = suffix_val
                elif base == "param":
                    meta["param"] = base_val
                    if suffix_val is not None:
                        meta["modifier"] = suffix_val
                else:
                    meta[base] = base_val
                    if suffix_val is not None:
                        meta[f"{base}_suffix"] = suffix_val
            else:
                meta[k] = v

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
        if "@" in token:
            base, suffix = token.split("@", 1)

            if base == "key":
                base = key_column

            if meta.get(suffix) not in (None, "default", "none", ""):
                score += 2
            else:
                score += 1
        else:
            score += 1
    return score


def _meta_supports_template(meta, template, *, key_column="station_id"):
    for token in _template_tokens(template):
        base = token.split("@", 1)[0]

        if base == "key":
            base = key_column

        if meta.get(base) is None:
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
    site_key = spec.get("site_key", "station_id")
    if not templates:
        raise ValueError("Naming spec has no filename_templates")

    template_pairs = []
    for tmpl in templates:
        effective = tmpl if include_shard else _drop_shard_tokens(tmpl)
        template_pairs.append((tmpl, effective))

    compatible = [
        (orig, eff)
        for (orig, eff) in template_pairs
        if _meta_supports_template(meta, eff, key_column=site_key)
    ]
    if not compatible:
        raise ValueError(f"No configured filename template is supported by metadata: {meta}")

    orig_template, effective_template = max(
        compatible,
        key=lambda pair: (
            _template_score(pair[1], meta, key_column=site_key),
            len(_template_tokens(pair[1])),
        ),
    )

    values = dict(meta)

    # support templates that still use {key} / {key@subloc}

    values.setdefault("subloc", meta.get("subloc"))
    values.setdefault("modifier", meta.get("modifier"))
    values.setdefault("year", meta.get("year"))
    values.setdefault("syear", meta.get("syear"))
    values.setdefault("eyear", meta.get("eyear"))

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
