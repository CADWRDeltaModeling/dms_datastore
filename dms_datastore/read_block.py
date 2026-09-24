#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Multi-station, multi-parameter block reading built on :func:`read_ts_repo`.

A "block" is a single tabular assembly of many station/parameter time series.
:func:`read_ts_block` gathers the requested series, then reshapes them into one
of several layouts selected by the *dim* specification.

Layout specification
--------------------
The *dim* argument is written ``"[[<index dims>],[<column dims>]]"`` using the
single-letter dimension tokens ``t`` (datetime), ``s`` (station) and ``p``
(param).  Tokens joined by ``_`` inside the column part are flattened into one
underscore-joined column label.

===========  ==================  ==========================================
Alias        Equivalent dim      Result
===========  ==================  ==========================================
``tidy``     ``[[t,s],[p]]``     index (datetime, station), one col per param
``long``     ``[[t,s,p],[]]``    fully stacked with a single ``value`` column
``sd_tidy``  ``[[s,t],[p]]``     index (station, datetime), one col per param
``flat``     ``[[t],[s_p]]``     datetime index, ``station_param`` columns
``wide``     ``[[t],[s]]``       datetime index, station columns (one param)
===========  ==================  ==========================================

A dimension may be omitted from both parts only when the request resolves to a
single value for that dimension.

Sublocations and modifiers
---------------------------
A station with multiple sublocations (e.g. ``mrz`` with ``upper``/``lower``
sensors) is requested by appending ``@subloc`` directly to the station id,
and a variable modifier is requested by appending ``@modifier`` to the param,
exactly as :func:`read_ts_repo` expects them::

    read_ts_block(station=["mrz@upper", "mrz@lower"], param="ec", dim="wide")

The ``station@subloc`` string is used as-is as the ``station`` dimension
label, so ``mrz@upper`` and ``mrz@lower`` are distinct stations throughout —
they never collide in any layout.  When using ``request`` CSVs, an explicit
``subloc`` column is also accepted (see :func:`read_request_csv`) and is
folded into the station label the same way.  There is no separate
``--subloc`` CLI option; encode the subloc in ``--station`` instead.

Repository selection
--------------------
When *repo* is ``None`` the repositories named by the ``repo_search_order``
configuration key are tried in order for each station/parameter pair, and the
first repository holding data wins.
"""

import itertools
import logging
import re

import click
import pandas as pd

from dms_datastore import dstore_config
from dms_datastore.read_multi import read_ts_repo

logger = logging.getLogger(__name__)

__all__ = [
    "read_ts_block",
    "ts_block",
    "read_request_csv",
    "parse_dim",
    "DIM_ALIASES",
]


DIM_ALIASES = {
    "tidy": "[[t,s],[p]]",
    "long": "[[t,s,p],[]]",
    "sd_tidy": "[[s,t],[p]]",
    "flat": "[[t],[s_p]]",
    "wide": "[[t],[s]]",
}

_DIM_NAMES = {"t": "datetime", "s": "station", "p": "param"}

_DIM_PATTERN = re.compile(r"^\[\[([^\[\]]*)\],\[([^\[\]]*)\]\]$")


def parse_dim(dim):
    """
    Parse a block layout specification into index and column dimensions.

    Parameters
    ----------
    dim : str
        Either an alias from :data:`DIM_ALIASES` (e.g. ``"tidy"``) or an
        explicit specification such as ``"[[t,s],[p]]"`` or ``"[[t],[s_p]]"``.

    Returns
    -------
    index_dims : list of str
        Canonical dimension names (``"datetime"``, ``"station"``, ``"param"``)
        forming the output index, in order.
    column_groups : list of list of str
        One entry per column level.  Entries with more than one dimension are
        flattened into a single underscore-joined column label.

    Raises
    ------
    ValueError
        Raised if the specification is malformed, uses an unknown token, or
        repeats a dimension.

    Examples
    --------
    ::

        parse_dim("tidy")        # (["datetime", "station"], [["param"]])
        parse_dim("[[t],[s_p]]") # (["datetime"], [["station", "param"]])
    """
    spec = DIM_ALIASES.get(dim, dim)
    text = "".join(str(spec).split())

    match = _DIM_PATTERN.match(text)
    if match is None:
        raise ValueError(
            f"Malformed dim specification {dim!r}. Expected an alias in "
            f"{sorted(DIM_ALIASES)} or a form like '[[t,s],[p]]'."
        )

    index_tokens = [tok for tok in match.group(1).split(",") if tok]
    column_tokens = [tok for tok in match.group(2).split(",") if tok]

    def expand(token, part):
        letters = token.split("_")
        for letter in letters:
            if letter not in _DIM_NAMES:
                raise ValueError(
                    f"Unknown dimension token {letter!r} in {part} part of "
                    f"dim {dim!r}. Valid tokens are {sorted(_DIM_NAMES)}."
                )
        return [_DIM_NAMES[letter] for letter in letters]

    index_dims = []
    for token in index_tokens:
        expanded = expand(token, "index")
        if len(expanded) > 1:
            raise ValueError(
                f"Composite token {token!r} is not allowed in the index part "
                f"of dim {dim!r}. Composites apply to columns only."
            )
        index_dims.extend(expanded)

    column_groups = [expand(token, "column") for token in column_tokens]

    seen = list(index_dims) + [d for group in column_groups for d in group]
    duplicated = {d for d in seen if seen.count(d) > 1}
    if duplicated:
        raise ValueError(
            f"Dimension(s) {sorted(duplicated)} appear more than once in dim {dim!r}."
        )

    if not index_dims:
        raise ValueError(f"dim {dim!r} declares no index dimensions")

    return index_dims, column_groups


def read_request_csv(request):
    """
    Read a station/parameter request table.

    Parameters
    ----------
    request : str or path-like
        CSV file with ``station`` and ``param`` columns and an optional
        ``subloc`` column.  A non-empty ``subloc`` is appended to the station
        label as ``station@subloc``.

    Returns
    -------
    list of tuple of str
        Ordered, de-duplicated ``(station, param)`` pairs.

    Raises
    ------
    ValueError
        Raised if required columns are missing or the table is empty.

    Examples
    --------
    ::

        pairs = read_request_csv("station_list.csv")
    """
    table = pd.read_csv(request, comment="#", dtype=str).rename(
        columns=lambda c: str(c).strip().lower()
    )

    missing = [col for col in ("station", "param") if col not in table.columns]
    if missing:
        raise ValueError(
            f"Request file {request} is missing required column(s): {missing}"
        )

    pairs = []
    for _, row in table.iterrows():
        station = str(row["station"]).strip()
        param = str(row["param"]).strip()
        subloc = row.get("subloc")
        if isinstance(subloc, str) and subloc.strip():
            station = f"{station}@{subloc.strip()}"
        pair = (station, param)
        if pair not in pairs:
            pairs.append(pair)

    if not pairs:
        raise ValueError(f"Request file {request} produced no station/param pairs")

    return pairs


def _pairs_from_lists(station, param):
    """Form the ordered cross product of station and param lists."""
    stations = [station] if isinstance(station, str) else list(station)
    params = [param] if isinstance(param, str) else list(param)

    if not stations:
        raise ValueError("station list is empty")
    if not params:
        raise ValueError("param list is empty")

    return [(s, p) for s in stations for p in params]


def _value_series(ts, station, param, repo_name):
    """Reduce a repository read to a single value series."""
    if ts.shape[1] == 1:
        return ts.iloc[:, 0]
    if "value" in ts.columns:
        return ts["value"]
    raise ValueError(
        f"Series for station {station!r} param {param!r} in repo {repo_name!r} "
        f"has ambiguous value columns: {list(ts.columns)}"
    )


def _read_pair(station, param, order, start, end, force_regular, freq_resolver,
               provider_priority):
    """Read one station/param pair from the first repo in *order* that has it."""
    for repo_name in order:
        ts = read_ts_repo(
            station,
            param,
            repo=repo_name,
            start=start,
            end=end,
            force_regular=force_regular,
            freq_resolver=freq_resolver,
            provider_priority=provider_priority,
        )
        if ts is None or ts.empty:
            continue
        return _value_series(ts, station, param, repo_name), repo_name
    return None, None


def _ordered_categories(values, order):
    """Return *order* restricted to values actually present, preserving order."""
    present = set(values)
    return [item for item in order if item in present]


def _composite_series(frame, group):
    """Build an ordered categorical joining several dimension labels with '_'."""
    joined = frame[group[0]].astype(str)
    for dim_name in group[1:]:
        joined = joined.str.cat(frame[dim_name].astype(str), sep="_")

    level_cats = []
    for dim_name in group:
        col = frame[dim_name]
        if isinstance(col.dtype, pd.CategoricalDtype):
            level_cats.append(list(col.cat.categories))
        else:
            level_cats.append(sorted(col.astype(str).unique()))

    categories = ["_".join(combo) for combo in itertools.product(*level_cats)]
    return pd.Categorical(joined, categories=categories, ordered=True)


def ts_block(series, dim="tidy"):
    """
    Assemble already-read series into a block with the requested layout.

    Parameters
    ----------
    series : list of tuple
        Entries of ``(station, param, series)`` where ``series`` is a
        :class:`pandas.Series` with a ``DatetimeIndex``.  The order of the
        entries sets the station and param ordering in the output.
    dim : str, optional
        Layout alias or specification; see :func:`parse_dim`.  Default
        ``"tidy"``.

    Returns
    -------
    pandas.DataFrame
        Block in the requested layout.

    Raises
    ------
    ValueError
        Raised if *series* is empty, or if *dim* omits a dimension that has
        more than one distinct value in the request.

    See Also
    --------
    read_ts_block : Repository-aware entry point.

    Examples
    --------
    ::

        block = ts_block([("sjw", "ec", ec_sjw), ("emm2", "ec", ec_emm2)],
                         dim="wide")
    """
    if not series:
        raise ValueError("No series supplied to ts_block")

    index_dims, column_groups = parse_dim(dim)

    station_order = []
    param_order = []
    frames = []
    for station, param, values in series:
        if station not in station_order:
            station_order.append(station)
        if param not in param_order:
            param_order.append(param)
        piece = values.rename("value").to_frame()
        piece.index.name = "datetime"
        piece = piece.reset_index()
        piece["station"] = station
        piece["param"] = param
        frames.append(piece)

    long = pd.concat(frames, ignore_index=True)
    long["station"] = pd.Categorical(
        long["station"],
        categories=_ordered_categories(long["station"], station_order),
        ordered=True,
    )
    long["param"] = pd.Categorical(
        long["param"],
        categories=_ordered_categories(long["param"], param_order),
        ordered=True,
    )

    used = set(index_dims) | {d for group in column_groups for d in group}
    for dim_name in ("datetime", "station", "param"):
        if dim_name not in used and long[dim_name].nunique() > 1:
            raise ValueError(
                f"dim {dim!r} omits the {dim_name!r} dimension, but the request "
                f"spans {long[dim_name].nunique()} distinct {dim_name} values."
            )

    if not column_groups:
        return long.set_index(index_dims).sort_index()[["value"]]

    work = long
    column_keys = []
    for group in column_groups:
        if len(group) == 1:
            column_keys.append(group[0])
        else:
            key = "_".join(group)
            work = work.assign(**{key: _composite_series(work, group)})
            column_keys.append(key)

    block = work.pivot_table(
        index=index_dims,
        columns=column_keys,
        values="value",
        aggfunc="first",
        observed=True,
        dropna=False,  # keep requested series whose window is entirely flagged/missing
    )
    return block.sort_index()


def read_ts_block(
    station=None,
    param=None,
    request=None,
    repo=None,
    dim="tidy",
    start=None,
    end=None,
    force_regular=True,
    freq_resolver=None,
    provider_priority="infer",
):
    """
    Read many station/parameter series from the repositories as one block.

    Parameters
    ----------
    station : str or list of str or None, optional
        Station identifier(s), optionally with a ``station@subloc`` suffix.
        Combined with *param* as a cross product.  Mutually exclusive with
        *request*.
    param : str or list of str or None, optional
        Variable name(s) such as ``"ec"`` or ``"temp"``, optionally with a
        ``param@modifier`` suffix.  Mutually exclusive with *request*.
    request : str or path-like or None, optional
        CSV file naming explicit ``station``/``param`` pairs; see
        :func:`read_request_csv`.  Mutually exclusive with *station*/*param*.
    repo : str or list of str or None, optional
        Repository name, or ordered list of names, to search.  ``None``
        (default) uses the configured ``repo_search_order``.
    dim : str, optional
        Output layout alias or specification; see :func:`parse_dim`.  Default
        ``"tidy"``.
    start : str or pandas.Timestamp or None, optional
        Inclusive start of the requested window.
    end : str or pandas.Timestamp or None, optional
        Inclusive end of the requested window.
    force_regular : bool, optional
        Require a regular time index from each repository read.  Default
        ``True``.
    freq_resolver : str or dict or None, optional
        Frequency reconciliation strategy forwarded to :func:`read_ts_repo`.
    provider_priority : "infer" or None or str or list of str, optional
        Provider resolution forwarded to :func:`read_ts_repo`.

    Returns
    -------
    pandas.DataFrame
        Block in the requested layout.

    Raises
    ------
    ValueError
        Raised if the request is not specified exactly once, or if no
        requested pair yields data in any searched repository.

    See Also
    --------
    read_ts_repo : Single station/variable repository reader.
    ts_block : Reshaping of already-read series.

    Examples
    --------
    ::

        block = read_ts_block(station=["sjw", "emm2"], param=["ec", "temp"],
                              dim="tidy", start="2026-02-05")

        block = read_ts_block(request="station_list.csv", dim="flat",
                              start="2026-02-05", end="2026-02-06")
    """
    if request is not None:
        if station is not None or param is not None:
            raise ValueError(
                "request is mutually exclusive with the station and param arguments"
            )
        pairs = read_request_csv(request)
    else:
        if station is None or param is None:
            raise ValueError(
                "Supply either request, or both station and param"
            )
        pairs = _pairs_from_lists(station, param)

    order = dstore_config.repo_search_order(repo)

    start = pd.to_datetime(start) if start is not None else None
    end = pd.to_datetime(end) if end is not None else None

    series = []
    missing = []
    for station_id, variable in pairs:
        values, repo_used = _read_pair(
            station_id,
            variable,
            order,
            start,
            end,
            force_regular,
            freq_resolver,
            provider_priority,
        )
        if values is None:
            missing.append((station_id, variable))
            logger.warning(
                "No data for station %s param %s in repos %s",
                station_id,
                variable,
                order,
            )
            continue
        logger.info(
            "Read station %s param %s from repo %s", station_id, variable, repo_used
        )
        series.append((station_id, variable, values))

    if not series:
        raise ValueError(
            f"No data found for any of {len(pairs)} requested station/param "
            f"pairs in repos {order}"
        )

    if missing:
        logger.warning("Omitted %d station/param pairs with no data", len(missing))

    return ts_block(series, dim=dim)


@click.command(
    help=(
        "Read a block of station/parameter time series from the repositories "
        "and write it to a CSV file.\n\n"
        "Use --station/--param (cross product) or --request FILE, not both."
    )
)
@click.help_option("-h", "--help")
@click.option(
    "--station",
    multiple=True,
    help="Station id, optionally station@subloc (e.g. mrz@upper). Repeatable.",
)
@click.option(
    "--param",
    multiple=True,
    help="Variable name, optionally param@modifier. Repeatable.",
)
@click.option(
    "--request",
    type=click.Path(exists=True, dir_okay=False),
    default=None,
    help="CSV of explicit station,param[,subloc] pairs.",
)
@click.option(
    "--repo",
    multiple=True,
    help="Repository to search. Repeatable to set an order. "
    "Default is the configured repo_search_order.",
)
@click.option(
    "--dim",
    default="tidy",
    show_default=True,
    help="Layout alias (tidy, long, sd_tidy, flat, wide) or a form like '[[t,s],[p]]'.",
)
@click.option("--start", "-s", default=None, help="Inclusive start date/time.")
@click.option("--end", "-e", default=None, help="Inclusive end date/time.")
@click.option(
    "--force-regular/--no-force-regular",
    default=True,
    show_default=True,
    help="Require a regular time index from each repository read.",
)
@click.option(
    "--freq-resolver",
    default=None,
    help="Frequency reconciliation strategy, e.g. interp_to_finer.",
)
@click.option(
    "--output",
    "-o",
    required=True,
    type=click.Path(dir_okay=False),
    help="Output CSV file.",
)
def read_block_cli(
    station,
    param,
    request,
    repo,
    dim,
    start,
    end,
    force_regular,
    freq_resolver,
    output,
):
    """Command-line entry point for :func:`read_ts_block`."""
    block = read_ts_block(
        station=list(station) if station else None,
        param=list(param) if param else None,
        request=request,
        repo=list(repo) if repo else None,
        dim=dim,
        start=start,
        end=end,
        force_regular=force_regular,
        freq_resolver=freq_resolver,
    )
    block.to_csv(output)
    click.echo(f"Wrote {block.shape[0]} rows x {block.shape[1]} columns to {output}")


if __name__ == "__main__":
    read_block_cli()
