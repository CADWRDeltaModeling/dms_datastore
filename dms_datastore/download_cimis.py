"""Download daily or hourly CIMIS station data, one CSV per station and request window.

Stations are looked up in the ``atmospheric`` registry (``agency == "cimis"``).
Requests use the CIMIS web API and require the ``CIMIS_API_KEY`` environment
variable when run from the command line.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import date, timedelta
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import click
import pandas as pd
from vtools import ts_splice

from dms_datastore import dstore_config
from dms_datastore.logging_config import configure_logging, resolve_loglevel

logger = logging.getLogger(__name__)

CIMIS_REGISTRY = "atmospheric"
CIMIS_AGENCY = "cimis"
# The CIMIS network began in 1982; used as the default beginning of record.
CIMIS_RECORD_START = date(1982, 1, 1)

API_URL = "https://et.water.ca.gov/StationWeb/GetDataByStationNumber"
DATA_ITEMS = {
    "D": (
        "day-asce-eto,day-precip,day-sol-rad-avg,day-vap-pres-avg,"
        "day-air-tmp-max,day-air-tmp-min,day-air-tmp-avg,day-rel-hum-max,"
        "day-rel-hum-min,day-rel-hum-avg,day-dew-pnt,day-wind-spd-avg,"
        "day-wind-run,day-soil-tmp-avg"
    ),
    "H": (
        "hly-air-tmp,hly-dew-pnt,hly-asce-eto,hly-asce-etr,hly-net-rad,"
        "hly-precip,hly-rel-hum,hly-res-wind,hly-soil-tmp,hly-sol-rad,"
        "hly-vap-pres,hly-wind-dir,hly-wind-spd"
    ),
}
FREQ_NAMES = {"D": "daily", "H": "hourly"}
OUTPUT_DATE_FIELD = "Date"
# Hourly records carry an hour-ending "Hour" field, 0100-2400.
OUTPUT_HOUR_FIELD = "Hour"


class RequestTimeoutError(RuntimeError):
    pass


def normalize_freq(freq):
    """Return ``"D"`` or ``"H"`` for a case-insensitive frequency code."""
    code = str(freq).strip().upper()
    if code not in FREQ_NAMES:
        raise ValueError(f"freq must be D (daily) or H (hourly), got {freq!r}")
    return code


def normalize_station_id(station):
    """Return the registry id for a CIMIS station given as ``140`` or ``cms140``."""
    sid = str(station).strip().lower()
    return f"cms{int(sid)}" if sid.isdigit() else sid


def cimis_stations(station_ids=None):
    """Return CIMIS stations from the atmospheric registry.

    Parameters
    ----------
    station_ids : iterable of str or int, optional
        Registry station ids (``cms140``) or CIMIS station numbers
        (``140``). All CIMIS stations are returned when omitted.

    Returns
    -------
    pandas.DataFrame
        Registry rows, including ``station_id`` and ``agency_id``.

    Raises
    ------
    ValueError
        If any requested id is not a CIMIS station in the registry.
    """
    reg = dstore_config.registry_df(CIMIS_REGISTRY)
    reg = reg[reg["agency"] == CIMIS_AGENCY]
    if station_ids:
        station_ids = list(dict.fromkeys(normalize_station_id(s) for s in station_ids))
        missing = sorted(set(station_ids) - set(reg["station_id"]))
        if missing:
            raise ValueError(
                f"Not CIMIS stations in the {CIMIS_REGISTRY} registry: {missing}"
            )
        reg = reg[reg["station_id"].isin(station_ids)]
    return reg.reset_index(drop=True)


def flatten_record(value: Any, prefix: str = "") -> dict[str, str]:
    """Flatten nested JSON objects into CSV-friendly string fields."""
    if not isinstance(value, dict):
        return {prefix: stringify(value)} if prefix else {}

    result: dict[str, str] = {}
    for key, nested_value in value.items():
        field = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(nested_value, dict):
            result.update(flatten_record(nested_value, field))
        elif isinstance(nested_value, list):
            result[field] = json.dumps(nested_value, separators=(",", ":"))
        else:
            result[field] = stringify(nested_value)
    return result


def stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)


def find_date(record: dict[str, str]) -> str:
    for key, value in record.items():
        if key.rsplit(".", 1)[-1].lower() == "date" and value:
            try:
                return date.fromisoformat(value[:10]).isoformat()
            except ValueError:
                continue
    raise ValueError("record has no usable Date field")


def extract_records(payload: Any) -> list[dict[str, str]]:
    if not isinstance(payload, dict):
        raise ValueError("API response is not a JSON object")

    status = payload.get("Status")
    if isinstance(status, dict):
        code = str(status.get("StatusCode", ""))
        message = str(status.get("Message", ""))
        if code and code not in {"200", "0"} and message.lower() != "success":
            raise ValueError(f"CIMIS API error {code}: {message}")

    data = payload.get("Data", payload)
    providers = data.get("Providers", []) if isinstance(data, dict) else []
    records: list[Any] = []
    for provider in providers:
        if isinstance(provider, dict):
            provider_records = provider.get("Records", [])
            if isinstance(provider_records, list):
                records.extend(provider_records)

    if not records and isinstance(data, dict) and isinstance(data.get("Records"), list):
        records = data["Records"]

    normalized = []
    for raw_record in records:
        if not isinstance(raw_record, dict):
            raise ValueError("API response contains a non-object record")
        normalized_record = flatten_record(raw_record)
        normalized_record[OUTPUT_DATE_FIELD] = find_date(normalized_record)
        normalized.append(normalized_record)
    return normalized


def request_records(
    station_number: int,
    start_date: date,
    end_date: date,
    api_key: str,
    timeout: int,
    freq: str = "D",
) -> list[dict[str, str]]:
    parameters = {
        "stationNbrs": str(station_number),
        "startDate": start_date.isoformat(),
        "endDate": end_date.isoformat(),
        "isHourly": "true" if freq == "H" else "false",
        "unitOfMeasure": "E",
        "dataItems": DATA_ITEMS[freq],
        "prefer": "WSN",
    }
    request = Request(
        f"{API_URL}?{urlencode(parameters)}",
        headers={
            "Accept": "application/json",
            "Ocp-Apim-Subscription-Key": api_key,
            "User-Agent": "cimis-downloader/1.0",
        },
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            content_type = response.headers.get_content_type()
            body = response.read().decode("utf-8", errors="replace")
            if content_type != "application/json":
                preview = " ".join(body.split())[:300]
                raise RuntimeError(
                    f"API returned HTTP {response.status} with content type "
                    f"{content_type!r}, not JSON: {preview}"
                )
            try:
                payload = json.loads(body)
            except json.JSONDecodeError as error:
                preview = " ".join(body.split())[:300]
                raise RuntimeError(f"API returned invalid JSON: {preview}") from error
    except HTTPError as error:
        body = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {error.code}: {body[:300]}") from error
    except TimeoutError as error:
        raise RequestTimeoutError(f"request timed out after {timeout} seconds") from error
    except URLError as error:
        if isinstance(error.reason, TimeoutError):
            raise RequestTimeoutError(
                f"request timed out after {timeout} seconds"
            ) from error
        raise RuntimeError(f"request failed: {error}") from error
    return extract_records(payload)


def chunk_windows(start_date: date, end_date: date, freq: str) -> list[tuple[date, date]]:
    """Split ``[start_date, end_date]`` into request windows.

    The API returns at most 5000 records per request, so daily requests
    break at decades (Jan 1, 1990, 2000, ...) and hourly requests at
    Jan 1 and Jul 1.

    Parameters
    ----------
    start_date, end_date : date
        Inclusive window.
    freq : {"D", "H"}
        Daily or hourly.

    Returns
    -------
    list of (date, date)
        Inclusive sub-windows covering the request in order.
    """
    windows = []
    chunk_start = start_date
    while chunk_start <= end_date:
        if freq == "D":
            boundary = date((chunk_start.year // 10 + 1) * 10, 1, 1)
        elif chunk_start.month < 7:
            boundary = date(chunk_start.year, 7, 1)
        else:
            boundary = date(chunk_start.year + 1, 1, 1)
        chunk_end = min(boundary - timedelta(days=1), end_date)
        windows.append((chunk_start, chunk_end))
        chunk_start = chunk_end + timedelta(days=1)
    return windows


def records_to_frame(records: list[dict[str, str]], freq: str) -> pd.DataFrame:
    """Convert API records to a DataFrame indexed by timestamp.

    Hourly ``Hour`` values are hour-ending (``0100``-``2400``), so ``2400``
    becomes midnight of the following day. ``Date`` and ``Hour`` are
    dropped once folded into the index.
    """
    df = pd.DataFrame(records)
    index = pd.to_datetime(df[OUTPUT_DATE_FIELD])
    if freq == "H":
        index = index + pd.to_timedelta(df[OUTPUT_HOUR_FIELD].astype(int) // 100, unit="h")
    df = df.drop(columns=[c for c in (OUTPUT_DATE_FIELD, OUTPUT_HOUR_FIELD) if c in df.columns])
    df.index = pd.DatetimeIndex(index, name="datetime")
    return df.sort_index()


def process_station(
    station_id: str,
    station_number: int,
    dest_dir: Path,
    start_date: date,
    end_date: date,
    endfile,
    api_key: str,
    timeout: int,
    overwrite: bool = False,
    freq: str = "D",
) -> str:
    """Download one station for ``[start_date, end_date]`` into a single CSV.

    Requests are made per :func:`chunk_windows`, stacked, and combined with
    :func:`vtools.ts_splice`.

    Parameters
    ----------
    station_id : str
        Registry station id, e.g. ``cms140``.
    station_number : int
        CIMIS station number (registry ``agency_id``).
    dest_dir : Path
        Output directory.
    start_date, end_date : date
        Requested window, inclusive.
    endfile : int
        End year used in the file name (9999 for open-ended requests).
    api_key : str
        CIMIS API subscription key.
    timeout : int
        HTTP timeout in seconds.
    overwrite : bool, optional
        Replace an existing file instead of skipping it.
    freq : {"D", "H"}, optional
        Daily or hourly data.

    Returns
    -------
    str
        Outcome, as in ``download_cdec``: ``"success"``, ``"exists"``
        (skipped existing file) or ``"no_data"`` (nothing written).
    """
    output_path = (
        Path(dest_dir)
        / f"cimis_{station_id}_{station_number}_{start_date.year}_{endfile}.csv"
    )
    if output_path.exists() and not overwrite:
        logger.info(f"Skipping existing station because file exists: {output_path}")
        return "exists"

    frames = []
    for chunk_start, chunk_end in chunk_windows(start_date, end_date, freq):
        records = request_records(
            station_number, chunk_start, chunk_end, api_key, timeout, freq
        )
        logger.debug(
            f"{station_id}: downloaded {len(records)} rows "
            f"({chunk_start} to {chunk_end})"
        )
        if records:
            frames.append(records_to_frame(records, freq))

    if not frames:
        return "no_data"
    columns = list(dict.fromkeys(c for frame in frames for c in frame.columns))
    df = ts_splice([frame.reindex(columns=columns) for frame in frames], transition="prefer_last")
    df.index.name = "datetime"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path)
    logger.info(f"{station_id}: wrote {len(df)} rows to {output_path.name}")
    return "success"


def cimis_download(
    stations: pd.DataFrame,
    dest_dir,
    api_key: str,
    start: date | None = None,
    end: date | None = None,
    overwrite: bool = False,
    timeout: int = 60,
    freq: str = "D",
) -> None:
    """Download daily or hourly CIMIS data for each station into ``dest_dir``.

    Each station is written to
    ``cimis_<station_id>_<agency_id>_<syear>_<eyear>.csv``, where
    ``eyear`` is 9999 when ``end`` is omitted. Existing files are skipped
    unless ``overwrite`` is set. As in ``download_cdec``, stations with no
    data in the window write nothing and are listed at INFO level. Request
    failures are logged per station and reported together at the end.

    Parameters
    ----------
    stations : pandas.DataFrame
        Station rows with ``station_id`` and ``agency_id`` (CIMIS station
        number) columns, e.g. from :func:`cimis_stations`.
    dest_dir : str or Path
        Output directory; created if needed.
    api_key : str
        CIMIS API subscription key.
    start : date, optional
        First day. Defaults to the start of the CIMIS network (1982).
    end : date, optional
        Last day, inclusive. Defaults to the latest available (today).
    overwrite : bool, optional
        Replace existing files instead of skipping them.
    timeout : int, optional
        HTTP timeout in seconds.
    freq : str, optional
        ``"D"`` (daily, default) or ``"H"`` (hourly), case-insensitive.

    Raises
    ------
    ValueError
        If ``api_key`` is empty, ``freq`` is invalid, ``end`` is after
        today, or ``start`` is after ``end``.
    RuntimeError
        If any station failed.
    """
    if not api_key:
        raise ValueError("api_key is required")
    freq = normalize_freq(freq)
    today = date.today()
    if end is not None and end > today:
        raise ValueError(f"end {end} is after today ({today})")
    endfile = 9999 if end is None else end.year
    end = end or today
    start = start or CIMIS_RECORD_START
    if start > end:
        raise ValueError(f"start {start} is after end {end}")
    dest_dir = Path(dest_dir)

    failures = []
    no_data = []
    for _, station in stations.iterrows():
        station_id = station["station_id"]
        try:
            outcome = process_station(
                station_id,
                int(station["agency_id"]),
                dest_dir,
                start,
                end,
                endfile,
                api_key,
                timeout,
                overwrite,
                freq,
            )
        except (KeyError, ValueError, RuntimeError, OSError) as error:
            failures.append(station_id)
            logger.error(f"{station_id}: {error}")
            continue
        if outcome == "no_data":
            no_data.append(station_id)
            logger.info(
                f"No data found for station={station_id} freq={FREQ_NAMES[freq]} "
                f"window={start} to {end}"
            )
    if no_data:
        logger.info(f"Failed query stations (no data): {no_data}")
    if failures:
        raise RuntimeError(f"CIMIS download failed for stations: {failures}")


@click.command()
@click.option(
    "--dest",
    "dest_dir",
    default="cimis_download",
    type=click.Path(path_type=Path),
    help="Destination directory for station CSV files.",
)
@click.option(
    "--start",
    default=None,
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="First date, e.g. 2024-01-01. Default: beginning of record (1982).",
)
@click.option(
    "--end",
    default=None,
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Last date, inclusive; may not be after today. Default: latest available.",
)
@click.option(
    "--stations",
    multiple=True,
    help="Station(s) as registry id (cms140) or CIMIS number (140). Default: all CIMIS stations in the atmospheric registry.",
)
@click.option(
    "--overwrite",
    is_flag=True,
    help="Overwrite existing files (otherwise they are skipped).",
)
@click.option(
    "--freq",
    default="D",
    show_default=True,
    type=click.Choice(["D", "H"], case_sensitive=False),
    help="Frequency code: D (daily) or H (hourly).",
)
@click.option("--timeout", default=60, show_default=True, type=int, help="HTTP timeout in seconds.")
@click.option("--logdir", type=click.Path(path_type=Path), default=None)
@click.option("--debug", is_flag=True)
@click.option("--quiet", is_flag=True)
@click.help_option("-h", "--help")
def download_cimis_cli(dest_dir, start, end, stations, overwrite, freq, timeout, logdir, debug, quiet):
    """Download daily or hourly CIMIS station data. Requires CIMIS_API_KEY in the environment."""
    level, console = resolve_loglevel(debug=debug, quiet=quiet)
    configure_logging(
        package_name="dms_datastore",
        level=level,
        console=console,
        logdir=logdir,
        logfile_prefix="download_cimis",
    )
    api_key = os.environ.get("CIMIS_API_KEY")
    if not api_key:
        raise click.UsageError("CIMIS_API_KEY environment variable is not set.")
    cimis_download(
        stations=cimis_stations(stations),
        dest_dir=dest_dir,
        api_key=api_key,
        start=start.date() if start else None,
        end=end.date() if end else None,
        overwrite=overwrite,
        timeout=timeout,
        freq=freq,
    )


if __name__ == "__main__":
    download_cimis_cli()
