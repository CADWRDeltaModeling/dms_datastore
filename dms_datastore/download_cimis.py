"""Download daily CIMIS station data and maintain one CSV per station."""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

API_URL = "https://et.water.ca.gov/StationWeb/GetDataByStationNumber"
DATA_ITEMS = (
    "day-asce-eto,day-precip,day-sol-rad-avg,day-vap-pres-avg,"
    "day-air-tmp-max,day-air-tmp-min,day-air-tmp-avg,day-rel-hum-max,"
    "day-rel-hum-min,day-rel-hum-avg,day-dew-pnt,day-wind-spd-avg,"
    "day-wind-run,day-soil-tmp-avg"
)
OUTPUT_DATE_FIELD = "Date"


class RequestTimeoutError(RuntimeError):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--stations",
        type=Path,
        default=Path(__file__).with_name("cimis_active_stations_20260825.csv"),
        help="Active CIMIS station CSV.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).with_name("cimis_downloaded"),
        help="Directory for station CSV files.",
    )
    parser.add_argument(
        "--station",
        action="append",
        type=int,
        help="Download only this station number; may be repeated.",
    )
    parser.add_argument(
        "--start-date",
        type=date.fromisoformat,
        help="Override the computed start date (Connect date or last saved date).",
    )
    parser.add_argument(
        "--end-date",
        type=date.fromisoformat,
        help="Override yesterday's date, useful for testing.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="HTTP timeout in seconds (default: 60).",
    )
    parser.add_argument(
        "--chunk-days",
        type=int,
        default=5000,
        help="Maximum days per API request (default: 5000; maximum: 5020).",
    )
    return parser.parse_args()


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
) -> list[dict[str, str]]:
    parameters = {
        "stationNbrs": str(station_number),
        "startDate": start_date.isoformat(),
        "endDate": end_date.isoformat(),
        "isHourly": "false",
        "unitOfMeasure": "E",
        "dataItems": DATA_ITEMS,
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


def read_existing(path: Path) -> tuple[list[dict[str, str]], date | None]:
    if not path.exists():
        return [], None
    with path.open(newline="", encoding="utf-8-sig") as handle:
        rows = list(csv.DictReader(handle))
    dates = [date.fromisoformat(row[OUTPUT_DATE_FIELD][:10]) for row in rows if row.get(OUTPUT_DATE_FIELD)]
    return rows, max(dates) if dates else None


def write_records(path: Path, rows: list[dict[str, str]]) -> None:
    fields = sorted({field for row in rows for field in row})
    fields.remove(OUTPUT_DATE_FIELD)
    fields.insert(0, OUTPUT_DATE_FIELD)
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", newline="", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        temporary_path = Path(handle.name)
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(sorted(rows, key=lambda row: row[OUTPUT_DATE_FIELD]))
    temporary_path.replace(path)


def process_station(
    station: dict[str, str],
    output_dir: Path,
    start_date_override: date | None,
    end_date: date,
    api_key: str,
    timeout: int,
    chunk_days: int,
) -> str:
    station_number = int(station["Station_Number"])
    output_path = output_dir / f"station_{station_number}.csv"
    existing, latest_date = read_existing(output_path)
    if start_date_override:
        start_date = start_date_override
    else:
        start_date = (latest_date + timedelta(days=1)) if latest_date else date.fromisoformat(station["Connect"][:10])
    if start_date > end_date:
        return f"{station_number}: up to date through {latest_date}"

    by_date = {row[OUTPUT_DATE_FIELD]: row for row in existing}
    total_records = 0
    chunk_start = start_date
    current_chunk_days = chunk_days
    while chunk_start <= end_date:
        chunk_end = min(
            chunk_start + timedelta(days=current_chunk_days - 1), end_date
        )
        try:
            records = request_records(
                station_number, chunk_start, chunk_end, api_key, timeout
            )
        except RequestTimeoutError:
            attempted_days = (chunk_end - chunk_start).days + 1
            if attempted_days == 1:
                raise
            current_chunk_days = max(1, attempted_days // 2)
            print(
                f"{station_number}: request timed out; retrying from "
                f"{chunk_start} with {current_chunk_days}-day chunks"
            )
            continue
        by_date.update({row[OUTPUT_DATE_FIELD]: row for row in records})
        if records:
            write_records(output_path, list(by_date.values()))
        total_records += len(records)
        print(
            f"{station_number}: downloaded {len(records)} rows "
            f"({chunk_start} to {chunk_end})"
        )
        chunk_start = chunk_end + timedelta(days=1)
    return f"{station_number}: complete, downloaded {total_records} rows"


def main() -> int:
    args = parse_args()
    api_key = os.environ.get("CIMIS_API_KEY")
    if not api_key:
        print("CIMIS_API_KEY is not set in this PowerShell session.", file=sys.stderr)
        return 2
    if not 1 <= args.chunk_days <= 5020:
        print("--chunk-days must be between 1 and 5020.", file=sys.stderr)
        return 2

    end_date = args.end_date or (date.today() - timedelta(days=1))
    with args.stations.open(newline="", encoding="utf-8-sig") as handle:
        stations = list(csv.DictReader(handle))
    selected = set(args.station or [])
    if selected:
        stations = [station for station in stations if int(station["Station_Number"]) in selected]

    failures = 0
    for station in stations:
        try:
            print(
                process_station(
                    station,
                    args.output_dir,
                    args.start_date,
                    end_date,
                    api_key,
                    args.timeout,
                    args.chunk_days,
                )
            )
        except (KeyError, ValueError, RuntimeError, OSError) as error:
            failures += 1
            print(f"{station.get('Station_Number', '?')}: ERROR: {error}", file=sys.stderr)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
