#!/usr/bin/env python3
"""Download NOAA RRFS (Rapid Refresh Forecast System) GRIB2 forecasts from AWS.

The GSL page https://rapidrefresh.noaa.gov/RRFS/ only shows graphics and blocks
automated scrapers; the model data are published on NOAA's open-data bucket:

    s3://noaa-rrfs-ops-pds/rrfs.YYYYMMDD/HH/rrfs.tHHz.PRODUCT.RES.fFFF.DOMAIN.grib2

Registry: https://registry.opendata.aws/noaa-rrfs-ops/
Product inventory: https://www.nco.ncep.noaa.gov/pmb/products/rrfs/

Use --archive spring|summer|winter for selected retrospective periods in
s3://noaa-rrfs-pds/retro_output_final/ (not a continuous historical archive).
These datasets use prslev files without resolution tokens in their filenames.
Inspect retained dates with --archive summer --list-dates --days-back 60.

Cycles 00/06/12/18 UTC run to 84 h (hourly files f000-f084); other hourly cycles
provide only 18 h sub-hourly products. Products and grids:

    2dfld      surface/2-D fields   3km (conus, ak), 13km (na), 2p5km (hi, pr)
    prslev     pressure levels      3km (conus, ak), 13km (na), 2p5km (hi, pr)
    2dfld.subh sub-hourly 2-D       3km (conus, ak), 2p5km (hi, pr), f001-f018

Full CONUS 3-km files are ~340 MB (2dfld) to ~560 MB (prslev). Use --vars to
download only matching GRIB records via the .idx byte index (HTTP range requests).

f001-f084 from the 00Z cycle, crops to the HRRR lat/lon box, and writes one file
per complete America/Los_Angeles day as OUTPUT/YYYY/rrfs_YYYYMMDD00.nc. The time
coordinate counts days since each file's Pacific midnight; valid_date_pacific identifies the
local date. Local civil time includes PST/PDT daylight-saving transitions.
--sflux reproduces the dms_datastore hrrr3.py schema: it defaults to f001-f084
from the 00Z cycle, crops to the HRRR lat/lon box, and writes one file per complete
America/Los_Angeles day as OUTPUT/YYYY/rrfs_YYYYMMDD00.nc. Its time coordinate
counts days since each file's Pacific midnight, while valid_date_pacific records the local date.
"""

import logging
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import boto3
import click
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError


BUCKET = "noaa-rrfs-ops-pds"
ARCHIVE_BUCKET = "noaa-rrfs-pds"
ARCHIVE_SEASONS = ("spring", "summer", "winter")
DOMAIN_RESOLUTION = {"conus": "3km", "ak": "3km", "na": "13km", "hi": "2p5km", "pr": "2p5km"}
PRODUCTS = ("2dfld", "prslev", "2dfld.subh")
SYNOPTIC_CYCLES = (0, 6, 12, 18)
SCHISM_VARS = (
    r":MSLET:mean sea level:",
    r":UGRD:10 m above ground:",
    r":VGRD:10 m above ground:",
    r":TMP:2 m above ground:",
    r":SPFH:2 m above ground:",
    r":PRATE:surface:(anl|[0-9]+ hour fcst):",
    r":DSWRF:surface:(anl|[0-9]+ hour fcst):",
    r":DLWRF:surface:(anl|[0-9]+ hour fcst):",
)
# Same defaults as dms_datastore download_hrrr.py
HRRR_LAT_BOUNDS = (37.36, 39.0)
HRRR_LON_BOUNDS = (-123.023, -121.16)
# RRFS has no MSLMA (HRRR's prmsl source); MSLET is its sea-level pressure
SFLUX_NAMES = {
    "2t": "stmp", "2sh": "spfh", "10u": "uwind", "10v": "vwind",
    "mslet": "prmsl", "prate": "prate",
    "sdlwrf": "dlwrf", "sdswrf": "dswrf",
    "dlwrf": "dlwrf", "dswrf": "dswrf",
}


def setup_logging(debug: bool = False) -> None:
    """Configure CLI logging."""
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def rrfs_key(date: datetime, cycle: int, hour: int, product: str = "2dfld", domain: str = "conus", archive=None) -> str:
    """Return the S3 key of one RRFS GRIB2 file.

    Parameters
    ----------
    date : datetime
        Cycle date (UTC).
    cycle : int
        Cycle hour, 0-23.
    hour : int
        Forecast lead hour.
    product : {'2dfld', 'prslev', '2dfld.subh'}
        RRFS product.
    domain : {'conus', 'ak', 'na', 'hi', 'pr'}
        Output domain; determines the grid resolution in the file name.
    archive : {'spring', 'summer', 'winter'}, optional
        Retrospective dataset in noaa-rrfs-pds. Requires prslev; archive
        filenames omit the resolution token used by operational files.
    """
    if product not in PRODUCTS:
        raise ValueError(f"product must be one of {PRODUCTS}")
    if domain not in DOMAIN_RESOLUTION:
        raise ValueError(f"domain must be one of {tuple(DOMAIN_RESOLUTION)}")
    if product == "2dfld.subh" and domain == "na":
        raise ValueError("2dfld.subh is not published for the na domain")
    if not 0 <= cycle <= 23:
        raise ValueError("cycle must be between 0 and 23")
    if archive is not None:
        if archive not in ARCHIVE_SEASONS:
            raise ValueError(f"archive must be one of {ARCHIVE_SEASONS}")
        if product != "prslev":
            raise ValueError("Retrospective archives use the prslev product")
        if domain == "na":
            raise ValueError("Retrospective prslev archives do not provide the na domain")
        return (f"retro_output_final/{archive}/rrfs.{date:%Y%m%d}/{cycle:02d}/"
                f"rrfs.t{cycle:02d}z.prslev.f{hour:03d}.{domain}.grib2")
    resolution = DOMAIN_RESOLUTION[domain]
    name_product = "2dfld" if product == "2dfld.subh" else product
    suffix = ".subh" if product == "2dfld.subh" else ""
    filename = f"rrfs.t{cycle:02d}z.{name_product}.{resolution}{suffix}.f{hour:03d}.{domain}.grib2"
    return f"rrfs.{date:%Y%m%d}/{cycle:02d}/{filename}"


def local_path(output_dir: Path, key: str, subset: bool) -> Path:
    """Return where :func:`download_rrfs` saves an S3 key."""
    path = Path(output_dir) / key
    return path.with_name(path.name.replace(".grib2", ".subset.grib2")) if subset else path


def crop_indices(lat, lon, lat_bounds=HRRR_LAT_BOUNDS, lon_bounds=HRRR_LON_BOUNDS):
    """Return (row, column) slices of the smallest index rectangle covering the box.

    Follows hrrr3.HRRR.modified_latlon: points inside the box select a rectangle
    of the native (projected) grid, so corners may extend beyond the box.

    Parameters
    ----------
    lat, lon : ndarray
        2-D grid coordinates; longitudes in either -180..180 or 0..360.
    lat_bounds : pair of float
        South/north limits, default from download_hrrr.py (37.36, 39.0).
    lon_bounds : pair of float
        West/east limits in either convention, default (-123.023, -121.16).
    """
    import numpy as np

    west, east = (value % 360 for value in lon_bounds)
    south, north = lat_bounds
    if not (-90 <= south < north <= 90) or not west < east:
        raise ValueError("Bounds must satisfy SOUTH < NORTH and WEST < EAST")
    longitude = np.asarray(lon) % 360
    inside = (lat >= south) & (lat <= north) & (longitude >= west) & (longitude <= east)
    rows, columns = np.nonzero(inside)
    if not rows.size:
        raise ValueError("No RRFS grid points fall within the latitude/longitude box")
    return slice(int(rows.min()), int(rows.max()) + 1), slice(int(columns.min()), int(columns.max()) + 1)


def read_sflux_fields(grib_path: Path):
    """Read the SCHISM forcing records of one GRIB2 file with eccodes.

    Returns
    -------
    fields : dict of str to ndarray
        sflux name to 2-D (Nj, Ni) float32 values; GRIB missing values are NaN.
    lat, lon : ndarray
        2-D grid coordinates of the records.
    """
    import numpy as np
    from eccodes import codes_get, codes_get_array, codes_get_values, codes_grib_new_from_file, codes_release

    fields = {}
    lat = lon = None
    with open(grib_path, "rb") as stream:
        while True:
            handle = codes_grib_new_from_file(stream)
            if handle is None:
                break
            try:
                name = SFLUX_NAMES.get(codes_get(handle, "shortName"))
                if name is None:
                    continue
                if name in fields:
                    raise ValueError(f"Duplicate {name} record in {grib_path}")
                shape = (codes_get(handle, "Nj"), codes_get(handle, "Ni"))
                values = codes_get_values(handle).astype("float32")
                if codes_get(handle, "bitmapPresent"):
                    values[values == codes_get(handle, "missingValue")] = np.nan
                fields[name] = values.reshape(shape)
                if lat is None:
                    lat = codes_get_array(handle, "latitudes").reshape(shape)
                    lon = codes_get_array(handle, "longitudes").reshape(shape)
            finally:
                codes_release(handle)
    missing = set(SFLUX_NAMES.values()) - set(fields)
    if missing:
        raise ValueError(f"{grib_path} lacks sflux fields: {', '.join(sorted(missing))}")
    return fields, lat, lon


def write_sflux(
    grib_paths, lead_hours, date: datetime, cycle: int, output_path: Path,
    lat_bounds=HRRR_LAT_BOUNDS, lon_bounds=HRRR_LON_BOUNDS, valid_date=None,
    source_bucket=BUCKET, archive=None,
) -> Path:
    """Write cropped RRFS forcing with the hrrr3.py SCHISM sflux NetCDF schema.

    Parameters
    ----------
    grib_paths : sequence of Path
        One GRIB2 file per lead hour containing the SFLUX_NAMES records.
    lead_hours : sequence of int
        Forecast hours of grib_paths; output time is days since local midnight.
    date : datetime
        Cycle date.
    cycle : int
        Cycle hour (UTC).
    output_path : Path
        NetCDF destination, replaced atomically.
    lat_bounds, lon_bounds : pair of float, optional
        Crop box, default from download_hrrr.py.
    valid_date : datetime.date, optional
        Pacific valid date used as the output time origin.
    source_bucket : str, optional
        S3 bucket recorded as source provenance.
    archive : {'spring', 'summer', 'winter'}, optional
        Retrospective dataset recorded as source provenance.

    Returns
    -------
    Path
        output_path.
    """
    import numpy as np
    import xarray as xr
    import pytz

    if len(grib_paths) != len(lead_hours) or not grib_paths:
        raise ValueError("Need one GRIB file per lead hour")
    pacific = pytz.timezone("America/Los_Angeles")
    cycle_start_utc = datetime(date.year, date.month, date.day, cycle, tzinfo=timezone.utc)
    if valid_date is None:
        valid_date = cycle_start_utc.astimezone(pacific).date()
    local_midnight = pacific.localize(datetime.combine(valid_date, datetime.min.time()))
    midnight_utc = local_midnight.astimezone(pytz.UTC)
    valid_times_utc = [cycle_start_utc + timedelta(hours=lead) for lead in lead_hours]
    time_values = np.asarray([
        (valid_time - midnight_utc).total_seconds() / 86400
        for valid_time in valid_times_utc
    ], dtype="float32")
    local_offset = local_midnight.strftime("%z")
    local_offset = f"{local_offset[:3]}:{local_offset[3:]}"
    series = {name: [] for name in SFLUX_NAMES.values()}
    rows = columns = grid_lat = grid_lon = None
    for path in grib_paths:
        fields, lat, lon = read_sflux_fields(path)
        if rows is None:
            rows, columns = crop_indices(lat, lon, lat_bounds, lon_bounds)
            grid_lat = lat[rows, columns].astype("float32")
            grid_lon = lon[rows, columns].astype("float32")
            grid_lon[grid_lon > 180] -= 360
            reference = (lat, lon)
        elif lat.shape != reference[0].shape or not (np.allclose(lat, reference[0]) and np.allclose(lon, reference[1])):
            raise ValueError(f"Grid of {path} differs from {grib_paths[0]}")
        for name, values in fields.items():
            series[name].append(values[rows, columns])
    dims = ["time", "ny_grid", "nx_grid"]
    fout = xr.Dataset(
        {name: (dims, np.array(series[name], dtype="float32")) for name in
         ("stmp", "spfh", "uwind", "vwind", "prmsl", "prate", "dlwrf", "dswrf")},
        coords={
            "time": np.round(time_values, 6),
            "lon": (["ny_grid", "nx_grid"], grid_lon),
            "lat": (["ny_grid", "nx_grid"], grid_lat),
        },
    )
    fout.time.attrs = {
        "long_name": "Time",
        "standard_name": "time",
        "base_date": [valid_date.year, valid_date.month, valid_date.day, 0, 0],
        "units": f"days since {valid_date:%Y-%m-%d} 00:00:00 {local_offset}",
        "timezone": "America/Los_Angeles",
    }
    fout.lat.attrs = {"units": "degrees_north", "long_name": "Latitude", "standard_name": "latitude"}
    fout.lon.attrs = {"units": "degrees_east", "long_name": "Longitude", "standard_name": "longitude"}
    fout.uwind.attrs = {"units": "m/s", "long_name": "10m_above_ground/UGRD", "standard_name": "eastward_wind"}
    fout.vwind.attrs = {"units": "m/s", "long_name": "10m_above_ground/VGRD", "standard_name": "northward_wind"}
    fout.spfh.attrs = {"units": "kg kg-1", "long_name": "2m_above_ground/Specific Humidity", "standard_name": "specific_humidity"}
    fout.prmsl.attrs = {"units": "Pa", "long_name": "Pressure reduced to MSL", "standard_name": "air_pressure_at_sea_level"}
    fout.stmp.attrs = {"units": "K", "long_name": "2m_above_ground/Temperature"}
    fout.prate.attrs = {"units": "kg m-2 s-1", "long_name": "Precipitation rate"}
    fout.dlwrf.attrs = {"units": "W m-2", "long_name": "Downward long-wave radiation flux"}
    fout.dswrf.attrs = {"units": "W m-2", "long_name": "Downward short-wave radiation flux"}
    fout.attrs = {
        "creator_institution": "NOAA",
        "geospatial_lat_min": lat_bounds[0],
        "geospatial_lat_max": lat_bounds[1],
        "geospatial_lon_min": lon_bounds[0],
        "geospatial_lon_max": lon_bounds[1],
        "publisher_institution": "California Department of Water Resources",
        "program": "RRFS",
        "title": "Subsetted forecast atmospheric data for Bay-Delta SCHISM",
        "time_of_preparation": datetime.now().strftime("%Y%m%d"),
        "forecast_start_time_utc": cycle_start_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "valid_date_pacific": valid_date.strftime("%Y-%m-%d") if valid_date else "",
        "valid_timezone": "America/Los_Angeles",
        "source_bucket": source_bucket,
        "source_prefix": f"retro_output_final/{archive}/" if archive else "",
    }
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    partial_path = output_path.with_name(output_path.name + ".part")
    try:
        fout.to_netcdf(partial_path, mode="w", format="NETCDF4", engine="netcdf4", unlimited_dims="time")
        partial_path.replace(output_path)
    finally:
        partial_path.unlink(missing_ok=True)
    logging.info("Saved sflux file %s (%d times, %d x %d grid)", output_path, len(lead_hours), *grid_lat.shape)
    return output_path


def sflux_path(output_dir: Path, date: datetime, cycle: int) -> Path:
    """Return OUTPUT/YYYY/rrfs_YYYYMMDDHH.nc, keyed by Pacific valid date."""
    return Path(output_dir) / f"{date:%Y}" / f"rrfs_{date:%Y%m%d}{cycle:02d}.nc"


def remove_gribs(paths, output_dir: Path) -> None:
    """Delete GRIB files and any directories they leave empty below output_dir."""
    output_dir = Path(output_dir).resolve()
    folders = set()
    for path in paths:
        path.unlink(missing_ok=True)
        folders.add(path.parent.resolve())
    for folder in sorted(folders, key=lambda item: len(item.parts), reverse=True):
        while folder != output_dir and output_dir in folder.parents:
            if not folder.is_dir() or any(folder.iterdir()):
                break
            folder.rmdir()
            folder = folder.parent


def process_sflux_days(args, downloader: "S3Downloader", date: datetime, cycle: int,
                       day_groups, targets, variables) -> bool:
    """Download, convert, and clean up one complete Pacific day at a time.

    Only lead hours belonging to complete Pacific days are downloaded. Each
    day's GRIB subsets are deleted after its sflux file is written, or after
    a failure, unless args.keep_grib is set. Disk use is therefore bounded
    by one day of subsets instead of a whole forecast cycle.

    Returns
    -------
    bool
        True when every Pacific day exists or was written successfully.
    """
    success = True
    for local_date, day_hours in day_groups.items():
        target = targets[local_date]
        if target.is_file() and not args.overwrite and not args.dry_run:
            logging.info("sflux file exists: %s", target)
            continue
        gribs = [
            local_path(args.output, rrfs_key(date, cycle, hour, args.product, args.domain,
                                             archive=args.archive), True)
            for hour in day_hours
        ]
        try:
            if not download_rrfs(
                args.output, downloader, date, cycle, day_hours, args.product, args.domain,
                variables, args.dry_run, args.overwrite, archive=args.archive,
            ):
                logging.error("Skipping Pacific day %s because forecast hours are missing", local_date)
                success = False
                continue
            if args.dry_run:
                continue
            write_sflux(
                gribs, day_hours, date, cycle, target, args.lat_bounds, args.lon_bounds,
                valid_date=local_date, source_bucket=downloader.bucket, archive=args.archive,
            )
        finally:
            if not args.keep_grib and not args.dry_run:
                remove_gribs(gribs, args.output)
    return success


def complete_sflux_days(date: datetime, cycle: int, forecast_hours, timezone_name="America/Los_Angeles"):
    """Group lead hours into fully covered local Pacific calendar days.

    Returns a mapping from local date to lead hours. A local day is emitted
    only when every hourly valid time from midnight to the next midnight is
    present. This naturally handles 23/25-hour daylight-saving transition days.
    """
    from datetime import time
    import pytz

    pacific = pytz.timezone(timezone_name)
    cycle_start = pytz.UTC.localize(datetime(date.year, date.month, date.day, cycle))
    grouped = {}
    for lead in sorted(set(forecast_hours)):
        valid_utc = cycle_start + timedelta(hours=lead)
        local_date = valid_utc.astimezone(pacific).date()
        grouped.setdefault(local_date, set()).add(lead)

    complete = {}
    for local_date, available in grouped.items():
        local_start = pacific.localize(datetime.combine(local_date, time.min))
        local_end = pacific.localize(datetime.combine(local_date + timedelta(days=1), time.min))
        start_utc = local_start.astimezone(timezone.utc)
        end_utc = local_end.astimezone(timezone.utc)
        first_lead = int((start_utc - cycle_start).total_seconds() // 3600)
        hours_in_day = int((end_utc - start_utc).total_seconds() // 3600)
        required = set(range(first_lead, first_lead + hours_in_day))
        if required and required.issubset(available):
            complete[local_date] = sorted(required)
    return dict(sorted(complete.items()))


def select_byte_ranges(index_text: str, file_size: int, patterns):
    """Return merged (start, end) byte ranges of GRIB records matching any pattern.

    Parameters
    ----------
    index_text : str
        Contents of the wgrib2-style .idx file (``n:offset:d=...:VAR:LEVEL:FCST:``).
    file_size : int
        Size of the GRIB2 file in bytes; bounds the final record.
    patterns : sequence of str
        Regular expressions searched against each index line.

    Returns
    -------
    ranges : list of (int, int)
        Inclusive byte ranges, adjacent records merged.
    unmatched : list of str
        Patterns that matched no record.
    """
    entries = []
    for line in index_text.splitlines():
        parts = line.split(":", 2)
        if len(parts) == 3 and parts[1].isdigit():
            entries.append((int(parts[1]), line))
    if not entries:
        raise ValueError("GRIB index contains no records")
    compiled = [re.compile(pattern) for pattern in patterns]
    matched = set()
    ranges = []
    for position, (offset, line) in enumerate(entries):
        hits = {pattern.pattern for pattern in compiled if pattern.search(line)}
        if not hits:
            continue
        matched |= hits
        end = entries[position + 1][0] - 1 if position + 1 < len(entries) else file_size - 1
        if ranges and ranges[-1][1] + 1 == offset:
            ranges[-1] = (ranges[-1][0], end)
        else:
            ranges.append((offset, end))
    unmatched = [pattern.pattern for pattern in compiled if pattern.pattern not in matched]
    return ranges, unmatched


def parse_grib_index(index_text: str):
    """Parse a wgrib2 index into record number, variable, level, and forecast.

    Parameters
    ----------
    index_text : str
        Contents of an RRFS ``.idx`` file.

    Returns
    -------
    list of dict
        Parsed records, retaining the variable/level/time detail for each GRIB
        message instead of collapsing fields that occur at multiple levels.
    """
    records = []
    for line in index_text.splitlines():
        parts = line.split(":", 5)
        if len(parts) < 6 or not parts[0].isdigit() or not parts[1].isdigit():
            continue
        records.append({
            "record": int(parts[0]),
            "offset": int(parts[1]),
            "reference_time": parts[2],
            "variable": parts[3],
            "level": parts[4],
            "forecast": parts[5].rstrip(":"),
        })
    return records


def list_rrfs_variables(
    s3_downloader: "S3Downloader", date: datetime, cycle: int, hour: int,
    product: str = "2dfld", domain: str = "conus", archive=None,
):
    """Return the full GRIB-record inventory for one RRFS forecast file.

    The inventory is read from the small adjacent ``.idx`` file; the large
    forecast GRIB2 object itself is not downloaded.
    Optional archive selects the spring, summer, or winter retrospective layout.
    """
    key = rrfs_key(date, cycle, hour, product, domain, archive=archive)
    if s3_downloader.object_size(key) is None:
        raise FileNotFoundError(f"RRFS file not found: {s3_downloader.bucket}/{key}")
    records = parse_grib_index(s3_downloader.read_text(key + ".idx"))
    if not records:
        raise ValueError(f"No GRIB records found in index for {key}")
    return key, records


class S3Downloader:
    """Access NOAA's public RRFS bucket without AWS credentials."""

    def __init__(self, bucket: str = BUCKET):
        self.bucket = bucket
        self._s3_client = None

    @property
    def s3(self):
        """Lazily create an unsigned S3 client."""
        if self._s3_client is None:
            self._s3_client = boto3.client(
                "s3", config=Config(signature_version=UNSIGNED, region_name="us-east-1")
            )
        return self._s3_client

    def object_size(self, key: str):
        """Return the object size in bytes, or None when it does not exist."""
        try:
            return self.s3.head_object(Bucket=self.bucket, Key=key)["ContentLength"]
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
                return None
            raise

    def read_text(self, key: str) -> str:
        """Return a small text object such as a GRIB index."""
        return self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read().decode()

    def download(self, key: str, output_path: Path, ranges=None) -> None:
        """Download a whole object or byte ranges to a temporary file, then rename it."""
        partial_path = output_path.with_name(output_path.name + ".part")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            if ranges is None:
                self.s3.download_file(self.bucket, key, str(partial_path))
            else:
                with open(partial_path, "wb") as stream:
                    for start, end in ranges:
                        body = self.s3.get_object(Bucket=self.bucket, Key=key, Range=f"bytes={start}-{end}")["Body"]
                        stream.write(body.read())
            partial_path.replace(output_path)
        finally:
            partial_path.unlink(missing_ok=True)


def download_rrfs(
    output_dir: Path,
    s3_downloader: S3Downloader,
    date: datetime,
    cycle: int,
    forecast_hours,
    product: str = "2dfld",
    domain: str = "conus",
    variables=None,
    dry_run: bool = False,
    overwrite: bool = False,
    archive=None,
) -> bool:
    """Download one RRFS cycle's forecast hours, optionally only selected variables.

    Parameters
    ----------
    output_dir : Path
        Files mirror the source key under output_dir, including the archive
        prefix when selecting a retrospective dataset.
    s3_downloader : S3Downloader
        Public S3 downloader.
    date : datetime
        Cycle date (UTC).
    cycle : int
        Cycle hour (UTC). Only 00/06/12/18 run beyond 18 h.
    forecast_hours : iterable of int
        Lead hours to download.
    product, domain : str, optional
        See :func:`rrfs_key`.
    variables : sequence of str, optional
        Regular expressions matched against .idx lines (for example
        ``":TMP:2 m above ground:"``). Matching records are fetched by byte range
        and saved as ``*.subset.grib2``. None downloads complete files.
    dry_run : bool, optional
        Report selections and sizes without downloading.
    overwrite : bool, optional
        Download again even when an existing file has the expected size.
    archive : {'spring', 'summer', 'winter'}, optional
        Retrospective dataset, requiring the prslev product.

    Returns
    -------
    bool
        True when every requested file exists and every variable pattern matched.
    """
    success = True
    total_bytes = 0
    for hour in forecast_hours:
        key = rrfs_key(date, cycle, hour, product, domain, archive=archive)
        size = s3_downloader.object_size(key)
        if size is None:
            logging.warning("Not found: s3://%s/%s", s3_downloader.bucket, key)
            success = False
            continue
        ranges = None
        expected = size
        output_path = local_path(output_dir, key, bool(variables))
        if variables:
            ranges, unmatched = select_byte_ranges(s3_downloader.read_text(key + ".idx"), size, variables)
            if unmatched:
                logging.error("No GRIB records in %s match: %s", key, ", ".join(unmatched))
                success = False
                continue
            expected = sum(end - start + 1 for start, end in ranges)
        total_bytes += expected
        if dry_run:
            logging.info("s3://%s/%s -> %s (%.1f MB)", s3_downloader.bucket, key, output_path.name, expected / 1e6)
            continue
        if not overwrite and output_path.is_file() and output_path.stat().st_size == expected:
            logging.info("Already downloaded: %s", output_path)
            continue
        try:
            logging.info("Downloading %s (%.1f MB)", key, expected / 1e6)
            s3_downloader.download(key, output_path, ranges)
            if output_path.stat().st_size != expected:
                raise OSError(f"Downloaded size does not match expected {expected} bytes")
        except (BotoCoreError, ClientError, OSError) as error:
            logging.error("Download failed for %s: %s", key, error)
            output_path.unlink(missing_ok=True)
            success = False
    logging.info("rrfs.%s/%02d: %.1f MB selected", f"{date:%Y%m%d}", cycle, total_bytes / 1e6)
    return success


def list_available_dates(s3_downloader: S3Downloader, days_back: int = 7, archive=None) -> None:
    """Log retained cycle dates in the operational or seasonal archive prefix."""
    dates = set()
    prefix = f"retro_output_final/{archive}/" if archive else ""
    paginator = s3_downloader.s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=s3_downloader.bucket, Prefix=prefix + "rrfs.", Delimiter="/"):
        for entry in page.get("CommonPrefixes", []):
            match = re.fullmatch(re.escape(prefix) + r"rrfs\.(\d{8})/", entry["Prefix"])
            if match:
                dates.add(match.group(1))
    if not dates:
        logging.warning("No RRFS dates found")
    for date in sorted(dates, reverse=True)[:days_back]:
        logging.info("%s", datetime.strptime(date, "%Y%m%d").date())


def parse_hours(value: str) -> range:
    """Parse a single hour or inclusive START:END[:STRIDE] range."""
    try:
        parts = [int(part) for part in value.split(":")]
        if len(parts) == 1:
            parts = [parts[0], parts[0], 1]
        elif len(parts) == 2:
            parts.append(1)
        if len(parts) != 3 or parts[0] < 0 or parts[1] < parts[0] or parts[2] < 1:
            raise ValueError
        return range(parts[0], parts[1] + 1, parts[2])
    except ValueError as error:
        raise click.BadParameter(
            "Use HOUR or START:END[:STRIDE] with END >= START >= 0"
        ) from error


@click.command(
    context_settings={"help_option_names": ["-h", "--help"]},
    help="Download NOAA RRFS GRIB2 forecasts from public AWS S3 (no credentials required).",
    epilog="""Examples:
  python download_rrfs.py -d 2026-10-13 -c 12 -fh 0:24 --dry-run
  python download_rrfs.py -d 2026-10-13 -c 0 -c 6 -c 12 -c 18 -fh 0:84 -o ./data
  python download_rrfs.py -d 2026-10-13 -c 12 --product prslev --domain na -fh 0:12:3
  python download_rrfs.py -d 2026-10-13 -c 12 -fh 0:48 --vars ":TMP:2 m above ground:" --vars ":UGRD:10 m above ground:"
    python download_rrfs.py -d 2026-10-13 -c 12 --list-vars
    python download_rrfs.py -d 2026-10-13 -c 12 -fh 6 --list-vars
  python download_rrfs.py -sdate 2026-10-10 -edate 2026-10-13 -c 0 -fh 0:24 --schism-vars -o ./data
  python download_rrfs.py --list-dates
    python download_rrfs.py --archive summer --list-dates --days-back 60
    python download_rrfs.py --archive summer -d 2023-07-17 --sflux -o ./rrfs_retro
    python download_rrfs.py --archive spring -d 2024-05-03 -c 0 -fh 1 --list-vars
    python download_rrfs.py -d 2026-10-16 --sflux -o ./rrfs_sflux
    python download_rrfs.py -d 2026-10-16 --sflux -fh 1:84 --lat-bounds 37 39 --lon-bounds -124 -121

Cycles 00/06/12/18 UTC run to f084; other cycles only provide 2dfld.subh to f018.
--vars uses regular expressions on .idx lines (VAR:LEVEL:TIME); matching records are
saved as *.subset.grib2. --schism-vars selects MSLET, 10 m U/V, 2 m TMP/SPFH,
instantaneous PRATE, DSWRF and DLWRF. Output: OUTPUT/rrfs.YYYYMMDD/HH/.
--list-vars prints all indexed records (variable, level, forecast time) for the
selected cycle/hour without downloading the GRIB2 file. Default listing hour is f000
(f001 for 2dfld.subh); use -fh to select another hour or range.

--sflux defaults to f001-f084 (84 hours) from the 00Z cycle. It writes one file for
each complete America/Los_Angeles calendar day covered by the forecast, e.g. a
2026-10-16 run produces rrfs_2026101600.nc, rrfs_2026101700.nc, and
rrfs_2026101800.nc. The NetCDF time coordinate counts days since the file's local
midnight; valid_date_pacific records each file's Pacific date. DST is honored.
Use --records 1, 2, or 3 to limit the forecast to that many 24-hour lead blocks.
GRIB subsets are deleted after each Pacific day is converted (or fails) unless
--keep-grib is given, so at most one day of subsets is stored at a time. Without
--sflux, no cropping or cleanup is performed; without --vars/--schism-vars, complete
~340 MB CONUS files are kept.

--archive selects noaa-rrfs-pds and its retrospective prslev products. It is not
a continuous historical archive: currently summer covers July 2023, winter covers
January 8-February 8, 2024, and spring covers May 2-31, 2024. Some cycles are absent.
Use --list-dates and --list-vars to inspect availability; dates are never replaced
by another run. In archive mode --sflux selects the same forcing fields from prslev.
""",
)
@click.option("-d", "--date", type=click.DateTime(formats=["%Y-%m-%d"]),
              help="Cycle date (YYYY-MM-DD; default: today UTC)")
@click.option("-sdate", "--start", "start", type=click.DateTime(formats=["%Y-%m-%d"]),
              help="First cycle date (inclusive)")
@click.option("-edate", "--end", "end", type=click.DateTime(formats=["%Y-%m-%d"]),
              help="Last cycle date (inclusive)")
@click.option("-c", "--cycles", "cycles", multiple=True, type=int,
              help="Cycle hour UTC; repeat -c for multiple cycles (default: 12, or 0 with --sflux)")
@click.option("-fh", "--forecast-hours", type=click.UNPROCESSED, callback=(
    lambda ctx, param, value: parse_hours(value) if value is not None else None
), help="Lead hours HOUR or START:END[:STRIDE] (default: 0:84, or 1:84 with --sflux)")
@click.option("--product", type=click.Choice(PRODUCTS),
              help="RRFS product (default: 2dfld; prslev with --archive)")
@click.option("--domain", type=click.Choice(tuple(DOMAIN_RESOLUTION)), default="conus",
              show_default=True, help="Domain")
@click.option("--vars", "variables", multiple=True,
              help="Regex pattern of GRIB records to download; repeat for multiple patterns")
@click.option("--schism-vars", is_flag=True,
              help="Download atmospheric forcing fields used by SCHISM sflux")
@click.option("--sflux", is_flag=True,
              help="Write HRRR-style SCHISM sflux NetCDF (hrrr3 schema) cropped to the lat/lon box")
@click.option("--records", type=int,
              help="Limit --sflux to the first 1-3 blocks of 24 forecast hours (default: all 84 hours)")
@click.option("--lat-bounds", type=(float, float), default=HRRR_LAT_BOUNDS,
              metavar="SOUTH NORTH", show_default=True, help="--sflux latitude box")
@click.option("--lon-bounds", type=(float, float), default=HRRR_LON_BOUNDS,
              metavar="WEST EAST", show_default=True, help="--sflux longitude box")
@click.option("--keep-grib", is_flag=True,
              help="Keep downloaded GRIB subsets after each --sflux day is converted")
@click.option("-o", "--output", type=click.Path(path_type=Path), default=Path("./rrfs_data"),
              show_default=True, help="Output directory")
@click.option("--archive", type=click.Choice(ARCHIVE_SEASONS),
              help="Retrospective dataset in noaa-rrfs-pds; selected 2023/2024 periods, not continuous history")
@click.option("--bucket", help=f"Override S3 bucket (default: {BUCKET}; {ARCHIVE_BUCKET} with --archive)")
@click.option("--list-dates", is_flag=True, help="List recent cycle dates and exit")
@click.option("--list-vars", is_flag=True,
              help="List variable/level/forecast records from RRFS .idx files; does not download GRIB data")
@click.option("--days-back", type=int, default=7, show_default=True,
              help="Number of dates to list")
@click.option("--dry-run", is_flag=True, help="Show selected files and sizes without downloading")
@click.option("--overwrite", is_flag=True, help="Download again even if a complete file exists")
@click.option("--debug", is_flag=True, help="Enable debug logging")
def main(date, start, end, cycles, forecast_hours, product, domain, variables,
         schism_vars, sflux, records, lat_bounds, lon_bounds, keep_grib, output,
         archive, bucket, list_dates, list_vars, days_back, dry_run, overwrite, debug) -> int:
    """CLI entry point."""
    args = SimpleNamespace(
        date=date, start=start, end=end, cycles=cycles or None,
        forecast_hours=forecast_hours, product=product, domain=domain,
        vars=variables or None, schism_vars=schism_vars, sflux=sflux,
        records=records, lat_bounds=lat_bounds, lon_bounds=lon_bounds,
        keep_grib=keep_grib, output=output, archive=archive, bucket=bucket,
        list_dates=list_dates, list_vars=list_vars, days_back=days_back,
        dry_run=dry_run, overwrite=overwrite, debug=debug,
    )
    args.product = args.product or ("prslev" if args.archive else "2dfld")
    args.bucket = args.bucket or (ARCHIVE_BUCKET if args.archive else BUCKET)
    if args.bucket == ARCHIVE_BUCKET and args.archive is None:
        raise click.UsageError("Use --archive spring, summer, or winter for the noaa-rrfs-pds retrospective layouts")
    if args.archive and args.product != "prslev":
        raise click.UsageError("--archive requires --product prslev (selected automatically by default)")
    if args.archive and args.domain == "na":
        raise click.UsageError("Retrospective prslev archives do not provide the na domain")
    if bool(args.start) != bool(args.end):
        raise click.UsageError("--start and --end must be supplied together")
    if args.date and args.start:
        raise click.UsageError("Use --date or --start/--end, not both")
    if args.start and args.start > args.end:
        raise click.UsageError("Start date cannot be after end date")
    if args.product == "2dfld.subh" and args.domain == "na":
        raise click.UsageError("2dfld.subh is not published for the na domain")
    if args.list_vars and (args.sflux or args.vars or args.schism_vars or args.dry_run or args.keep_grib):
        raise click.UsageError("--list-vars is an inventory-only mode; do not combine it with download/subset options")
    if args.list_vars and args.list_dates:
        raise click.UsageError("Use --list-vars or --list-dates, not both")
    if args.sflux:
        if args.vars or args.product != ("prslev" if args.archive else "2dfld"):
            raise click.UsageError("--sflux uses its own variable set and requires 2dfld (or prslev with --archive)")
        if args.records is not None and not 1 <= args.records <= 3:
            raise click.UsageError("--records must be 1, 2 or 3 (RRFS runs to f084)")
        if args.records is not None and args.forecast_hours is not None:
            raise click.UsageError("Use --records or --forecast-hours with --sflux, not both")
    elif args.records is not None:
        raise click.UsageError("--records is only valid with --sflux")
    cycles = args.cycles or ([0] if args.sflux else [12])
    if any(not 0 <= cycle <= 23 for cycle in cycles):
        raise click.UsageError("Cycle hours must be between 0 and 23")
    if args.forecast_hours is not None:
        forecast_hours = args.forecast_hours
    elif args.sflux:
        forecast_hours = range(1, (24 * args.records if args.records is not None else 84) + 1)
    else:
        forecast_hours = range(0, 85)

    setup_logging(args.debug)
    downloader = S3Downloader(args.bucket)
    variables = SCHISM_VARS if (args.schism_vars or args.sflux) else args.vars
    if not args.sflux and not args.list_vars and not args.list_dates:
        logging.warning("Without --sflux, GRIB files are neither cropped nor deleted%s",
                        "; complete ~340 MB CONUS files will be kept" if variables is None else "")
    try:
        if args.list_dates:
            list_available_dates(downloader, args.days_back, archive=args.archive)
            return 0
        date = args.start or args.date or datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
        end_date = args.end or date
        if args.list_vars:
            inventory_hours = (
                list(args.forecast_hours)
                if args.forecast_hours is not None
                else [1 if args.product == "2dfld.subh" else 0]
            )
            while date <= end_date:
                for cycle in sorted(set(cycles)):
                    for hour in inventory_hours:
                        key, records = list_rrfs_variables(
                            downloader, date, cycle, hour, args.product, args.domain,
                            archive=args.archive,
                        )
                        print(f"\nFile: s3://{downloader.bucket}/{key}")
                        print(f"Records: {len(records)}; variables: {len({record['variable'] for record in records})}")
                        print(f"{'No.':>4}  {'Variable':<28} {'Level':<48} Forecast")
                        for record in records:
                            print(
                                f"{record['record']:4d}  {record['variable']:<28} "
                                f"{record['level']:<48} {record['forecast']}"
                            )
                date += timedelta(days=1)
            return 0
        success = True
        while date <= end_date:
            for cycle in sorted(set(cycles)):
                hours = list(forecast_hours)
                logging.info("Requested initialization: %s %02dZ; bucket=%s; archive=%s",
                             date.strftime("%Y-%m-%d"), cycle, downloader.bucket, args.archive or "operational")
                if args.product != "2dfld.subh" and cycle not in SYNOPTIC_CYCLES and max(hours) > 18:
                    logging.warning("Cycle %02dz has no %s output; use cycles 0 6 12 18 or --product 2dfld.subh", cycle, args.product)
                if args.product == "2dfld.subh":
                    hours = [hour for hour in hours if 1 <= hour <= 18]
                day_groups = complete_sflux_days(date, cycle, hours) if args.sflux else None
                if args.sflux and not day_groups:
                    logging.warning("Forecast hours do not cover a complete Pacific day for %s %02dz", date.date(), cycle)
                    success = False
                    continue
                if day_groups is not None:
                    targets = {
                        local_date: sflux_path(args.output, datetime.combine(local_date, datetime.min.time()), cycle)
                        for local_date in day_groups
                    }
                    if all(path.is_file() for path in targets.values()) and not args.overwrite and not args.dry_run:
                        for path in targets.values():
                            logging.info("sflux file exists: %s", path)
                        continue
                if not args.sflux:
                    if not download_rrfs(
                        args.output, downloader, date, cycle, hours, args.product, args.domain,
                        variables, args.dry_run, args.overwrite, archive=args.archive,
                    ):
                        success = False
                    continue
                if not process_sflux_days(args, downloader, date, cycle, day_groups, targets, variables):
                    success = False
            date += timedelta(days=1)
        logging.info("%s", "Completed successfully" if success else "Some files were missing or failed")
        if not success:
            raise click.exceptions.Exit(1)
        return 0
    except (BotoCoreError, ClientError, OSError, ValueError) as error:
        logging.error("RRFS download failed: %s", error)
        raise click.exceptions.Exit(1) from error


if __name__ == "__main__":
    raise SystemExit(main())
