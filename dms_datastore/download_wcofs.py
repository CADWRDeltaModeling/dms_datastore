#!/usr/bin/env python3
"""
Download NOAA WCOFS and RTOFS forecast data from Amazon S3.

WCOFS (West Coast Operational Forecast System): US West Coast forecasts
RTOFS (Real-Time Ocean Forecast System): Global ocean forecasts

WCOFS NetCDF downloads use the regular-grid files
wcofs/netcdf/YYYY/MM/DD/wcofs.tCCz.YYYYMMDD.regulargrid.fNNN.nc (NNN = 003..072,
every 3 hours from the cycle start; each file's internal time is used). The previous
day's cycle is also downloaded, files are cropped to the latitude/longitude box used
by download_rtofs.py, and complete fixed-PST days are interpolated hourly into
OUTPUT/wcofs/processed/wcofs_interpolated_hourly_pstYYYYMMDD.nc.
For each downloaded WCOFS forecast run, NAVD88 surface elevations at the
configured UTM 10N points are written to OUTPUT/wcofs/csv/
forecast_wcofs_YYYYMMDD_tCCz.csv using lead files f003 through f072.
Use --add-vel to ensure regular-grid NetCDF subsets include velocity fields.
Datum-grid matching uses the same locally installed horizontal projection for
both grids, without remote PROJ downloads; offsets must be within 10 km.
The --nowcast-elevation option downloads WCOFS 2ds.nNNN files, crops them to
the same box, and combines their elevation and grid data into one hourly
fixed-PST file for each complete Pacific day.
The --native-grid option instead downloads unprocessed ROMS forecast fields
from the noaa-ofs-pds archive, including forecasts starting 2021-03-23.

References:
    - WCOFS S3: https://noaa-nos-ofs-pds.s3.amazonaws.com/wcofs/
    - RTOFS S3: https://noaa-nos-ofs-pds.s3.amazonaws.com/rtofs/ (check availability)
    - AWS S3 Explorer: https://noaa-nos-ofs-pds.s3.amazonaws.com/index.html
    - NOAA Open Data: https://www.noaa.gov/organization/information-technology/open-data-dissemination
"""

import os
import re
import shutil
import sys
import warnings
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from urllib.request import urlopen
import argparse
import logging
import boto3
import click
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
try:
    from dms_datastore.logging_config import configure_logging, resolve_loglevel
except ModuleNotFoundError:  # pragma: no cover - supports direct script execution
    from logging_config import configure_logging, resolve_loglevel


BUCKET = "noaa-nos-ofs-pds"
WCOFS_ARCHIVE_BUCKET = "noaa-ofs-pds"
WCOFS_ARCHIVE_RENAME = datetime(2024, 9, 17)
LAT_LON_BOUNDS = {"lat": (37, 39), "lon": (236, 239)}
NETCDF_STEPS = range(3, 73, 3)
NOWCAST_STEPS = range(1, 25)
NETCDF_NAME = re.compile(r"wcofs\.t(\d{2})z\.(\d{8})\.regulargrid\.f(\d{3})\.nc$")
WCOFS_POINT_COORDS_UTM10N = (
    (508879.733, 4157870.920),
    (549801.682, 4139846.580),
    (503429.325, 4204146.620),
)
_WCOFS_DATUM_GRID_NAME = "wcofs_vdatums.nc"
_WCOFS_DATUM_GRID_URL = (
    "https://noaa-nos-ofs-pds.s3.amazonaws.com/"
    "OFS_Grid_Datum/wcofs_vdatums.nc"
)
logger = logging.getLogger("dms_datastore.download_wcofs" if __name__ == "__main__" else __name__)


def _wcofs_key(date, round, product, step, file_format):
    extension = "nc" if file_format == "netcdf" else "grib2"
    filename = f"wcofs.t{round:02d}z.{date:%Y%m%d}.{product}{step:03d}.{extension}"
    return f"wcofs/{file_format}/{date:%Y/%m/%d}/{filename}"


def _wcofs_native_key(date, round, step):
    if date.date() < WCOFS_ARCHIVE_RENAME.date():
        filename = f"nos.wcofs.fields.f{step:03d}.{date:%Y%m%d}.t{round:02d}z.nc"
    else:
        filename = f"wcofs.t{round:02d}z.{date:%Y%m%d}.fields.f{step:03d}.nc"
    return f"wcofs.{date:%Y%m%d}/{filename}"


def _rtofs_key(date, round, file_format):
    extension = "nc" if file_format == "netcdf" else "grb2"
    return f"rtofs/{file_format}/{date:%Y/%m/%d}/rtofs.t{round:02d}z.{date:%Y%m%d}.{extension}"


def _nowcast_source_files(date):
    return (
        [(date + timedelta(days=1), step) for step in range(5, 25)]
        + [(date + timedelta(days=2), step) for step in range(1, 5)]
    )


def plan_downloads(downloader, forecast_type, date, file_format, forecast_steps, nowcast_elevation,
                   native_grid=False):
    """Report source objects and sizes without downloading or writing files."""
    bucket = WCOFS_ARCHIVE_BUCKET if native_grid else BUCKET
    if native_grid:
        steps = NETCDF_STEPS if forecast_steps is None else forecast_steps
        keys = [_wcofs_native_key(date, 3, step) for step in steps]
    elif forecast_type == "rtofs":
        keys = [_rtofs_key(date, 3, file_format)]
    elif nowcast_elevation:
        keys = [_wcofs_key(source_date, 3, "2ds.n", step, "netcdf")
                for source_date, step in _nowcast_source_files(date)]
    else:
        steps = forecast_steps if forecast_steps is not None else (
            NETCDF_STEPS if file_format == "netcdf" else range(1, 2)
        )
        product = "regulargrid.f" if file_format == "netcdf" else "2ds.f"
        keys = [_wcofs_key(date, 3, product, step, file_format) for step in steps]
    success = True
    for key in keys:
        size = downloader.object_size(bucket, key)
        if size is None:
            logger.warning("File not found on S3: s3://%s/%s", bucket, key)
            success = False
        else:
            logger.info("Would download s3://%s/%s (%.1f MiB)", bucket, key, size / 1024**2)
    return success


def download_wcofs_native(output_dir, s3_downloader, date, round=3,
                          forecast_steps=None, overwrite=False):
    """Download full native ROMS forecast fields without cropping or processing."""
    steps = list(NETCDF_STEPS if forecast_steps is None else forecast_steps)
    if not steps or any(step not in NETCDF_STEPS for step in steps):
        raise ValueError("Native-grid forecast steps must be lead hours 3, 6, ..., 72")
    output_dir = Path(output_dir) / "native"
    success = True
    for step in steps:
        key = _wcofs_native_key(date, round, step)
        try:
            size = s3_downloader.object_size(WCOFS_ARCHIVE_BUCKET, key)
            if size is None:
                logger.warning("File not found on S3: s3://%s/%s", WCOFS_ARCHIVE_BUCKET, key)
                success = False
                continue
            output_path = output_dir / Path(key).name
            if output_path.is_file() and not overwrite:
                if output_path.stat().st_size == size:
                    logger.info("Already available: %s", output_path)
                    continue
                logger.warning("Existing file has the wrong size; rebuilding: %s", output_path)
            output_dir.mkdir(parents=True, exist_ok=True)
            with TemporaryDirectory(prefix="wcofs_native_", dir=output_dir) as directory:
                source_path = Path(directory) / output_path.name
                if not s3_downloader.download_file(WCOFS_ARCHIVE_BUCKET, key, source_path):
                    success = False
                    continue
                if source_path.stat().st_size != size:
                    raise ValueError(f"Downloaded size does not match S3 object: {key}")
                source_path.replace(output_path)
            logger.info("Saved native-grid forecast to: %s", output_path)
        except (BotoCoreError, ClientError, OSError, ValueError) as error:
            logger.error("Native-grid download failed for %s: %s", key, error)
            success = False
    return success


def setup_logging(debug: bool = False, quiet: bool = False, logdir: Path = Path("logs")) -> None:
    """Configure logging."""
    level, console = resolve_loglevel(debug=debug, quiet=quiet)
    configure_logging(
        package_name="dms_datastore",
        level=level,
        console=console,
        logdir=Path(logdir) if logdir is not None else None,
        logfile_prefix="download_wcofs",
    )


class S3Downloader:
    """Download forecast data from NOAA's public S3 bucket."""
    
    def __init__(self):
        """Initialize S3 client."""
        self._s3_client = None
    
    @property
    def s3(self):
        """Get or create S3 client for public bucket (no credentials needed)."""
        if self._s3_client is None:
            self._s3_client = boto3.client(
                "s3", 
                config=Config(signature_version=UNSIGNED)
            )
        return self._s3_client
    
    def download_file(
        self, 
        bucket: str, 
        key: str, 
        output_path: Path
    ) -> bool:
        """
        Download file from S3 bucket.
        
        Parameters
        ----------
        bucket : str
            S3 bucket name
        key : str
            S3 object key (path)
        output_path : Path
            Local output file path
            
        Returns
        -------
        bool
            True if successful, False otherwise
        """
        try:
            logger.info(f"Downloading: s3://{bucket}/{key}")
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'wb') as f:
                self.s3.download_fileobj(bucket, key, f)
            
            logger.info(f"Saved to: {output_path}")
            return True
        except Exception as e:
            logger.error(f"S3 download failed: {e}")
            return False
    
    def file_exists(self, bucket: str, key: str) -> bool:
        """
        Check if file exists in S3 bucket.
        
        Parameters
        ----------
        bucket : str
            S3 bucket name
        key : str
            S3 object key (path)
            
        Returns
        -------
        bool
            True if file exists
        """
        try:
            self.s3.head_object(Bucket=bucket, Key=key)
            return True
        except:
            return False

    def object_size(self, bucket: str, key: str):
        """Return the S3 object size in bytes, or None if the object does not exist."""
        try:
            return self.s3.head_object(Bucket=bucket, Key=key)["ContentLength"]
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
                return None
            raise


def download_wcofs(
    output_dir: Path,
    s3_downloader: S3Downloader,
    date: datetime = None,
    round: int = 3,
    file_format: str = "grib2",
    forecast_steps: range = None,
    lat_bounds=LAT_LON_BOUNDS["lat"],
    lon_bounds=LAT_LON_BOUNDS["lon"],
    overwrite: bool = False,
    add_vel: bool = False,
) -> bool:
    """
    Download WCOFS data from NOAA S3 bucket.

    Parameters
    ----------
    output_dir : Path
        Output directory
    s3_downloader : S3Downloader
        S3 downloader instance
    date : datetime
        Forecast date (default: today)
    round : int
        Forecast round 3
    file_format : str
        File format: 'grib2' or 'netcdf'
    forecast_steps : range
        Forecast steps to download. For netcdf these are regular-grid lead hours
        (default: 3, 6, ..., 72); for grib2 the default is step 1 only.
    lat_bounds, lon_bounds : pair of float, optional
        NetCDF only: latitude and longitude (0-360 degrees east) crop box.
    overwrite : bool, optional
        NetCDF only: rebuild subsets even when a matching subset exists.
    add_vel : bool, optional
        NetCDF only: ensure subset files include velocity variables.

    Returns
    -------
    bool
        True if successful
    """
    if date is None:
        date = datetime.utcnow()

    if file_format == "netcdf":
        return download_wcofs_netcdf(
            output_dir, s3_downloader, date, round, forecast_steps,
            lat_bounds, lon_bounds, overwrite, add_vel,
        )

    output_dir.mkdir(parents=True, exist_ok=True)

    # Format date strings for S3 path
    date_str = date.strftime("%Y%m%d")
    round_str = f"{round:02d}"

    if forecast_steps is None:
        forecast_steps = range(1, 2)

    # WCOFS S3 bucket and path
    bucket = "noaa-nos-ofs-pds"
    
    downloaded_count = 0
    failed_count = 0
    
    try:
        # Download each forecast step
        for step in forecast_steps:
            step_str = f"{step:03d}"
            
            filename = f"wcofs.t{round_str}z.{date_str}.2ds.f{step_str}.grib2"
            s3_key = _wcofs_key(date, round, "2ds.f", step, "grib2")
            
            output_path = output_dir / filename
            
            # Check if file exists on S3
            if not s3_downloader.file_exists(bucket, s3_key):
                logger.warning(f"File not found on S3: {s3_key}")
                failed_count += 1
                continue
            
            if s3_downloader.download_file(bucket, s3_key, output_path):
                downloaded_count += 1
            else:
                failed_count += 1
        
        if downloaded_count > 0:
            logger.info(f"Downloaded {downloaded_count}/{len(list(forecast_steps))} files")
            return True
        else:
            logger.error(f"Failed to download any files")
            return False

    except Exception as e:
        logger.error(f"WCOFS download error: {e}")
        return False


def download_rtofs(
    output_dir: Path,
    s3_downloader: S3Downloader,
    date: datetime = None,
    round: int = 3,
    file_format: str = "grib2",
) -> bool:
    """
    Download RTOFS global ocean forecast data from NOAA S3 bucket.

    Parameters
    ----------
    output_dir : Path
        Output directory
    s3_downloader : S3Downloader
        S3 downloader instance
    date : datetime
        Forecast date (default: today)
    forecast_hour : int
        Forecast hour (0, 6, 12, 18)
    file_format : str
        File format: 'grib2' or 'netcdf'

    Returns
    -------
    bool
        True if successful
    """
    if date is None:
        date = datetime.utcnow()



    output_dir.mkdir(parents=True, exist_ok=True)

    date_str = date.strftime("%Y%m%d")
    round_str = f"{round:02d}"

    # RTOFS S3 bucket and path
    bucket = "noaa-nos-ofs-pds"

    try:
        # RTOFS file naming and path structure
        if file_format == "grib2":
            filename = f"rtofs.t{round_str}z.{date_str}.grb2"
            s3_key = _rtofs_key(date, round, file_format)
        else:
            filename = f"rtofs.t{round_str}z.{date_str}.nc"
            s3_key = _rtofs_key(date, round, file_format)
        
        output_path = output_dir / filename
        
        # Check if file exists on S3
        if not s3_downloader.file_exists(bucket, s3_key):
            logger.warning(f"File not found on S3: {s3_key}")
            return False
        
        return s3_downloader.download_file(bucket, s3_key, output_path)

    except Exception as e:
        logger.error(f"RTOFS download error: {e}")
        return False


def list_available_dates(
    s3_downloader: S3Downloader,
    forecast_type: str = "wcofs", 
    days_back: int = 7
) -> None:
    """
    List recently available forecast dates from S3.

    Parameters
    ----------
    s3_downloader : S3Downloader
        S3 downloader instance
    forecast_type : str
        'wcofs' or 'rtofs'
    days_back : int
        Number of days to look back
    """
    bucket = "noaa-nos-ofs-pds"
    prefix = f"{forecast_type}/netcdf/"
    
    try:
        logger.info(f"Listing available {forecast_type.upper()} dates from S3...")
        logger.info(f"S3 Bucket: {bucket}")
        logger.info(f"Prefix: {prefix}")
        
        paginator = s3_downloader.s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        
        dates = set()
        for page in pages:
            if "Contents" in page:
                for obj in page["Contents"]:
                    key = obj["Key"]
                    # Extract date from path: wcofs/netcdf/YYYY/MM/DD/...
                    parts = key.split("/")
                    if len(parts) >= 5:
                        try:
                            date_str = f"{parts[2]}-{parts[3]}-{parts[4]}"
                            dates.add(date_str)
                        except:
                            pass
        
        if dates:
            sorted_dates = sorted(dates, reverse=True)[:days_back]
            logger.info(f"\nRecent dates available (last {days_back} days):")
            for date in sorted(sorted_dates):
                logger.info(f"  {date}")
        else:
            logger.warning("No dates found")
            
    except Exception as e:
        logger.error(f"Error listing dates: {e}")


def subset_wcofs_dataset(
    data,
    lat_bounds=LAT_LON_BOUNDS["lat"],
    lon_bounds=LAT_LON_BOUNDS["lon"],
    include_velocity: bool = False,
):
    """Crop a WCOFS regular-grid dataset to a latitude/longitude box.

    Parameters
    ----------
    data : xarray.Dataset
        WCOFS regular-grid file with rectilinear 2-D Latitude/Longitude and
        temp, salt, and zeta; when requested, velocity variables are retained.
    lat_bounds : pair of float, optional
        Inclusive south/north limits in degrees north, default (37, 39).
    lon_bounds : pair of float, optional
        Inclusive west/east limits in degrees east on a 0-360 convention,
        default (236, 239). Source longitudes may use either convention.

    Returns
    -------
    xarray.Dataset
        temp and salt (time, Depth, lat, lon), zeta (time, lat, lon), and
        optionally velocity variables on 1-D lat and lon (0-360 degrees east)
        coordinates, at all depths.
    """
    import numpy as np

    south, north = lat_bounds
    west, east = lon_bounds
    if not (-90 <= south < north <= 90):
        raise ValueError("Latitude bounds must satisfy -90 <= SOUTH < NORTH <= 90")
    if not (0 <= west < east <= 360):
        raise ValueError("Longitude bounds must satisfy 0 <= WEST < EAST <= 360")
    latitude = data["Latitude"].values
    longitude = data["Longitude"].values % 360
    if (
        latitude.ndim != 2
        or not np.allclose(latitude, latitude[:, :1], equal_nan=True)
        or not np.allclose(longitude, longitude[:1, :], equal_nan=True)
    ):
        raise ValueError("WCOFS regular-grid Latitude/Longitude must be rectilinear")
    lat = latitude[:, 0]
    lon = longitude[0, :]
    rows = np.nonzero((lat >= south) & (lat <= north))[0]
    columns = np.nonzero((lon >= west) & (lon <= east))[0]
    if not rows.size or not columns.size:
        raise ValueError("No WCOFS grid cells fall within the latitude/longitude box")
    rows = slice(int(rows.min()), int(rows.max()) + 1)
    columns = slice(int(columns.min()), int(columns.max()) + 1)
    y_dim, x_dim = data["Latitude"].dims
    fields = ["temp", "salt", "zeta"]
    if include_velocity:
        velocity_pattern = re.compile(
            r"(?:^u$|^v$|^ubar$|^vbar$|velocity|water_u|water_v|u_eastward|v_northward|uvel|vvel)",
            re.IGNORECASE,
        )
        velocity_fields = [
            name for name in data.data_vars
            if velocity_pattern.search(name)
        ]
        if not velocity_fields:
            raise ValueError("Requested velocity fields but none are present in this WCOFS source file")
        for name in velocity_fields:
            if name not in fields:
                fields.append(name)
    subset = data[fields].isel({y_dim: rows, x_dim: columns})
    subset = subset.drop_vars(["Latitude", "Longitude"], errors="ignore").rename({y_dim: "lat", x_dim: "lon"})
    subset = subset.assign_coords(lat=lat[rows], lon=lon[columns])
    subset.attrs = dict(data.attrs)
    subset.attrs["subset_lat_bounds"] = list(lat_bounds)
    subset.attrs["subset_lon_bounds"] = list(lon_bounds)
    subset.attrs["subset_include_velocity"] = "true" if include_velocity else "false"
    return subset


def _has_wcofs_velocity_variables(dataset):
    names = {name.lower() for name in dataset.data_vars}
    if {"u", "v"}.issubset(names) or {"ubar", "vbar"}.issubset(names):
        return True
    return any(
        ("velocity" in name)
        or ("water_u" in name)
        or ("water_v" in name)
        or ("u_eastward" in name)
        or ("v_northward" in name)
        for name in names
    )


def subset_wcofs_nowcast_elevation(
    data,
    lat_bounds=LAT_LON_BOUNDS["lat"],
    lon_bounds=LAT_LON_BOUNDS["lon"],
):
    """Crop a WCOFS 2ds nowcast to zeta and its required grid/time variables."""
    import numpy as np

    south, north = lat_bounds
    west, east = lon_bounds
    if not (-90 <= south < north <= 90):
        raise ValueError("Latitude bounds must satisfy -90 <= SOUTH < NORTH <= 90")
    if not (0 <= west < east <= 360):
        raise ValueError("Longitude bounds must satisfy 0 <= WEST < EAST <= 360")

    required = {"zeta", "lon_rho", "lat_rho", "mask_rho", "ocean_time"}
    missing = required.difference(data.variables)
    if missing:
        raise ValueError(f"WCOFS nowcast is missing required variables: {sorted(missing)}")
    latitude = np.asarray(data["lat_rho"].values, dtype=float)
    longitude = np.asarray(data["lon_rho"].values, dtype=float) % 360
    if latitude.ndim != 2 or longitude.shape != latitude.shape:
        raise ValueError("WCOFS nowcast lon_rho and lat_rho must be matching 2-D grids")
    eta_dim, xi_dim = data["lat_rho"].dims
    if data["lon_rho"].dims != (eta_dim, xi_dim):
        raise ValueError("WCOFS nowcast longitude and latitude dimensions must match")
    if set(data["zeta"].dims) != {"ocean_time", eta_dim, xi_dim}:
        raise ValueError("WCOFS nowcast zeta must use ocean_time, eta_rho, and xi_rho")

    inside = (
        np.isfinite(latitude)
        & np.isfinite(longitude)
        & (latitude >= south)
        & (latitude <= north)
        & (longitude >= west)
        & (longitude <= east)
    )
    rows = np.flatnonzero(inside.any(axis=1))
    columns = np.flatnonzero(inside.any(axis=0))
    if not rows.size or not columns.size:
        raise ValueError("No WCOFS nowcast grid cells fall within the latitude/longitude box")
    row_start, row_stop = int(rows[0]), int(rows[-1]) + 1
    column_start, column_stop = int(columns[0]), int(columns[-1]) + 1

    subset = data[["zeta", "lon_rho", "lat_rho", "mask_rho", "ocean_time"]].isel(
        {
            eta_dim: slice(row_start, row_stop),
            xi_dim: slice(column_start, column_stop),
        }
    )
    in_box = inside[row_start:row_stop, column_start:column_stop]
    subset["zeta"] = subset["zeta"].where(in_box)
    subset["mask_rho"] = subset["mask_rho"].where(in_box, 0)
    subset = subset.assign_coords(
        {
            eta_dim: np.arange(row_start, row_stop),
            xi_dim: np.arange(column_start, column_stop),
        }
    )
    subset.attrs = dict(data.attrs)
    subset.attrs["subset_lat_bounds"] = list(lat_bounds)
    subset.attrs["subset_lon_bounds"] = list(lon_bounds)
    return subset


def download_wcofs_nowcast_elevation(
    output_dir: Path,
    s3_downloader: S3Downloader,
    date: datetime,
    round: int = 3,
    nowcast_steps=None,
    lat_bounds=LAT_LON_BOUNDS["lat"],
    lon_bounds=LAT_LON_BOUNDS["lon"],
    overwrite: bool = False,
) -> bool:
    """Combine the hourly 2ds nowcasts into one complete fixed-PST daily file."""
    import xarray as xr
    import numpy as np
    import pandas as pd

    steps = set(NOWCAST_STEPS if nowcast_steps is None else nowcast_steps)
    if not set(NOWCAST_STEPS).issubset(steps):
        raise ValueError(
            "A complete Pacific day requires all nowcast leads 1 through 24; "
            "select --nowcast-steps 1:24 or omit the option"
        )

    date = pd.Timestamp(date).normalize()
    expected_pst = pd.date_range(date, periods=24, freq="h")
    expected_utc = expected_pst + pd.Timedelta(hours=8)
    # NOAA's daily nNNN files span run-date minus 20 hours through plus 3
    # hours: a PST day therefore uses n005-n024 from date+1 and n001-n004
    # from date+2. The internal ocean_time values are checked below.
    source_files = _nowcast_source_files(date)
    subset_dir = Path(output_dir) / "nowcast_elevation"
    output_path = subset_dir / f"wcofs_nowcast_elevation_pst{date:%Y%m%d}.nc"
    source_keys = [
        _wcofs_key(source_date, round, "2ds.n", step, "netcdf")
        for source_date, step in source_files
    ]
    metadata = {
        "subset_source": "NOAA WCOFS 2ds nowcast",
        "subset_source_keys": "\n".join(source_keys),
        "subset_version": 2,
    }

    if not overwrite and output_path.is_file():
        try:
            with xr.open_dataset(output_path, decode_times=False) as existing:
                if (
                    all(existing.attrs.get(name) == value for name, value in metadata.items())
                    and list(existing.attrs.get("subset_lat_bounds", [])) == list(lat_bounds)
                    and list(existing.attrs.get("subset_lon_bounds", [])) == list(lon_bounds)
                    and existing.sizes.get("time") == 24
                    and {"zeta", "lon_rho", "lat_rho", "mask_rho"}.issubset(existing.variables)
                ):
                    logger.info(f"Already available: {output_path}")
                    return True
        except (OSError, ValueError):
            logger.warning(f"Cannot read existing daily file; rebuilding: {output_path}")

    success = True
    samples = []
    for source_date, step in source_files:
        filename = f"wcofs.t{round:02d}z.{source_date:%Y%m%d}.2ds.n{step:03d}.nc"
        key = _wcofs_key(source_date, round, "2ds.n", step, "netcdf")
        size = s3_downloader.object_size(BUCKET, key)
        if size is None:
            logger.warning(f"File not found on S3: {key}")
            success = False
            continue

        try:
            subset_dir.mkdir(parents=True, exist_ok=True)
            with TemporaryDirectory(prefix="wcofs_nowcast_", dir=subset_dir) as directory:
                source_path = Path(directory) / filename
                if not s3_downloader.download_file(BUCKET, key, source_path):
                    success = False
                    continue
                if source_path.stat().st_size != size:
                    raise ValueError(f"Downloaded size does not match S3 object: {key}")
                with xr.open_dataset(source_path) as data:
                    subset = subset_wcofs_nowcast_elevation(
                        data, lat_bounds, lon_bounds
                    ).load()
            samples.append(subset)
        except (OSError, ValueError, KeyError) as error:
            logger.error(f"Nowcast elevation subsetting failed for {key}: {error}")
            success = False
    if not success:
        logger.error(
            "Not writing %s because one or more hourly nowcast files could not be loaded",
            output_path,
        )
        return False

    combined = xr.concat(
        samples,
        dim="ocean_time",
        data_vars="minimal",
        coords="minimal",
        compat="equals",
        join="exact",
    ).sortby("ocean_time")
    utc_index = pd.DatetimeIndex(combined["ocean_time"].values)
    if utc_index.has_duplicates:
        keep = ~utc_index.duplicated(keep="last")
        combined = combined.isel(ocean_time=keep)
        utc_index = utc_index[keep]

    selected = (utc_index >= expected_utc[0]) & (utc_index <= expected_utc[-1])
    combined = combined.isel(ocean_time=selected)
    utc_index = utc_index[selected]
    pst_index = utc_index - pd.Timedelta(hours=8)
    if not pst_index.equals(expected_pst):
        missing = expected_pst.difference(pst_index)
        extra = pst_index.difference(expected_pst)
        raise ValueError(
            f"Nowcast files do not provide a complete hourly PST day for "
            f"{date:%Y-%m-%d}; missing {len(missing)} hour(s), "
            f"unexpected {len(extra)} hour(s)"
        )

    combined = combined.rename({"ocean_time": "time"})
    combined = combined.assign_coords(
        time=(
            "time",
            np.arange(24, dtype=np.int32),
            {
                "timezone": "modified from utc to pst",
                "utc_offset": "-08:00",
                "units": f"hours since {date:%Y-%m-%d} 00:00:00",
                "calendar": "proleptic_gregorian",
            },
        )
    )
    combined["time"].encoding = {"_FillValue": None}
    combined.attrs.update(metadata)
    combined.attrs["subset_lat_bounds"] = list(lat_bounds)
    combined.attrs["subset_lon_bounds"] = list(lon_bounds)
    combined.attrs["institution"] = "NOAA National Ocean Service"
    combined.attrs["history"] = "Hourly WCOFS 2ds nowcasts combined and converted from UTC to fixed PST"
    combined.attrs["field_type"] = "instantaneous"
    combined["zeta"].attrs = dict(combined["zeta"].attrs)
    combined["zeta"].attrs.update(
        long_name="Water Surface Elevation",
        standard_name="sea_surface_elevation",
        units="m",
    )

    partial_path = output_path.with_name(output_path.name + ".part")
    try:
        encoding = {
            "zeta": {"zlib": True, "complevel": 4, "_FillValue": -9999.0},
            "mask_rho": {"zlib": True, "complevel": 4, "_FillValue": None},
            "time": {"dtype": "int32", "_FillValue": None},
        }
        for name in ("lat_rho", "lon_rho"):
            encoding[name] = {"dtype": "float64", "_FillValue": np.nan}
        combined.to_netcdf(
            partial_path,
            engine="netcdf4",
            format="NETCDF4",
            encoding=encoding,
        )
        partial_path.replace(output_path)
    finally:
        partial_path.unlink(missing_ok=True)
    logger.info("Saved 24 hourly fixed-PST nowcast samples to: %s", output_path)
    return True


def download_wcofs_netcdf(
    output_dir: Path,
    s3_downloader: S3Downloader,
    date: datetime,
    round: int = 3,
    forecast_steps=None,
    lat_bounds=LAT_LON_BOUNDS["lat"],
    lon_bounds=LAT_LON_BOUNDS["lon"],
    overwrite: bool = False,
    add_vel: bool = False,
) -> bool:
    """Download one cycle of regular-grid NetCDF forecasts and save box subsets.

    Parameters
    ----------
    output_dir : Path
        Subsets are written to output_dir/subset; full source files are
        downloaded to a temporary folder and removed after cropping.
    s3_downloader : S3Downloader
        S3 downloader instance.
    date : datetime
        Forecast cycle date.
    round : int, optional
        Forecast cycle hour (default 3, i.e. t03z).
    forecast_steps : iterable of int, optional
        Lead hours from the cycle start; default 3, 6, ..., 72.
    lat_bounds, lon_bounds : pair of float, optional
        Crop box, defaulting to the download_rtofs.py box.
    overwrite : bool, optional
        Rebuild subsets even when a subset from the same source and box exists.
    add_vel : bool, optional
        Ensure subset files include velocity variables; existing subsets
        without velocity are rebuilt.

    Returns
    -------
    bool
        True if every requested step was found and subsetted.
    """
    import xarray as xr

    steps = NETCDF_STEPS if forecast_steps is None else forecast_steps
    subset_dir = Path(output_dir) / "subset"
    success = True
    for step in steps:
        filename = f"wcofs.t{round:02d}z.{date:%Y%m%d}.regulargrid.f{step:03d}.nc"
        key = _wcofs_key(date, round, "regulargrid.f", step, "netcdf")
        output_path = subset_dir / filename
        size = s3_downloader.object_size(BUCKET, key)
        if size is None:
            logger.warning(f"File not found on S3: {key}")
            success = False
            continue
        metadata = {
            "subset_source_key": key,
            "subset_source_size": str(size),
            "subset_version": 1,
            "subset_include_velocity": "true" if add_vel else "false",
        }
        if not overwrite and output_path.is_file():
            try:
                with xr.open_dataset(output_path, decode_times=False) as existing:
                    if add_vel and not _has_wcofs_velocity_variables(existing):
                        logger.info(
                            "Subset missing velocity fields; rebuilding with --add-vel: %s",
                            output_path,
                        )
                        raise ValueError("subset missing velocity fields")
                    if (
                        existing.attrs.get("subset_source_key") == key
                        and existing.attrs.get("subset_source_size") == str(size)
                        and existing.attrs.get("subset_version") == 1
                        and list(existing.attrs.get("subset_lat_bounds", [])) == list(lat_bounds)
                        and list(existing.attrs.get("subset_lon_bounds", [])) == list(lon_bounds)
                    ):
                        logger.info(f"Already subsetted: {output_path}")
                        continue
            except (OSError, ValueError):
                logger.warning(f"Cannot read existing subset; rebuilding: {output_path}")
        partial_path = output_path.with_name(output_path.name + ".part")
        try:
            subset_dir.mkdir(parents=True, exist_ok=True)
            with TemporaryDirectory(prefix="wcofs_", dir=subset_dir) as directory:
                source_path = Path(directory) / filename
                if not s3_downloader.download_file(BUCKET, key, source_path):
                    success = False
                    continue
                if source_path.stat().st_size != size:
                    raise ValueError(f"Downloaded size does not match S3 object: {key}")
                with xr.open_dataset(source_path) as data:
                    subset = subset_wcofs_dataset(
                        data, lat_bounds, lon_bounds, include_velocity=add_vel
                    ).load()
            subset.attrs.update(metadata)
            for name in subset.variables:
                subset[name].encoding = {
                    name: value for name, value in subset[name].encoding.items()
                    if name in {"units", "calendar"}
                }
            encoding = {
                name: {"zlib": True, "complevel": 4, "_FillValue": -9999.0}
                for name in subset.data_vars
            }
            subset.to_netcdf(partial_path, engine="netcdf4", format="NETCDF4", encoding=encoding)
            partial_path.replace(output_path)
            logger.info(f"Saved geographic subset to: {output_path}")
        except (OSError, ValueError, KeyError) as error:
            logger.error(f"Subsetting failed for {key}: {error}")
            success = False
        finally:
            partial_path.unlink(missing_ok=True)
    return success


def _load_wcofs_series(paths):
    """Concatenate cropped forecasts; newer cycles win at duplicate valid times."""
    import numpy as np
    import pandas as pd
    import xarray as xr

    def order(path):
        cycle, day, step = NETCDF_NAME.search(path.name).groups()
        return day, int(cycle), int(step)

    samples = []
    for path in sorted(paths, key=order):
        with xr.open_dataset(path) as data:
            data = data[["temp", "salt", "zeta"]].load()
        if samples:
            for name in ("lat", "lon", "Depth"):
                reference = samples[0][name]
                if data[name].shape != reference.shape or not np.allclose(data[name], reference):
                    raise ValueError(f"Inconsistent WCOFS {name} grid in {path}")
        samples.append(data)
    combined = xr.concat(samples, dim="time", data_vars="all", coords="minimal", compat="override", join="override")
    unique = ~pd.Index(combined.time.values).duplicated(keep="last")
    return combined.isel(time=unique).sortby("time")


def _fixed_pst_index(index):
    """Return a timezone-naive index expressed in fixed PST (UTC-08:00)."""
    import pandas as pd

    index = pd.DatetimeIndex(index)
    if index.tz is not None:
        index = index.tz_convert("Etc/GMT+8").tz_localize(None)
    return index


def _ensure_wcofs_datum_grid(data_dir):
    """Find or download NOAA's WCOFS vertical-datum grid."""
    import xarray as xr

    data_dir = Path(data_dir)
    for candidate in (data_dir / _WCOFS_DATUM_GRID_NAME, data_dir.parent / _WCOFS_DATUM_GRID_NAME):
        if candidate.is_file():
            return candidate

    matches = list(data_dir.rglob(_WCOFS_DATUM_GRID_NAME))
    if len(matches) > 1:
        raise ValueError(f"Multiple WCOFS datum grids found under {data_dir}")
    if matches:
        return matches[0]

    output_dir = (
        data_dir.parent
        if data_dir.name in {"processed", "nowcast_elevation"}
        else data_dir
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / _WCOFS_DATUM_GRID_NAME
    temporary_path = None
    try:
        with NamedTemporaryFile(
            dir=output_dir, prefix=".wcofs_vdatums_", suffix=".part", delete=False
        ) as temporary_file:
            temporary_path = Path(temporary_file.name)
            with urlopen(_WCOFS_DATUM_GRID_URL, timeout=120) as response:
                shutil.copyfileobj(response, temporary_file)
        with xr.open_dataset(temporary_path, decode_times=False) as dataset:
            required = {"latitude", "longitude", "navd88tomsl"}
            missing = required.difference(dataset.variables)
            if missing:
                raise ValueError(
                    f"Downloaded WCOFS datum grid is missing variables: {sorted(missing)}"
                )
        temporary_path.replace(output_path)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
    return output_path


@contextmanager
def _wcofs_local_transformer(grid_lon, grid_lat):
    """Keep local operation selection and use inside the offline PROJ scope."""
    import numpy as np
    from pyproj import network
    from pyproj.aoi import AreaOfInterest
    from pyproj.transformer import TransformerGroup

    target_lon = np.where(grid_lon > 180.0, grid_lon - 360.0, grid_lon)
    finite = np.isfinite(target_lon) & np.isfinite(grid_lat)
    if not finite.any():
        raise ValueError("No finite WCOFS coordinates for datum-grid projection")
    enabled = network.is_network_enabled()
    try:
        network.set_network_enabled(False)
        group = TransformerGroup(
            "EPSG:4326", "EPSG:26910", always_xy=True, allow_ballpark=False,
            area_of_interest=AreaOfInterest(
                float(target_lon[finite].min()), float(grid_lat[finite].min()),
                float(target_lon[finite].max()), float(grid_lat[finite].max()),
            ),
        )
        if not group.transformers:
            raise ValueError("No local non-ballpark EPSG:26910 transformation for WCOFS")
        transformer = group.transformers[0]
        if not group.best_available:
            warnings.warn(
                f"Using local WCOFS horizontal transformation: {transformer.description}; "
                f"stated accuracy {transformer.accuracy:g} m. Optional PROJ grids "
                "are not downloaded; the 10 km datum-coverage check is unchanged."
            )
        yield transformer
    finally:
        network.set_network_enabled(enabled)


def _project_wcofs_datum_grids(datum_longitude, datum_latitude, grid_lon, grid_lat):
    """Project both grids consistently without fetching optional PROJ grids."""
    import numpy as np

    target_lon = np.where(grid_lon > 180.0, grid_lon - 360.0, grid_lon)
    with _wcofs_local_transformer(grid_lon, grid_lat) as transformer:
        datum_x, datum_y = transformer.transform(
            datum_longitude, datum_latitude, errcheck=True
        )
        target_x, target_y = transformer.transform(target_lon, grid_lat, errcheck=True)
    return datum_x, datum_y, target_x, target_y


def _convert_wcofs_surface_to_navd88(
    data_dir, latitude, longitude, surface, mask=None, datum_path=None
):
    """Convert MSL to NAVD88 using local projection and offsets within 10 km."""
    import numpy as np
    import xarray as xr
    from scipy.spatial import cKDTree

    datum_path = Path(datum_path) if datum_path is not None else _ensure_wcofs_datum_grid(data_dir)
    with xr.open_dataset(datum_path, decode_times=False) as dataset:
        datum_latitude = np.asarray(dataset["latitude"].values, dtype=float)
        datum_longitude = np.asarray(dataset["longitude"].values, dtype=float)
        navd88_to_msl = np.asarray(dataset["navd88tomsl"].values, dtype=float).copy()
        fill_value = dataset["navd88tomsl"].attrs.get("filledvalue")
    if not (
        datum_latitude.ndim == datum_longitude.ndim == navd88_to_msl.ndim == 2
        and datum_latitude.shape == datum_longitude.shape == navd88_to_msl.shape
    ):
        raise ValueError(f"Invalid coordinate or offset dimensions in {datum_path}")
    if fill_value is not None:
        navd88_to_msl[navd88_to_msl == float(fill_value)] = np.nan

    latitude = np.asarray(latitude, dtype=float)
    longitude = np.asarray(longitude, dtype=float)
    if latitude.ndim == 1 and longitude.ndim == 1:
        grid_lon, grid_lat = np.meshgrid(longitude, latitude)
    elif latitude.ndim == 2 and longitude.shape == latitude.shape:
        grid_lat, grid_lon = latitude, longitude
    else:
        raise ValueError("WCOFS latitude and longitude must be matching 2-D grids or 1-D axes")
    if surface.shape[1:] != grid_lat.shape:
        raise ValueError("WCOFS elevation shape does not match its grid")
    valid_target = (
        np.isfinite(grid_lat) & np.isfinite(grid_lon) & np.isfinite(surface).any(axis=0)
    )
    if mask is not None:
        mask = np.asarray(mask, dtype=float)
        if mask.shape != grid_lat.shape:
            raise ValueError("WCOFS water mask shape does not match its grid")
        valid_target &= np.isfinite(mask) & (mask > 0.5)
    valid_datum = (
        np.isfinite(datum_latitude) & np.isfinite(datum_longitude) & np.isfinite(navd88_to_msl)
    )
    if not valid_datum.any():
        raise ValueError(f"No valid NAVD88 offsets in {datum_path}")
    datum_values = navd88_to_msl[valid_datum]
    datum_x, datum_y, target_x, target_y = _project_wcofs_datum_grids(
        datum_longitude[valid_datum], datum_latitude[valid_datum], grid_lon, grid_lat
    )
    valid_projected_datum = np.isfinite(datum_x) & np.isfinite(datum_y)
    if not valid_projected_datum.all():
        invalid_count = int(np.count_nonzero(~valid_projected_datum))
        warnings.warn(f"Excluding {invalid_count} datum-grid cell(s) with non-finite projected coordinates")
    datum_points = np.column_stack((datum_x[valid_projected_datum], datum_y[valid_projected_datum]))
    datum_values = datum_values[valid_projected_datum]
    if not len(datum_points):
        raise ValueError(f"No valid projected NAVD88 datum-grid points in {datum_path}")
    invalid_target = valid_target & ~(np.isfinite(target_x) & np.isfinite(target_y))
    if invalid_target.any():
        raise ValueError(
            f"Could not project {int(np.count_nonzero(invalid_target))} valid "
            "WCOFS water-grid cell(s) to EPSG:26910"
        )
    target_points = np.column_stack((target_x[valid_target], target_y[valid_target]))
    if not len(target_points):
        raise ValueError("WCOFS data has no valid wet grid cells to convert")
    neighbor_count = min(4, len(datum_points))
    distances, neighbors = cKDTree(datum_points).query(target_points, k=neighbor_count)
    if neighbor_count == 1:
        distances = distances[:, np.newaxis]
        neighbors = neighbors[:, np.newaxis]
    if np.any(distances[:, 0] > 10000.0):
        count = int(np.count_nonzero(distances[:, 0] > 10000.0))
        raise ValueError(
            f"No nearby NAVD88 datum-grid coverage for {count} WCOFS cell(s) in {datum_path}"
        )
    datum_values = datum_values[neighbors]
    offsets = np.empty(len(target_points), dtype=float)
    exact = distances[:, 0] <= 1e-3
    offsets[exact] = datum_values[exact, 0]
    if (~exact).any():
        weights = 1.0 / np.square(distances[~exact])
        offsets[~exact] = np.sum(weights * datum_values[~exact], axis=1) / weights.sum(axis=1)
    offset_grid = np.full(grid_lat.shape, np.nan, dtype=float)
    offset_grid[valid_target] = offsets
    if not np.isfinite(offset_grid[valid_target]).all():
        raise ValueError("Could not determine NAVD88 offsets for all WCOFS water cells")
    # navd88tomsl is NAVD88 minus MSL, so subtract it from MSL-referenced zeta.
    return surface - offset_grid[np.newaxis, :, :]


def _project_wcofs_grid(latitude, longitude, surface, mask=None):
    """Project WCOFS grid coordinates to EPSG:26910 and identify wet cells."""
    import numpy as np

    latitude = np.asarray(latitude, dtype=float)
    longitude = np.asarray(longitude, dtype=float)
    if latitude.ndim == 1 and longitude.ndim == 1:
        grid_lon, grid_lat = np.meshgrid(longitude, latitude)
    elif latitude.ndim == 2 and longitude.shape == latitude.shape:
        grid_lat, grid_lon = latitude, longitude
    else:
        raise ValueError("WCOFS latitude and longitude must be matching 2-D grids or 1-D axes")
    if surface.shape[1:] != grid_lat.shape:
        raise ValueError("WCOFS elevation shape does not match its grid")
    lon_180 = np.where(grid_lon > 180.0, grid_lon - 360.0, grid_lon)
    with _wcofs_local_transformer(grid_lon, grid_lat) as transformer:
        grid_x, grid_y = transformer.transform(lon_180, grid_lat, errcheck=True)
    valid_grid = np.isfinite(grid_x) & np.isfinite(grid_y)
    valid_grid &= np.isfinite(surface).any(axis=0)
    if mask is not None:
        mask = np.asarray(mask)
        if mask.shape != grid_lat.shape:
            raise ValueError("WCOFS water mask shape does not match its grid")
        valid_grid &= np.isfinite(mask) & (mask > 0.5)
    return grid_x, grid_y, valid_grid


def _interpolate_wcofs_to_boundary(
    time_index, latitude, longitude, surface, boundary_nodes, boundary_xy, mask=None
):
    """Linearly interpolate WCOFS elevations to projected boundary points."""
    import numpy as np
    import pandas as pd
    from scipy.interpolate import LinearNDInterpolator
    from scipy.spatial import Delaunay

    grid_x, grid_y, valid_grid = _project_wcofs_grid(latitude, longitude, surface, mask)
    projected_grid = np.column_stack((grid_x[valid_grid], grid_y[valid_grid]))
    if len(projected_grid) < 3:
        raise ValueError("WCOFS grid has fewer than three valid water cells")
    triangulation = Delaunay(projected_grid)
    values = surface.transpose(1, 2, 0)[valid_grid]
    interpolator = LinearNDInterpolator(triangulation, values, fill_value=np.nan)
    boundary_values = interpolator(boundary_xy)
    return pd.DataFrame(
        boundary_values.T,
        index=pd.DatetimeIndex(time_index, name="time"),
        columns=pd.Index(boundary_nodes, name="boundary_node_index"),
    )


def _extract_wcofs_run_point_timeseries(run_files, processed_dir, run_day, run_cycle):
    """Write one run's f003-f072 NAVD88 point time series CSV."""
    import numpy as np
    import pandas as pd
    import xarray as xr

    run_steps = []
    run_times = []
    run_surface = []
    latitude = longitude = None
    for path in sorted(run_files):
        name_match = NETCDF_NAME.search(path.name)
        if name_match is None:
            raise ValueError(f"Unexpected WCOFS subset filename: {path.name}")
        _, _, step = name_match.groups()
        step = int(step)
        with xr.open_dataset(path) as dataset:
            required = {"time", "lat", "lon", "zeta"}
            missing = required.difference(dataset.variables)
            if missing:
                raise ValueError(
                    f"{path} is missing WCOFS variables: {sorted(missing)}"
                )
            this_lat = np.asarray(dataset["lat"].values, dtype=float)
            this_lon = np.asarray(dataset["lon"].values, dtype=float)
            if this_lat.ndim != 1 or this_lon.ndim != 1:
                raise ValueError(
                    f"Subset forecast coordinates must be 1-D in {path}"
                )
            zeta = dataset["zeta"]
            if set(zeta.dims) != {"time", "lat", "lon"}:
                raise ValueError(
                    f"WCOFS zeta must have time, lat, and lon dimensions in {path}"
                )
            zeta_values = np.asarray(
                zeta.transpose("time", "lat", "lon").values,
                dtype=float,
            )
            times = pd.to_datetime(dataset["time"].values)
        if zeta_values.shape[0] != len(times):
            raise ValueError(f"WCOFS time dimension does not match zeta in {path}")
        if latitude is None:
            latitude, longitude = this_lat, this_lon
        elif not (
            np.array_equal(this_lat, latitude, equal_nan=True)
            and np.array_equal(this_lon, longitude, equal_nan=True)
        ):
            raise ValueError(f"WCOFS grid differs across run files, including {path}")

        run_steps.extend([step] * len(times))
        run_times.extend(list(times))
        run_surface.append(zeta_values)

    required_steps = set(NETCDF_STEPS)
    present_steps = set(run_steps)
    missing_steps = sorted(required_steps - present_steps)
    if missing_steps:
        missing_text = ", ".join(f"f{value:03d}" for value in missing_steps)
        raise ValueError(
            f"Run {run_day:%Y-%m-%d} t{run_cycle:02d}z is missing lead files: {missing_text}"
        )

    surface = np.concatenate(run_surface, axis=0)
    frame = pd.DataFrame(
        {
            "step": np.asarray(run_steps, dtype=int),
            "time": pd.to_datetime(run_times),
        }
    )
    time_ns = frame["time"].to_numpy(dtype="datetime64[ns]").astype("int64")
    order = np.lexsort((time_ns, frame["step"].to_numpy()))
    frame = frame.iloc[order].reset_index(drop=True)
    surface = surface[order, :, :]
    unique = ~frame["step"].duplicated(keep="last")
    frame = frame.loc[unique, :].sort_values("step")
    surface = surface[unique.to_numpy(), :, :]
    time_index = _fixed_pst_index(frame["time"].to_numpy())

    surface_navd88 = _convert_wcofs_surface_to_navd88(
        processed_dir, latitude, longitude, surface
    )
    point_xy = np.asarray(WCOFS_POINT_COORDS_UTM10N, dtype=float)
    point_indices = np.arange(len(point_xy))
    point_series = _interpolate_wcofs_to_boundary(
        time_index,
        latitude,
        longitude,
        surface_navd88,
        point_indices,
        point_xy,
    )
    columns = [
        (
            f"point_{index}_easting_{easting:.3f}_northing_{northing:.3f}"
            "_NAVD88_m"
        )
        for index, (easting, northing) in enumerate(point_xy, start=1)
    ]
    point_series.columns = columns
    missing_points = [
        column for column in columns if not point_series[column].notna().any()
    ]
    if missing_points:
        raise ValueError(
            f"No WCOFS surface elevations are available for point(s): "
            f"{', '.join(missing_points)}"
        )

    csv_dir = Path(processed_dir).parent / "csv"
    csv_dir.mkdir(parents=True, exist_ok=True)
    csv_path = csv_dir / f"forecast_wcofs_{run_day:%Y%m%d}_t{run_cycle:02d}z.csv"
    point_series.to_csv(
        csv_path,
        index_label="time_fixed_PST",
        date_format="%Y-%m-%d %H:%M:%S",
    )
    logger.info("Saved NAVD88 point time series to: %s", csv_path)
    return csv_path


def process_wcofs(input_dir: Path, processed_dir: Path = None, start=None, end=None):
    """Interpolate cropped 3-hourly WCOFS forecasts to hourly fixed-PST daily files.

    Parameters
    ----------
    input_dir : Path
        Folder containing cropped wcofs.tCCz.YYYYMMDD.regulargrid.fNNN.nc files.
    processed_dir : Path, optional
        Destination, defaulting to a processed folder beside input_dir.
    start, end : datetime-like, optional
        Inclusive PST calendar dates, supplied together. By default every day
        fully bracketed by the available forecasts is written.

    Returns
    -------
    list of Path
        Files named wcofs_interpolated_hourly_pstYYYYMMDD.nc with unlimited
        24-sample int time (hours since PST midnight), double temp, salt
        (time, depth, lat, lon) and surf_el (time, lat, lon), all depths.
        One NAVD88 point time series CSV per forecast run is saved under
        processed_dir.parent / "csv" using lead files f003 through f072.

    Notes
    -----
    PST is fixed UTC-8 without daylight saving. Valid times come from each
    file's internal time. Duplicate valid times prefer the newest cycle, so
    the previous day's f024 supplies 03Z of the following date. Days that are
    not bracketed by samples at 3-hour or finer spacing are skipped (or rejected
    when requested explicitly); no extrapolation is performed. Point values
    use WCOFS NAVD88 datum offsets and linear spatial interpolation in UTM 10N.
    """
    import numpy as np
    import pandas as pd

    input_dir = Path(input_dir)
    if (start is None) != (end is None):
        raise ValueError("Processing start and end dates must be supplied together")
    paths = [path for path in input_dir.glob("wcofs.t*z.*.regulargrid.f*.nc") if NETCDF_NAME.search(path.name)]
    if not paths:
        raise ValueError(f"No cropped WCOFS regular-grid files under {input_dir}")
    series = _load_wcofs_series(paths)
    times = series.time.values
    explicit = start is not None
    if explicit:
        start, end = pd.Timestamp(start).normalize(), pd.Timestamp(end).normalize()
        if start > end:
            raise ValueError("Processing start date cannot be after end date")
    else:
        start = (pd.Timestamp(times[0]) - pd.to_timedelta(8, unit="h")).ceil("d")
        end = (pd.Timestamp(times[-1]) - pd.to_timedelta(31, unit="h")).floor("d")
        if start > end:
            raise ValueError("Downloaded forecasts do not bracket any complete 24-hour PST day")
    if processed_dir is None:
        processed_dir = input_dir.parent / "processed"
    processed_dir = Path(processed_dir)
    processed_dir.mkdir(parents=True, exist_ok=True)
    written = []
    for day in pd.date_range(start, end, freq="d"):
        utc_hours = pd.date_range(day, periods=24, freq="h").values + np.timedelta64(8, "h")
        first = int(np.searchsorted(times, utc_hours[0], side="right")) - 1
        last = int(np.searchsorted(times, utc_hours[-1], side="left"))
        if first < 0 or last >= len(times) or np.any(np.diff(times[first:last + 1]) > np.timedelta64(3, "h")):
            message = f"WCOFS forecasts do not cover every hour of PST {day:%Y-%m-%d} at 3-hour or finer spacing"
            if explicit:
                raise ValueError(message)
            logger.warning(f"{message}; skipping")
            continue
        data = series.isel(time=slice(first, last + 1)).interp(time=utc_hours)
        data = data.rename({"Depth": "depth", "zeta": "surf_el"})
        data = data.assign_coords(time=("time", np.arange(24, dtype=np.int32), {
            "timezone": "modified from utc to pst", "utc_offset": "-08:00",
            "units": f"hours since {day:%Y-%m-%d} 00:00:00", "calendar": "proleptic_gregorian",
        }))
        for name in data.variables:
            data[name].encoding = {}
        data.lat.attrs = {"long_name": "Latitude", "standard_name": "latitude", "units": "degrees_north", "point_spacing": "even", "axis": "Y", "NAVO_code": 1}
        data.lon.attrs = {"long_name": "Longitude", "standard_name": "longitude", "units": "degrees_east", "modulo": "360 degrees", "axis": "X", "NAVO_code": 2}
        for field, long_name, standard_name, units, code in (
            ("temp", "Water Temperature", "sea_water_potential_temperature", "degC", 15),
            ("salt", "Salinity", "sea_water_salinity", "psu", 16),
            ("surf_el", "Water Surface Elevation", "sea_surface_elevation", "m", None),
        ):
            data[field].attrs = {
                "long_name": long_name, "standard_name": standard_name, "units": units,
                "_CoordinateAxes": " ".join(data[field].dims) + " ",
            }
            if code is not None:
                data[field].attrs["NAVO_code"] = code
        data.temp.attrs["comment"] = "WCOFS potential temperature; not converted to in-situ temperature"
        data.surf_el.attrs["comment"] = "WCOFS zeta, free surface referenced to mean sea level"
        data.attrs = {
            "institution": "NOAA National Ocean Service",
            "source": "NOAA WCOFS regular-grid forecast",
            "history": "Linear temporal interpolation of 3-hourly forecasts; UTC to fixed PST",
            "field_type": "instantaneous", "Conventions": "CF-1.6 NAVO_netcdf_v1.1",
            "subset_lat_bounds": series.attrs.get("subset_lat_bounds", []),
            "subset_lon_bounds": series.attrs.get("subset_lon_bounds", []),
            "source_files": "\n".join(sorted(path.name for path in paths)),
        }
        filename = processed_dir / f"wcofs_interpolated_hourly_pst{day:%Y%m%d}.nc"
        partial_path = filename.with_name(filename.name + ".part")
        encoding = {
            name: {"dtype": "float64", "_FillValue": -9999.0, "zlib": True, "complevel": 4}
            for name in ("temp", "salt", "surf_el")
        }
        encoding.update({name: {"dtype": "float64", "_FillValue": np.nan} for name in ("lat", "lon", "depth")})
        encoding["time"] = {"dtype": "int32", "_FillValue": None}
        try:
            data.to_netcdf(partial_path, engine="netcdf4", format="NETCDF4_CLASSIC", unlimited_dims=["time"], encoding=encoding)
            partial_path.replace(filename)
        finally:
            partial_path.unlink(missing_ok=True)
        written.append(filename)
        logger.info(f"Saved 24 hourly PST samples to: {filename}")

    runs = {}
    for path in paths:
        match = NETCDF_NAME.search(path.name)
        if match is None:
            continue
        cycle, day, _ = match.groups()
        key = (pd.to_datetime(day, format="%Y%m%d"), int(cycle))
        runs.setdefault(key, []).append(path)
    for (run_day, run_cycle), run_files in sorted(runs.items()):
        _extract_wcofs_run_point_timeseries(
            run_files, processed_dir, run_day, run_cycle
        )
    return written


def main(argv=None):
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Download NOAA WCOFS and RTOFS forecast data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download latest WCOFS data
  python download_wcofs.py wcofs --dest ./data

  # Download WCOFS from specific date
  python download_wcofs.py wcofs -d 2026-10-07 --dest ./data
  
  # Download WCOFS regular-grid netcdf (f003-f072) for a date and the day before,
  # crop to the lat/lon box, and write hourly PST files to OUTPUT/wcofs/processed
  python download_wcofs.py wcofs -d 2026-10-13 -f netcdf --dest ./data

  # Ensure existing/new NetCDF subsets include velocity variables
  python download_wcofs.py wcofs -d 2026-10-13 -f netcdf --add-vel --dest ./data
  
  # Date range, lead hours 3-24 only
  python download_wcofs.py wcofs --start 2026-10-01 --end 2026-10-07 -f netcdf -fs 3:24 --dest ./data

  # Download historical ROMS fields without cropping or processing
  python download_wcofs.py wcofs -d 2021-03-23 -f netcdf --native-grid --dest ./data

  # Download one complete fixed-PST daily sea-surface-elevation file
  python download_wcofs.py wcofs -d 2026-11-01 --nowcast-elevation --dest ./data

  # Nowcast mode requires the complete n001-n024 hourly sequence
  python download_wcofs.py wcofs --start 2026-11-01 --end 2026-11-02 \\
      --nowcast-elevation --nowcast-steps 1:24 --dest ./data
  
  # Reprocess existing subsets without downloading
  python download_wcofs.py wcofs --process-only --dest ./data
  
  # Download both WCOFS and RTOFS for date range
  python download_wcofs.py wcofs rtofs --start 2026-10-01 --end 2026-10-07 --dest ./data

  # Inspect source files and transfer sizes without writing data
  python download_wcofs.py wcofs -d 2026-10-13 -f netcdf --dry-run

NetCDF notes:
  - fNNN lead hours (003-072, every 3 h) count from cycle start (t03z).
  - Each source file's internal time is used.
  - Each source file is ~555 MB; one requested date (+ previous day) is ~26 GB transfer.
  - Only box subsets are kept in OUTPUT/wcofs/subset.
  - Processed files include temp, salt, surf_el (zeta), all depths, 24 hourly fixed-PST samples.
  - Incomplete days are skipped; no extrapolation is performed.

Native-grid notes:
  - --native-grid saves complete ROMS fields under OUTPUT/wcofs/native.
  - Source files are roughly 336-354 MB each; no subsets, PST files, or CSVs
    are generated. Only requested cycles are downloaded.
  - Archive filenames use nos.wcofs.fields before 2024-09-17 and
    wcofs.t03z.DATE.fields from 2024-09-17 onward.

Nowcast elevation notes:
  - --nowcast-elevation combines hourly 2ds.nNNN samples into one complete
    24-hour fixed-PST file per requested date.
  - Output includes zeta, time, lon_rho, lat_rho, mask_rho under
    OUTPUT/wcofs/nowcast_elevation.
  - Each source file is about 74 MB.
  - All lead hours 1-24 are required for a complete day.
        """,
    )

    parser.add_argument(
        "forecast_type",
        nargs="+",
        choices=["wcofs", "rtofs"],
        help="Forecast type(s) to download",
    )
    parser.add_argument(
        "-d",
        "--date",
        type=str,
        default=None,
        help="Single date in YYYY-MM-DD format (default: today)",
    )
    parser.add_argument(
        "-sdate",
        "--start",
        type=str,
        default=None,
        help="Start date for date range in YYYY-MM-DD format",
    )
    parser.add_argument(
        "-edate",
        "--end",
        type=str,
        default=None,
        help="End date for date range in YYYY-MM-DD format (inclusive)",
    )
    parser.add_argument(
        "-o",
        "--dest",
        "--output",
        dest="output",
        type=str,
        default="./wcofs_rtofs_data",
        help="Output directory (default: ./wcofs_rtofs_data)",
    )

    parser.add_argument(
        "-f",
        "--format",
        type=str,
        default="grib2",
        choices=["grib2", "netcdf"],
        help="File format (default: grib2)",
    )
    parser.add_argument(
        "-fs",
        "--forecast-steps",
        type=str,
        default=None,
        help="Forecast steps: 'START:END[:STRIDE]' or a single step. For netcdf these are lead hours; "
             "steps other than 3, 6, ..., 72 are ignored (default: all). Default for grib2: step 1 only.",
    )
    parser.add_argument(
        "--nowcast-elevation",
        action="store_true",
        help="Combine WCOFS 2ds.nNNN elevation nowcasts into one complete fixed-PST file per day",
    )
    parser.add_argument(
        "--native-grid", action="store_true",
        help="Download full native ROMS forecast fields from the historical archive, without processing",
    )
    parser.add_argument(
        "--nowcast-steps",
        type=str,
        default=None,
        help="Nowcast lead hours: a complete day requires all steps 1-24 (default: all)",
    )
    parser.add_argument(
        "--lat-bounds", type=float, nargs=2, default=LAT_LON_BOUNDS["lat"], metavar=("SOUTH", "NORTH"),
        help="NetCDF crop latitude box (default: 37 39)",
    )
    parser.add_argument(
        "--lon-bounds", type=float, nargs=2, default=LAT_LON_BOUNDS["lon"], metavar=("WEST", "EAST"),
        help="NetCDF crop longitude box in degrees east, 0-360 (default: 236 239)",
    )
    parser.add_argument("--overwrite", action="store_true", help="Rebuild NetCDF subsets or replace native files")
    parser.add_argument("--dry-run", action="store_true",
                        help="Inspect source objects and transfer sizes without writing files")
    parser.add_argument(
        "--add-vel",
        action="store_true",
        help="NetCDF only: ensure subset files include velocity variables",
    )
    processing_mode = parser.add_mutually_exclusive_group()
    processing_mode.add_argument(
        "--process-only", action="store_true",
        help="Process existing WCOFS subsets into hourly PST files without downloading",
    )
    processing_mode.add_argument(
        "--skip-processing", action="store_true", help="Download NetCDF subsets without hourly processing",
    )
    parser.add_argument("--process-start", type=str, default=None, help="First PST date to process (YYYY-MM-DD)")
    parser.add_argument("--process-end", type=str, default=None, help="Last PST date to process (YYYY-MM-DD)")
    parser.add_argument(
        "--list-dates",
        action="store_true",
        help="List available dates (requires internet)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )
    parser.add_argument(
        "--logdir",
        type=str,
        default="logs",
        help="Directory for log files (default: logs)",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress console logging",
    )

    args = parser.parse_args(argv)

    if bool(args.start) != bool(args.end):
        parser.error("--start and --end must be supplied together")
    if args.date and (args.start or args.end):
        parser.error("Use --date or --start/--end, not both")
    if args.dry_run and (args.process_only or args.list_dates):
        parser.error("--dry-run cannot be combined with --process-only or --list-dates")
    if args.native_grid:
        if args.forecast_type != ["wcofs"] or args.format != "netcdf":
            parser.error("--native-grid requires wcofs and --format netcdf")
        if (args.nowcast_elevation or args.nowcast_steps or args.process_only
                or args.process_start or args.process_end or args.list_dates or args.add_vel):
            parser.error("--native-grid cannot be combined with nowcast, processing, list-dates, or --add-vel options")
        if tuple(args.lat_bounds) != LAT_LON_BOUNDS["lat"] or tuple(args.lon_bounds) != LAT_LON_BOUNDS["lon"]:
            parser.error("--native-grid downloads full files and does not support crop bounds")

    setup_logging(args.debug, quiet=args.quiet, logdir=None if args.dry_run else Path(args.logdir))

    if args.nowcast_elevation:
        if args.forecast_type != ["wcofs"]:
            parser.error("--nowcast-elevation can only be used with the wcofs forecast type")
        if args.process_only or args.list_dates:
            parser.error("--nowcast-elevation cannot be combined with --process-only or --list-dates")
        if args.skip_processing:
            parser.error("--nowcast-elevation always writes complete daily files and cannot use --skip-processing")
        if args.forecast_steps:
            parser.error("Use --nowcast-steps instead of --forecast-steps with --nowcast-elevation")
        if args.add_vel:
            parser.error("--add-vel is not supported with --nowcast-elevation")
    elif args.nowcast_steps:
        parser.error("--nowcast-steps requires --nowcast-elevation")
    if args.add_vel and args.format != "netcdf":
        parser.error("--add-vel requires --format netcdf")
    
    # Initialize S3 downloader
    s3_downloader = S3Downloader()
    
    # Parse forecast steps argument
    forecast_steps = None
    if args.forecast_steps:
        try:
            parts = [int(part) for part in args.forecast_steps.split(":")]
            if len(parts) == 1:
                forecast_steps = range(parts[0], parts[0] + 1)
            elif len(parts) in (2, 3):
                stride = parts[2] if len(parts) == 3 else 1
                if stride < 1:
                    raise ValueError
                forecast_steps = range(parts[0], parts[1] + 1, stride)
            else:
                raise ValueError
        except ValueError:
            logger.error(f"Invalid forecast steps format: {args.forecast_steps}. Use 'START:END[:STRIDE]' or single step.")
            sys.exit(1)
        if args.format == "netcdf":
            forecast_steps = [step for step in forecast_steps if step in NETCDF_STEPS]
            if not forecast_steps:
                logger.error("NetCDF forecast steps must include lead hours 3, 6, ..., 72")
                sys.exit(1)
        logger.debug(f"Forecast steps: {list(forecast_steps)}")

    nowcast_steps = None
    if args.nowcast_steps:
        try:
            parts = [int(part) for part in args.nowcast_steps.split(":")]
            if len(parts) == 1:
                nowcast_steps = [parts[0]]
            elif len(parts) in (2, 3):
                stride = parts[2] if len(parts) == 3 else 1
                if stride < 1:
                    raise ValueError
                nowcast_steps = list(range(parts[0], parts[1] + 1, stride))
            else:
                raise ValueError
        except ValueError:
            parser.error(
                f"Invalid nowcast steps format: {args.nowcast_steps}. "
                "Use 'START:END[:STRIDE]' or a single step."
            )
        if not nowcast_steps or any(step not in NOWCAST_STEPS for step in nowcast_steps):
            parser.error("WCOFS nowcast steps must be lead hours 1 through 24")
        if not set(NOWCAST_STEPS).issubset(nowcast_steps):
            parser.error(
                "A complete PST day requires every nowcast lead hour 1 through 24; "
                "omit --nowcast-steps or use --nowcast-steps 1:24"
            )
        logger.debug(f"WCOFS nowcast steps: {nowcast_steps}")

    process_start = process_end = None
    if bool(args.process_start) != bool(args.process_end):
        logger.error("--process-start and --process-end must be supplied together")
        sys.exit(1)
    if args.process_start:
        try:
            process_start = datetime.strptime(args.process_start, "%Y-%m-%d")
            process_end = datetime.strptime(args.process_end, "%Y-%m-%d")
        except ValueError:
            logger.error("Invalid processing date format. Use YYYY-MM-DD")
            sys.exit(1)

    wcofs_dir = Path(args.output) / "wcofs"
    if args.process_only:
        if args.date or args.start or args.end or args.list_dates:
            logger.error("--process-only uses --process-start/--process-end, not download dates or --list-dates")
            sys.exit(1)
        try:
            process_wcofs(wcofs_dir / "subset", wcofs_dir / "processed", process_start, process_end)
        except (OSError, ValueError) as error:
            logger.error(f"WCOFS processing failed: {error}")
            sys.exit(1)
        sys.exit(0)

    # Parse dates
    dates_to_download = []
    
    # Check if date range is specified
    if args.start and args.end:
        try:
            start_date = datetime.strptime(args.start, "%Y-%m-%d")
            end_date = datetime.strptime(args.end, "%Y-%m-%d")
            
            if start_date > end_date:
                logger.error("Start date cannot be after end date")
                sys.exit(1)
            # Generate list of dates from start to end (inclusive)
            current_date = start_date
            while current_date <= end_date:
                dates_to_download.append(current_date)
                current_date += timedelta(days=1)
            
            logger.info(f"Date range: {start_date.date()} to {end_date.date()} ({len(dates_to_download)} days)")
        except ValueError:
            logger.error("Invalid date format. Use YYYY-MM-DD")
            sys.exit(1)
    elif args.date:
        try:
            date = datetime.strptime(args.date, "%Y-%m-%d")
            dates_to_download = [date]
        except ValueError:
            logger.error("Invalid date format. Use YYYY-MM-DD")
            sys.exit(1)
    else:
        dates_to_download = [datetime.utcnow()]

    output_dir = Path(args.output)

    # List available dates if requested
    if args.list_dates:
        for forecast_type in set(args.forecast_type):
            list_available_dates(s3_downloader, forecast_type)
        return

    # NetCDF WCOFS also needs the previous day's cycle for the start of each date
    wcofs_netcdf = (
        "wcofs" in args.forecast_type
        and args.format == "netcdf"
        and not args.nowcast_elevation
        and not args.native_grid
    )
    netcdf_dates = sorted(set(dates_to_download) | {date - timedelta(days=1) for date in dates_to_download})

    # Download data
    success = True
    wcofs_success = True
    total_files = sum(
        len(netcdf_dates)
        if forecast_type == "wcofs" and wcofs_netcdf
        else len(dates_to_download)
        for forecast_type in args.forecast_type
    )
    downloaded_files = 0
    
    for forecast_type in args.forecast_type:
        logger.info(f"\n{'='*60}")
        logger.info(f"Downloading {forecast_type.upper()}")
        logger.info(f"{'='*60}")

        forecast_dir = output_dir / forecast_type
        type_dates = (
            netcdf_dates
            if forecast_type == "wcofs" and wcofs_netcdf
            else dates_to_download
        )
        
        for date in type_dates:
            downloaded_files += 1
            date_str = date.strftime("%Y-%m-%d")
            logger.info(f"[{downloaded_files}/{total_files}] Downloading {forecast_type.upper()} for {date_str}")

            if args.dry_run:
                try:
                    result = plan_downloads(
                        s3_downloader, forecast_type, date, args.format,
                        forecast_steps, args.nowcast_elevation, args.native_grid,
                    )
                except (BotoCoreError, ClientError, OSError) as error:
                    logger.error("Could not inspect S3 objects: %s", error)
                    result = False
            elif forecast_type == "wcofs":
                if args.native_grid:
                    result = download_wcofs_native(
                        forecast_dir, s3_downloader, date, forecast_steps=forecast_steps,
                        overwrite=args.overwrite,
                    )
                elif args.nowcast_elevation:
                    result = download_wcofs_nowcast_elevation(
                        forecast_dir,
                        s3_downloader,
                        date=date,
                        nowcast_steps=nowcast_steps,
                        lat_bounds=args.lat_bounds,
                        lon_bounds=args.lon_bounds,
                        overwrite=args.overwrite,
                    )
                else:
                    result = download_wcofs(
                        forecast_dir,
                        s3_downloader,
                        date=date,
                        file_format=args.format,
                        forecast_steps=forecast_steps,
                        lat_bounds=args.lat_bounds,
                        lon_bounds=args.lon_bounds,
                        overwrite=args.overwrite,
                        add_vel=args.add_vel,
                    )
                if not result:
                    wcofs_success = False
            elif forecast_type == "rtofs":
                result = download_rtofs(
                    forecast_dir,
                    s3_downloader,
                    date=date,
                    file_format=args.format,
                )

            if not result:
                success = False
                logger.warning(f"Failed to download {forecast_type.upper()} for {date_str}")

    if wcofs_netcdf and not args.skip_processing and not args.dry_run:
        if wcofs_success:
            try:
                process_wcofs(wcofs_dir / "subset", wcofs_dir / "processed", process_start, process_end)
            except (OSError, ValueError) as error:
                logger.error(f"WCOFS processing failed: {error}")
                success = False
        else:
            logger.warning("Skipping WCOFS processing because some downloads failed; rerun with --process-only")

    if success:
        logger.info("Dry run completed successfully" if args.dry_run else "\n✓ Downloads completed successfully")
        sys.exit(0)
    else:
        logger.error("\n✗ Some downloads failed")
        sys.exit(1)


@click.command(
    context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
    add_help_option=False,
)
@click.pass_context
def download_wcofs_cli(ctx):
    """Download WCOFS and RTOFS forecasts."""
    main(ctx.args)


if __name__ == "__main__":
        main()
