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
The --nowcast-elevation option downloads WCOFS 2ds.nNNN files, crops them to
the same box, and combines their elevation and grid data into one hourly
fixed-PST file for each complete Pacific day.

References:
    - WCOFS S3: https://noaa-nos-ofs-pds.s3.amazonaws.com/wcofs/
    - RTOFS S3: https://noaa-nos-ofs-pds.s3.amazonaws.com/rtofs/ (check availability)
    - AWS S3 Explorer: https://noaa-nos-ofs-pds.s3.amazonaws.com/index.html
    - NOAA Open Data: https://www.noaa.gov/organization/information-technology/open-data-dissemination
"""

import os
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
import argparse
import logging
import boto3
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError


BUCKET = "noaa-nos-ofs-pds"
LAT_LON_BOUNDS = {"lat": (37, 39), "lon": (236, 239)}
NETCDF_STEPS = range(3, 73, 3)
NOWCAST_STEPS = range(1, 25)
NETCDF_NAME = re.compile(r"wcofs\.t(\d{2})z\.(\d{8})\.regulargrid\.f(\d{3})\.nc$")


def setup_logging(debug: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
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
            logging.info(f"Downloading: s3://{bucket}/{key}")
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'wb') as f:
                self.s3.download_fileobj(bucket, key, f)
            
            logging.info(f"Saved to: {output_path}")
            return True
        except Exception as e:
            logging.error(f"S3 download failed: {e}")
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
            lat_bounds, lon_bounds, overwrite,
        )

    round_str = f"{round:02d}"

    output_dir.mkdir(parents=True, exist_ok=True)

    # Format date strings for S3 path
    date_str = date.strftime("%Y%m%d")
    year_str = date.strftime("%Y")
    month_str = date.strftime("%m")
    day_str = date.strftime("%d")
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
            s3_key = f"wcofs/grib2/{year_str}/{month_str}/{day_str}/{filename}"
            
            output_path = output_dir / filename
            
            # Check if file exists on S3
            if not s3_downloader.file_exists(bucket, s3_key):
                logging.warning(f"File not found on S3: {s3_key}")
                failed_count += 1
                continue
            
            if s3_downloader.download_file(bucket, s3_key, output_path):
                downloaded_count += 1
            else:
                failed_count += 1
        
        if downloaded_count > 0:
            logging.info(f"Downloaded {downloaded_count}/{len(list(forecast_steps))} files")
            return True
        else:
            logging.error(f"Failed to download any files")
            return False

    except Exception as e:
        logging.error(f"WCOFS download error: {e}")
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
    year_str = date.strftime("%Y")
    month_str = date.strftime("%m")
    day_str = date.strftime("%d")
    round_str = f"{round:02d}"

    # RTOFS S3 bucket and path
    bucket = "noaa-nos-ofs-pds"

    try:
        # RTOFS file naming and path structure
        if file_format == "grib2":
            filename = f"rtofs.t{round_str}z.{date_str}.grb2"
            s3_key = f"rtofs/grib2/{year_str}/{month_str}/{day_str}/{filename}"
        else:
            filename = f"rtofs.t{round_str}z.{date_str}.nc"
            s3_key = f"rtofs/netcdf/{year_str}/{month_str}/{day_str}/{filename}"
        
        output_path = output_dir / filename
        
        # Check if file exists on S3
        if not s3_downloader.file_exists(bucket, s3_key):
            logging.warning(f"File not found on S3: {s3_key}")
            return False
        
        return s3_downloader.download_file(bucket, s3_key, output_path)

    except Exception as e:
        logging.error(f"RTOFS download error: {e}")
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
        logging.info(f"Listing available {forecast_type.upper()} dates from S3...")
        logging.info(f"S3 Bucket: {bucket}")
        logging.info(f"Prefix: {prefix}")
        
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
            logging.info(f"\nRecent dates available (last {days_back} days):")
            for date in sorted(sorted_dates):
                logging.info(f"  {date}")
        else:
            logging.warning("No dates found")
            
    except Exception as e:
        logging.error(f"Error listing dates: {e}")


def subset_wcofs_dataset(
    data,
    lat_bounds=LAT_LON_BOUNDS["lat"],
    lon_bounds=LAT_LON_BOUNDS["lon"],
):
    """Crop a WCOFS regular-grid dataset to a latitude/longitude box.

    Parameters
    ----------
    data : xarray.Dataset
        WCOFS regular-grid file with rectilinear 2-D Latitude/Longitude and
        temp, salt, and zeta.
    lat_bounds : pair of float, optional
        Inclusive south/north limits in degrees north, default (37, 39).
    lon_bounds : pair of float, optional
        Inclusive west/east limits in degrees east on a 0-360 convention,
        default (236, 239). Source longitudes may use either convention.

    Returns
    -------
    xarray.Dataset
        temp and salt (time, Depth, lat, lon) and zeta (time, lat, lon) on
        1-D lat and lon (0-360 degrees east) coordinates, at all depths.
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
    subset = data[["temp", "salt", "zeta"]].isel({y_dim: rows, x_dim: columns})
    subset = subset.drop_vars(["Latitude", "Longitude"], errors="ignore").rename({y_dim: "lat", x_dim: "lon"})
    subset = subset.assign_coords(lat=lat[rows], lon=lon[columns])
    subset.attrs = dict(data.attrs)
    subset.attrs["subset_lat_bounds"] = list(lat_bounds)
    subset.attrs["subset_lon_bounds"] = list(lon_bounds)
    return subset


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
    source_files = (
        [(date + pd.Timedelta(days=1), step) for step in range(5, 25)]
        + [(date + pd.Timedelta(days=2), step) for step in range(1, 5)]
    )
    subset_dir = Path(output_dir) / "nowcast_elevation"
    output_path = subset_dir / f"wcofs_nowcast_elevation_pst{date:%Y%m%d}.nc"
    source_keys = [
        f"wcofs/netcdf/{source_date:%Y/%m/%d}/"
        f"wcofs.t{round:02d}z.{source_date:%Y%m%d}.2ds.n{step:03d}.nc"
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
                    logging.info(f"Already available: {output_path}")
                    return True
        except (OSError, ValueError):
            logging.warning(f"Cannot read existing daily file; rebuilding: {output_path}")

    success = True
    samples = []
    for source_date, step in source_files:
        filename = f"wcofs.t{round:02d}z.{source_date:%Y%m%d}.2ds.n{step:03d}.nc"
        key = (
            f"wcofs/netcdf/{source_date:%Y/%m/%d}/{filename}"
        )
        size = s3_downloader.object_size(BUCKET, key)
        if size is None:
            logging.warning(f"File not found on S3: {key}")
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
            logging.error(f"Nowcast elevation subsetting failed for {key}: {error}")
            success = False
    if not success:
        logging.error(
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
    logging.info("Saved 24 hourly fixed-PST nowcast samples to: %s", output_path)
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
        key = f"wcofs/netcdf/{date:%Y/%m/%d}/{filename}"
        output_path = subset_dir / filename
        size = s3_downloader.object_size(BUCKET, key)
        if size is None:
            logging.warning(f"File not found on S3: {key}")
            success = False
            continue
        metadata = {"subset_source_key": key, "subset_source_size": str(size), "subset_version": 1}
        if not overwrite and output_path.is_file():
            try:
                with xr.open_dataset(output_path, decode_times=False) as existing:
                    if (
                        all(existing.attrs.get(name) == value for name, value in metadata.items())
                        and list(existing.attrs.get("subset_lat_bounds", [])) == list(lat_bounds)
                        and list(existing.attrs.get("subset_lon_bounds", [])) == list(lon_bounds)
                    ):
                        logging.info(f"Already subsetted: {output_path}")
                        continue
            except (OSError, ValueError):
                logging.warning(f"Cannot read existing subset; rebuilding: {output_path}")
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
                    subset = subset_wcofs_dataset(data, lat_bounds, lon_bounds).load()
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
            logging.info(f"Saved geographic subset to: {output_path}")
        except (OSError, ValueError, KeyError) as error:
            logging.error(f"Subsetting failed for {key}: {error}")
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

    Notes
    -----
    PST is fixed UTC-8 without daylight saving. Valid times come from each
    file's internal time. Duplicate valid times prefer the newest cycle, so
    the previous day's f024 supplies 03Z of the following date. Days that are
    not bracketed by samples at 3-hour or finer spacing are skipped (or rejected
    when requested explicitly); no extrapolation is performed.
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
            logging.warning(f"{message}; skipping")
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
        logging.info(f"Saved 24 hourly PST samples to: {filename}")
    return written


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Download NOAA WCOFS and RTOFS forecast data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download latest WCOFS data
  python download_wcofs.py wcofs -o ./data

  # Download WCOFS from specific date
  python download_wcofs.py wcofs -d 2026-10-07 -o ./data
  
  # Download WCOFS regular-grid netcdf (f003-f072) for a date and the day before,
  # crop to the lat/lon box, and write hourly PST files to OUTPUT/wcofs/processed
  python download_wcofs.py wcofs -d 2026-10-13 -f netcdf -o ./data
  
  # Date range, lead hours 3-24 only
  python download_wcofs.py wcofs -sdate 2026-10-01 -edate 2026-10-07 -f netcdf -fs 3:24 -o ./data

  # Download one complete fixed-PST daily sea-surface-elevation file
  python download_wcofs.py wcofs -d 2026-11-01 --nowcast-elevation -o ./data

  # Nowcast mode requires the complete n001-n024 hourly sequence
  python download_wcofs.py wcofs -sdate 2026-11-01 -edate 2026-11-02 \\
      --nowcast-elevation --nowcast-steps 1:24 -o ./data
  
  # Reprocess existing subsets without downloading
  python download_wcofs.py wcofs --process-only -o ./data
  
  # Download both WCOFS and RTOFS for date range
  python download_wcofs.py wcofs rtofs -sdate 2026-10-01 -edate 2026-10-07 -o ./data

NetCDF notes:
  fNNN lead hours (003-072, every 3 h) count from the cycle start (t03z); each file's
  internal time is used. Each source file is ~555 MB, so one requested date (plus the
  previous day) transfers ~26 GB; only box subsets are kept in OUTPUT/wcofs/subset.
  Processed files hold temp, salt, surf_el (zeta) on the native 0.04 deg grid within
  the box, all depths, 24 hourly samples in fixed PST (UTC-8, no daylight saving).
  Incomplete days are skipped; no extrapolation is performed.

Nowcast elevation notes:
  --nowcast-elevation combines hourly 2ds.nNNN samples into one complete
  24-hour fixed-PST file per requested date, containing zeta, time, lon_rho,
  lat_rho, and mask_rho under OUTPUT/wcofs/nowcast_elevation. It uses the
  NOAA source files whose internal times cover that PST day; each source file
  is about 74 MB. All lead hours 1-24 are required for a complete day.
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
        "--output",
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
    parser.add_argument("--overwrite", action="store_true", help="Rebuild NetCDF subsets even if they already exist")
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

    args = parser.parse_args()

    setup_logging(args.debug)

    if args.nowcast_elevation:
        if args.forecast_type != ["wcofs"]:
            parser.error("--nowcast-elevation can only be used with the wcofs forecast type")
        if args.process_only or args.list_dates:
            parser.error("--nowcast-elevation cannot be combined with --process-only or --list-dates")
        if args.skip_processing:
            parser.error("--nowcast-elevation always writes complete daily files and cannot use --skip-processing")
        if args.forecast_steps:
            parser.error("Use --nowcast-steps instead of --forecast-steps with --nowcast-elevation")
    elif args.nowcast_steps:
        parser.error("--nowcast-steps requires --nowcast-elevation")
    
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
            logging.error(f"Invalid forecast steps format: {args.forecast_steps}. Use 'START:END[:STRIDE]' or single step.")
            sys.exit(1)
        if args.format == "netcdf":
            forecast_steps = [step for step in forecast_steps if step in NETCDF_STEPS]
            if not forecast_steps:
                logging.error("NetCDF regular-grid forecast steps must include lead hours 3, 6, ..., 72")
                sys.exit(1)
        logging.debug(f"Forecast steps: {list(forecast_steps)}")

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
        logging.debug(f"WCOFS nowcast steps: {nowcast_steps}")

    process_start = process_end = None
    if bool(args.process_start) != bool(args.process_end):
        logging.error("--process-start and --process-end must be supplied together")
        sys.exit(1)
    if args.process_start:
        try:
            process_start = datetime.strptime(args.process_start, "%Y-%m-%d")
            process_end = datetime.strptime(args.process_end, "%Y-%m-%d")
        except ValueError:
            logging.error("Invalid processing date format. Use YYYY-MM-DD")
            sys.exit(1)

    wcofs_dir = Path(args.output) / "wcofs"
    if args.process_only:
        if args.date or args.start or args.end or args.list_dates:
            logging.error("--process-only uses --process-start/--process-end, not download dates or --list-dates")
            sys.exit(1)
        try:
            process_wcofs(wcofs_dir / "subset", wcofs_dir / "processed", process_start, process_end)
        except (OSError, ValueError) as error:
            logging.error(f"WCOFS processing failed: {error}")
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
                logging.error("Start date cannot be after end date")
                sys.exit(1)
            # Generate list of dates from start to end (inclusive)
            current_date = start_date
            while current_date <= end_date:
                dates_to_download.append(current_date)
                current_date += timedelta(days=1)
            
            logging.info(f"Date range: {start_date.date()} to {end_date.date()} ({len(dates_to_download)} days)")
        except ValueError:
            logging.error("Invalid date format. Use YYYY-MM-DD")
            sys.exit(1)
    elif args.date:
        try:
            date = datetime.strptime(args.date, "%Y-%m-%d")
            dates_to_download = [date]
        except ValueError:
            logging.error("Invalid date format. Use YYYY-MM-DD")
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
    )
    netcdf_dates = sorted(set(dates_to_download) | {date - timedelta(days=1) for date in dates_to_download})

    # Download data
    success = True
    wcofs_success = True
    total_files = sum(
        len(netcdf_dates)
        if forecast_type == "wcofs" and args.format == "netcdf" and not args.nowcast_elevation
        else len(dates_to_download)
        for forecast_type in args.forecast_type
    )
    downloaded_files = 0
    
    for forecast_type in args.forecast_type:
        logging.info(f"\n{'='*60}")
        logging.info(f"Downloading {forecast_type.upper()}")
        logging.info(f"{'='*60}")

        forecast_dir = output_dir / forecast_type
        type_dates = (
            netcdf_dates
            if forecast_type == "wcofs" and args.format == "netcdf" and not args.nowcast_elevation
            else dates_to_download
        )
        
        for date in type_dates:
            downloaded_files += 1
            date_str = date.strftime("%Y-%m-%d")
            logging.info(f"[{downloaded_files}/{total_files}] Downloading {forecast_type.upper()} for {date_str}")

            if forecast_type == "wcofs":
                if args.nowcast_elevation:
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
                logging.warning(f"Failed to download {forecast_type.upper()} for {date_str}")

    if wcofs_netcdf and not args.skip_processing:
        if wcofs_success:
            try:
                process_wcofs(wcofs_dir / "subset", wcofs_dir / "processed", process_start, process_end)
            except (OSError, ValueError) as error:
                logging.error(f"WCOFS processing failed: {error}")
                success = False
        else:
            logging.warning("Skipping WCOFS processing because some downloads failed; rerun with --process-only")

    if success:
        logging.info("\n✓ Downloads completed successfully")
        sys.exit(0)
    else:
        logging.error("\n✗ Some downloads failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
