#!/usr/bin/env python3
"""Download CONUS RTMA hourly analyses and write cropped daily NetCDF files.

The NOAA archive supplies 2.5-km native NDFD grid analyses, not forecasts.
Daily output follows download_rrfs's Pacific-civil-day grid/time layout, but
only includes fields present in RTMA: stmp, spfh, uwind, vwind, and pres.
Surface pressure is not mean-sea-level pressure; RTMA does not supply the
RRFS precipitation-rate or radiation fields in its 2dvaranl product.
"""

import logging
from contextlib import nullcontext
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from zoneinfo import ZoneInfo

import click
from botocore.exceptions import BotoCoreError, ClientError

try:
    from dms_datastore.download_rrfs import (
        HRRR_LAT_BOUNDS, HRRR_LON_BOUNDS, S3Downloader, crop_indices,
        parse_grib_index, select_byte_ranges,
    )
    from dms_datastore.logging_config import configure_logging, resolve_loglevel
except ModuleNotFoundError as error:  # pragma: no cover - direct script execution
    if error.name not in ("dms_datastore", "dms_datastore.download_rrfs", "dms_datastore.logging_config"):
        raise
    from download_rrfs import (
        HRRR_LAT_BOUNDS, HRRR_LON_BOUNDS, S3Downloader, crop_indices,
        parse_grib_index, select_byte_ranges,
    )
    from logging_config import configure_logging, resolve_loglevel


BUCKET = "noaa-rtma-pds"
PACIFIC = ZoneInfo("America/Los_Angeles")
FIELD_PATTERNS = {
    "pres": r":PRES:surface:anl:",
    "stmp": r":TMP:2 m above ground:anl:",
    "spfh": r":SPFH:2 m above ground:anl:",
    "uwind": r":UGRD:10 m above ground:anl:",
    "vwind": r":VGRD:10 m above ground:anl:",
}
SHORT_NAMES = {"sp": "pres", "2t": "stmp", "2sh": "spfh", "10u": "uwind", "10v": "vwind"}
logger = logging.getLogger("dms_datastore.download_rtma" if __name__ == "__main__" else __name__)


def rtma_key(valid_time):
    """Return the archive key for a UTC hourly CONUS analysis."""
    if valid_time.tzinfo is None or valid_time.utcoffset() is None:
        raise ValueError("valid_time must be timezone-aware")
    valid_time = valid_time.astimezone(timezone.utc)
    if valid_time.minute or valid_time.second or valid_time.microsecond:
        raise ValueError("RTMA analysis time must be on the hour")
    return (f"rtma2p5.{valid_time:%Y%m%d}/"
            f"rtma2p5.t{valid_time:%H}z.2dvaranl_ndfd.grb2_wexp")


def pacific_day_hours(day):
    """Return UTC instants for a complete Pacific civil day, including DST."""
    start = datetime.combine(day, time.min, tzinfo=PACIFIC).astimezone(timezone.utc)
    end = datetime.combine(day + timedelta(days=1), time.min, tzinfo=PACIFIC).astimezone(timezone.utc)
    return [start + timedelta(hours=hour) for hour in range(int((end - start).total_seconds() // 3600))]


def _selected_ranges(downloader, valid_time):
    key = rtma_key(valid_time)
    size = downloader.object_size(key)
    if size is None:
        raise FileNotFoundError(f"RTMA source not found: s3://{BUCKET}/{key}")
    index = downloader.read_text(key + ".idx")
    records = parse_grib_index(index)
    expected_time = f"d={valid_time:%Y%m%d%H}"
    if not records or any(record["reference_time"] != expected_time for record in records):
        raise ValueError(f"Missing or inconsistent analysis timestamps in index for {key}")
    ranges, unmatched = select_byte_ranges(index, size, list(FIELD_PATTERNS.values()))
    if unmatched:
        raise ValueError(f"Missing RTMA records in {key}: {', '.join(unmatched)}")
    return key, ranges, sum(end - start + 1 for start, end in ranges)


def read_rtma_fields(grib_path, valid_time, lat_bounds, lon_bounds):
    """Decode and crop selected records, checking the GRIB analysis time."""
    import numpy as np
    from eccodes import codes_get, codes_get_array, codes_get_values, codes_new_from_message, codes_release

    fields = {}
    grid_lat = grid_lon = None
    rows = columns = grid_shape = None
    wind_projection = wind_relative = None
    with open(grib_path, "rb") as stream:
        while True:
            header = stream.read(16)
            if not header:
                break
            if len(header) != 16 or header[:4] != b"GRIB" or header[7] != 2:
                raise ValueError(f"Invalid GRIB2 header in {grib_path}")
            length = int.from_bytes(header[8:16], "big")
            if length < 20:
                raise ValueError(f"Invalid GRIB2 message length in {grib_path}")
            body = stream.read(length - 16)
            if len(body) != length - 16:
                raise ValueError(f"Truncated GRIB2 message in {grib_path}")
            handle = codes_new_from_message(header + body)
            try:
                short_name = codes_get(handle, "shortName")
                name = SHORT_NAMES.get(short_name)
                if name is None or name in fields:
                    raise ValueError(f"Unexpected or duplicate GRIB field {short_name} in {grib_path}")
                actual = (codes_get(handle, "validityDate"), codes_get(handle, "validityTime"))
                expected = (int(valid_time.strftime("%Y%m%d")), valid_time.hour * 100)
                if actual != expected:
                    raise ValueError(f"GRIB valid time {actual} does not match {expected} in {grib_path}")
                if name in ("uwind", "vwind"):
                    relative = codes_get(handle, "uvRelativeToGrid")
                    projection = None
                    if relative:
                        if codes_get(handle, "gridType") != "lambert":
                            raise ValueError(f"Unsupported grid-relative wind projection in {grib_path}")
                        projection = tuple(codes_get(handle, key) for key in (
                            "LoVInDegrees", "LaDInDegrees", "Latin1InDegrees",
                            "Latin2InDegrees", "radius",
                        ))
                    if wind_relative is not None and (wind_relative != relative or wind_projection != projection):
                        raise ValueError(f"Wind components have different coordinate systems in {grib_path}")
                    wind_relative, wind_projection = relative, projection
                shape = (codes_get(handle, "Nj"), codes_get(handle, "Ni"))
                lat = codes_get_array(handle, "latitudes").reshape(shape)
                lon = codes_get_array(handle, "longitudes").reshape(shape)
                if rows is None:
                    rows, columns = crop_indices(lat, lon, lat_bounds, lon_bounds)
                    grid_shape = shape
                    grid_lat = lat[rows, columns].astype("float32")
                    grid_lon = ((lon[rows, columns] + 180) % 360 - 180).astype("float32")
                elif shape != grid_shape or not (
                    np.allclose(lat[rows, columns], grid_lat)
                    and np.allclose(((lon[rows, columns] + 180) % 360 - 180), grid_lon)
                ):
                    raise ValueError(f"RTMA fields use different grids in {grib_path}")
                values = codes_get_values(handle).astype("float32")
                if codes_get(handle, "bitmapPresent"):
                    values[values == codes_get(handle, "missingValue")] = np.nan
                fields[name] = values.reshape(shape)[rows, columns]
            finally:
                codes_release(handle)
    missing = set(FIELD_PATTERNS) - fields.keys()
    if missing:
        raise ValueError(f"Missing RTMA fields in {grib_path}: {', '.join(sorted(missing))}")
    if wind_relative:
        from pyproj import Proj

        lov, lad, latin1, latin2, radius = wind_projection
        projection = Proj(proj="lcc", lon_0=lov, lat_0=lad, lat_1=latin1,
                          lat_2=latin2, R=radius)
        convergence = np.deg2rad(projection.get_factors(grid_lon, grid_lat).meridian_convergence)
        cos_angle, sin_angle = np.cos(convergence), np.sin(convergence)
        u, v = fields["uwind"], fields["vwind"]
        fields["uwind"] = (u * cos_angle + v * sin_angle).astype("float32")
        fields["vwind"] = (-u * sin_angle + v * cos_angle).astype("float32")
    return fields, grid_lat, grid_lon


def write_rtma_day(output_path, day, valid_times, samples, lat, lon, lat_bounds, lon_bounds):
    """Write a complete daily RTMA dataset with the RRFS grid and time layout."""
    import numpy as np
    import xarray as xr

    if valid_times != pacific_day_hours(day) or len(samples) != len(valid_times):
        raise ValueError("RTMA output requires every hourly analysis in the Pacific day")
    start = valid_times[0]
    midnight = datetime.combine(day, time.min, tzinfo=PACIFIC)
    offset = midnight.strftime("%z")
    offset = f"{offset[:3]}:{offset[3:]}"
    dims = ("time", "ny_grid", "nx_grid")
    dataset = xr.Dataset(
        {name: (dims, np.stack([sample[name] for sample in samples]).astype("float32"))
         for name in FIELD_PATTERNS},
        coords={
            "time": np.round(np.array([(t - start).total_seconds() / 86400 for t in valid_times],
                                       dtype="float32"), 6),
            "lat": (("ny_grid", "nx_grid"), lat),
            "lon": (("ny_grid", "nx_grid"), lon),
        },
    )
    dataset.time.attrs = {
        "long_name": "Time", "standard_name": "time",
        "base_date": [day.year, day.month, day.day, 0, 0],
        "units": f"days since {day:%Y-%m-%d} 00:00:00 {offset}",
        "timezone": "America/Los_Angeles",
    }
    dataset.lat.attrs = {"units": "degrees_north", "standard_name": "latitude", "long_name": "Latitude"}
    dataset.lon.attrs = {"units": "degrees_east", "standard_name": "longitude", "long_name": "Longitude"}
    dataset.stmp.attrs = {"units": "K", "long_name": "2m_above_ground/Temperature"}
    dataset.spfh.attrs = {"units": "kg kg-1", "standard_name": "specific_humidity", "long_name": "2m_above_ground/Specific Humidity"}
    dataset.uwind.attrs = {"units": "m/s", "standard_name": "eastward_wind", "long_name": "10m_above_ground/UGRD"}
    dataset.vwind.attrs = {"units": "m/s", "standard_name": "northward_wind", "long_name": "10m_above_ground/VGRD"}
    dataset.pres.attrs = {"units": "Pa", "standard_name": "surface_air_pressure", "long_name": "Surface pressure"}
    dataset.attrs = {
        "creator_institution": "NOAA", "program": "RTMA",
        "title": "Subsetted RTMA atmospheric analyses",
        "source_bucket": BUCKET, "source_product": "rtma2p5 2dvaranl_ndfd",
        "valid_date_pacific": day.isoformat(), "valid_timezone": "America/Los_Angeles",
        "geospatial_lat_min": lat_bounds[0], "geospatial_lat_max": lat_bounds[1],
        "geospatial_lon_min": lon_bounds[0], "geospatial_lon_max": lon_bounds[1],
    }
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    partial = output_path.with_name(output_path.name + ".part")
    try:
        dataset.to_netcdf(partial, engine="netcdf4", format="NETCDF4", unlimited_dims="time")
        partial.replace(output_path)
    finally:
        dataset.close()
        partial.unlink(missing_ok=True)
    logger.info("Saved RTMA daily analysis: %s (%d hours)", output_path, len(valid_times))
    return output_path


def _existing_output_matches(path, day, lat_bounds, lon_bounds):
    """Check that an existing daily file matches the requested data and box."""
    import numpy as np
    import xarray as xr

    try:
        with xr.open_dataset(path, decode_times=False) as dataset:
            attributes = {
                "source_bucket": BUCKET, "valid_date_pacific": day.isoformat(),
                "geospatial_lat_min": lat_bounds[0], "geospatial_lat_max": lat_bounds[1],
                "geospatial_lon_min": lon_bounds[0], "geospatial_lon_max": lon_bounds[1],
            }
            hours = len(pacific_day_hours(day))
            return (
                all(dataset.attrs.get(key) == value for key, value in attributes.items())
                and set(FIELD_PATTERNS).issubset(dataset.data_vars)
                and {"time", "lat", "lon"}.issubset(dataset.coords)
                and dataset.sizes.get("time") == hours
                and dataset.time.dims == ("time",)
                and np.allclose(dataset.time.values, np.arange(hours) / 24, atol=1e-6)
                and all(dataset[name].dims == ("time", "ny_grid", "nx_grid") for name in FIELD_PATTERNS)
                and dataset.lat.dims == dataset.lon.dims == ("ny_grid", "nx_grid")
            )
    except (OSError, ValueError, KeyError) as error:
        logger.warning("Cannot validate existing RTMA file %s: %s", path, error)
        return False


def download_rtma(output_dir, downloader, day, lat_bounds=HRRR_LAT_BOUNDS,
                  lon_bounds=HRRR_LON_BOUNDS, overwrite=False, dry_run=False):
    """Download and convert a complete Pacific civil day of CONUS RTMA analyses.

    Parameters
    ----------
    output_dir : path-like
        Output root; files are written to YYYY/rtma_YYYYMMDD00.nc.
    downloader : S3Downloader
        Unsigned client for noaa-rtma-pds.
    day : datetime.date
        Pacific civil date to download.
    lat_bounds, lon_bounds : pair of float
        Latitude and longitude crop bounds, matching RRFS defaults.
    overwrite, dry_run : bool
        Replace an existing output, or inspect source sizes without writing.

    Returns
    -------
    bool
        True only if the existing or newly written daily output is complete.
    """
    import numpy as np

    south, north = lat_bounds
    west, east = lon_bounds
    if not (-90 <= south < north <= 90 and 0 <= west % 360 < east % 360):
        raise ValueError("Bounds must satisfy SOUTH < NORTH and WEST < EAST without crossing 0 longitude")
    if downloader.bucket != BUCKET:
        raise ValueError(f"RTMA downloader must use bucket {BUCKET}")
    output_path = Path(output_dir) / f"{day:%Y}" / f"rtma_{day:%Y%m%d}00.nc"
    if output_path.exists() and not overwrite and not dry_run:
        if _existing_output_matches(output_path, day, lat_bounds, lon_bounds):
            logger.info("Already available: %s", output_path)
            return True
        logger.warning("Existing file does not match the requested RTMA day and bounds; rebuilding: %s", output_path)
    valid_times = pacific_day_hours(day)
    samples = []
    grid_lat = grid_lon = None
    total = 0
    try:
        with (nullcontext(None) if dry_run else TemporaryDirectory(prefix="rtma_")) as temporary:
            for valid_time in valid_times:
                key, ranges, size = _selected_ranges(downloader, valid_time)
                total += size
                logger.info("s3://%s/%s (%.1f MB selected)", BUCKET, key, size / 1e6)
                if dry_run:
                    continue
                path = Path(temporary) / "analysis.grib2"
                downloader.download(key, path, ranges)
                if path.stat().st_size != size:
                    raise OSError(f"Downloaded size mismatch for {key}")
                fields, lat, lon = read_rtma_fields(path, valid_time, lat_bounds, lon_bounds)
                if grid_lat is None:
                    grid_lat, grid_lon = lat, lon
                elif lat.shape != grid_lat.shape or not (
                    np.allclose(lat, grid_lat) and np.allclose(lon, grid_lon)
                ):
                    raise ValueError(f"RTMA grid differs between hours: {key}")
                samples.append(fields)
            logger.info("RTMA %s: %d hourly analyses, %.1f MB selected", day, len(valid_times), total / 1e6)
            if not dry_run:
                write_rtma_day(output_path, day, valid_times, samples, grid_lat, grid_lon, lat_bounds, lon_bounds)
    except (BotoCoreError, ClientError, OSError, ValueError, KeyError) as error:
        logger.error("RTMA download failed for %s: %s", day, error)
        return False
    return True


@click.command(context_settings={"help_option_names": ["-h", "--help"]},
               help="Download hourly CONUS RTMA analyses as daily Pacific-time NetCDF files.")
@click.option("-d", "--date", type=click.DateTime(formats=["%Y-%m-%d"]),
              help="Pacific date (default: yesterday in Pacific time).")
@click.option("-sdate", "--start", type=click.DateTime(formats=["%Y-%m-%d"]),
              help="First Pacific date, inclusive.")
@click.option("-edate", "--end", type=click.DateTime(formats=["%Y-%m-%d"]),
              help="Last Pacific date, inclusive.")
@click.option("-o", "--dest", "output_dir", type=click.Path(path_type=Path), default=Path("rtma_data"),
              show_default=True, help="Output directory.")
@click.option("--lat-bounds", type=(float, float), default=HRRR_LAT_BOUNDS, show_default=True)
@click.option("--lon-bounds", type=(float, float), default=HRRR_LON_BOUNDS, show_default=True)
@click.option("--overwrite", is_flag=True, help="Replace existing daily files.")
@click.option("--dry-run", is_flag=True, help="Inspect selected source records and sizes without writing.")
@click.option("--logdir", type=click.Path(path_type=Path), default=Path("logs"), show_default=True)
@click.option("--debug", is_flag=True)
@click.option("--quiet", is_flag=True)
def download_rtma_cli(date, start, end, output_dir, lat_bounds, lon_bounds,
                      overwrite, dry_run, logdir, debug, quiet):
    """Write stmp, spfh, uwind, vwind, and surface pres, not full RRFS forcing."""
    if bool(start) != bool(end):
        raise click.UsageError("--start and --end must be supplied together")
    if date and start:
        raise click.UsageError("Use --date or --start/--end, not both")
    if start and start > end:
        raise click.UsageError("--start must not be after --end")
    south, north = lat_bounds
    west, east = lon_bounds
    if not (-90 <= south < north <= 90 and 0 <= west % 360 < east % 360):
        raise click.UsageError("Bounds must satisfy SOUTH < NORTH and WEST < EAST without crossing 0 longitude")
    if debug and quiet:
        raise click.UsageError("--debug and --quiet cannot be combined")
    level, console = resolve_loglevel(debug=debug, quiet=quiet)
    configure_logging(package_name="dms_datastore", level=level, console=console,
                      logdir=None if dry_run else logdir, logfile_prefix="download_rtma")
    first = (start or date).date() if start or date else datetime.now(PACIFIC).date() - timedelta(days=1)
    last = end.date() if end else first
    downloader = S3Downloader(BUCKET)
    success = True
    for offset in range((last - first).days + 1):
        day = first + timedelta(days=offset)
        if not download_rtma(output_dir, downloader, day, lat_bounds, lon_bounds, overwrite, dry_run):
            success = False
    if not success:
        raise click.ClickException("One or more RTMA days failed; see log for details")


main = download_rtma_cli

if __name__ == "__main__":
    main()