#!/usr/bin/env python3
"""Download historical ESTOFS t00z CONUS-west combined water levels.

The archive's estofs.YYYYMMDD GRIB2 files are converted to cropped NetCDF
subsets, and processed into hourly fixed-PST daily grids and forecast-run
point CSVs in NAVD88. ESTOFS provides water levels, not temperature or salinity.
The later stofs_2d_glo archive uses different names and is not included.
"""

import logging
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import boto3
import click
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from dms_datastore.download_rtofs import (
    ForecastStepsType,
    LAT_LON_BOUNDS,
    RTOFS_POINT_COORDS_UTM10N,
    _convert_rtofs_surface_to_navd88,
    _interpolate_rtofs_grid,
    _interpolate_rtofs_to_boundary,
    _validate_bounds,
)
from dms_datastore.logging_config import configure_logging, resolve_loglevel

BUCKET = "noaa-gestofs-pds"
PRODUCT = re.compile(r"estofs\.t00z\.conus\.west\.f(\d{3})\.grib2$")
logger = logging.getLogger("dms_datastore.download_stofs2d" if __name__ == "__main__" else __name__)


def _client():
    return boto3.client("s3", config=Config(signature_version=UNSIGNED, region_name="us-east-1"))


def list_run(client, day, forecast_steps=None):
    """List matching GRIB objects and reject explicitly requested missing leads."""
    prefix = f"estofs.{day:%Y%m%d}/estofs.t00z.conus.west.f"
    records = {}
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for item in page.get("Contents", []):
            name = item["Key"].rsplit("/", 1)[-1]
            match = PRODUCT.fullmatch(name)
            if match:
                lead = int(match.group(1))
                if forecast_steps is None or lead in forecast_steps:
                    records[lead] = item
    if forecast_steps is not None:
        missing = sorted(set(forecast_steps) - records.keys())
        if missing:
            raise ValueError(f"{day:%Y-%m-%d} missing forecast leads: {missing}")
    if not records:
        raise ValueError(f"No historical ESTOFS t00z CONUS-west forecasts found for {day:%Y-%m-%d}")
    return [records[lead] for lead in sorted(records)]


def _subset_grib(path, day, lead, lat_bounds, lon_bounds):
    """Decode NOAA parameter 10/3/250 (combined tide and surge, metres)."""
    import numpy as np
    import pandas as pd
    import xarray as xr

    with xr.open_dataset(
        path, engine="cfgrib",
        backend_kwargs={
            "indexpath": "",
            "filter_by_keys": {"discipline": 10, "parameterCategory": 3, "parameterNumber": 250},
            "read_keys": ["discipline", "parameterCategory", "parameterNumber"],
        },
    ) as data:
        if len(data.data_vars) != 1:
            raise ValueError(f"Expected one combined-water-level field in {path}")
        field = next(iter(data.data_vars.values()))
        if tuple(field.dims) != ("y", "x"):
            raise ValueError(f"Unexpected ESTOFS field dimensions in {path}: {field.dims}")
        init = pd.Timestamp(data["time"].values)
        valid = pd.Timestamp(data["valid_time"].values)
        expected = pd.Timestamp(day)
        if init != expected or valid != expected + pd.Timedelta(hours=lead):
            raise ValueError(f"GRIB run or valid time disagrees with filename: {path}")
        lat = data.latitude.values
        lon = data.longitude.values % 360
        inside = (
            (lat >= lat_bounds[0]) & (lat <= lat_bounds[1])
            & (lon >= lon_bounds[0]) & (lon <= lon_bounds[1])
        )
        rows, cols = np.nonzero(inside)
        if not len(rows):
            raise ValueError(f"No ESTOFS grid cells within requested bounds in {path}")
        ys = slice(int(rows.min()), int(rows.max()) + 1)
        xs = slice(int(cols.min()), int(cols.max()) + 1)
        values = np.asarray(field.isel(y=ys, x=xs).values, dtype=float)
        values[~inside[ys, xs]] = np.nan
        if not np.isfinite(values).any():
            raise ValueError(f"No valid combined water levels within requested bounds in {path}")
        result = xr.Dataset(
            data_vars={
                "ssh": (("MT", "Y", "X"), values[np.newaxis, :, :], {
                    "long_name": "Combined tide and surge water level",
                    "units": "m", "vertical_datum": "MSL",
                    "GRIB_discipline": 10, "GRIB_parameterCategory": 3,
                    "GRIB_parameterNumber": 250,
                }),
            },
            coords={
                "MT": [valid.to_datetime64()],
                "Latitude": (("Y", "X"), lat[ys, xs]),
                "Longitude": (("Y", "X"), lon[ys, xs]),
            },
            attrs={"source": "NOAA ESTOFS t00z CONUS west", "source_vertical_datum": "MSL"},
        )
        return result


def download_run(client, day, output, lat_bounds, lon_bounds, forecast_steps=None,
                 overwrite=False, dry_run=False):
    """Download and convert each selected source object to a cropped subset."""
    records = list_run(client, day, forecast_steps)
    if dry_run:
        for item in records:
            logger.info("Would download s3://%s/%s (%.2f MiB)",
                        BUCKET, item["Key"], item["Size"] / 1024**2)
        return []
    paths = []
    for item in records:
        key, size = item["Key"], item["Size"]
        name = key.rsplit("/", 1)[-1]
        lead = int(PRODUCT.fullmatch(name).group(1))
        target = Path(output) / "subset" / f"estofs.{day:%Y%m%d}" / (name[:-6] + ".nc")
        metadata = {
            "subset_source_key": key, "subset_source_size": str(size),
            "subset_lat_bounds": list(lat_bounds), "subset_lon_bounds": list(lon_bounds),
            "subset_version": 1,
        }
        if target.is_file() and not overwrite:
            import xarray as xr
            with xr.open_dataset(target) as existing:
                same = all(
                    (list(existing.attrs[k]) == v if isinstance(v, list) else existing.attrs[k] == v)
                    for k, v in metadata.items() if k in existing.attrs
                ) and all(k in existing.attrs for k in metadata)
                same = same and "ssh" in existing and "Latitude" in existing and "Longitude" in existing
            if same:
                logger.info("Already subsetted: %s", target)
                paths.append(target)
                continue
            logger.warning("Existing subset metadata mismatch; rebuilding: %s", target)
        target.parent.mkdir(parents=True, exist_ok=True)
        partial = target.with_name(target.name + ".part")
        try:
            with TemporaryDirectory(prefix="estofs_", dir=target.parent) as directory:
                source = Path(directory) / name
                logger.info("Downloading s3://%s/%s", BUCKET, key)
                client.download_file(BUCKET, key, str(source))
                if source.stat().st_size != size:
                    raise ValueError(f"Downloaded size does not match S3 object: {key}")
                subset = _subset_grib(source, day, lead, lat_bounds, lon_bounds)
                subset.attrs.update(metadata)
                subset.to_netcdf(
                    partial, engine="netcdf4", format="NETCDF4",
                    encoding={"ssh": {"zlib": True, "complevel": 4, "_FillValue": -9999.0}},
                )
                partial.replace(target)
        finally:
            partial.unlink(missing_ok=True)
        paths.append(target)
        logger.info("Saved subset: %s", target)
    return paths


def _load_subsets(paths):
    """Load matching grids and select the newest run at duplicate valid times."""
    import numpy as np
    import pandas as pd
    import xarray as xr

    if not paths:
        raise ValueError("No ESTOFS subset files found")
    samples = []
    for path in sorted(paths, key=lambda p: (p.parent.name, p.name)):
        with xr.open_dataset(path) as source:
            data = source[["ssh"]].load()
        if len(data.MT) != 1:
            raise ValueError(f"Expected one time sample in {path}")
        if samples:
            for name in ("Latitude", "Longitude"):
                if data[name].shape != samples[0][name].shape or not np.allclose(
                    data[name], samples[0][name], equal_nan=True
                ):
                    raise ValueError(f"Inconsistent ESTOFS {name} grid in {path}")
        samples.append(data)
    combined = xr.concat(samples, dim="MT", data_vars="all", coords="minimal",
                         compat="equals", join="exact")
    unique = ~pd.Index(combined.MT.values).duplicated(keep="last")
    return combined.isel(MT=unique).sortby("MT")


def _regular_surface(data, times, latitudes, longitudes, vdatum_grid):
    """Interpolate without extrapolation, then apply NOAA NAVD88 offsets."""
    grid = _interpolate_rtofs_grid(data, ["ssh"], times, latitudes, longitudes, 1)
    return _convert_rtofs_surface_to_navd88(
        grid["ssh"].values, latitudes, longitudes, vdatum_grid, mask_uncovered=True
    )


def _atomic_netcdf(data, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(path.name + ".part")
    try:
        data.to_netcdf(
            partial, engine="netcdf4", format="NETCDF4", unlimited_dims=["time"],
            encoding={"surf_el": {"dtype": "float64", "zlib": True, "_FillValue": -9999.0}},
        )
        partial.replace(path)
    finally:
        partial.unlink(missing_ok=True)


def process_stofs2d(input_dir, processed_dir, vdatum_grid, lat_bounds, lon_bounds,
                    lat_count=51, lon_count=38, start=None, end=None):
    """Write complete daily surface grids and one CSV per forecast run."""
    import numpy as np
    import pandas as pd
    import xarray as xr

    input_dir, processed_dir, vdatum_grid = Path(input_dir), Path(processed_dir), Path(vdatum_grid)
    if not vdatum_grid.is_file():
        raise FileNotFoundError(f"WCOFS vertical-datum grid not found: {vdatum_grid}")
    if (start is None) != (end is None):
        raise ValueError("Processing start and end must be supplied together")
    paths = sorted(input_dir.rglob("estofs.t00z.conus.west.f*.nc"))
    groups = {}
    for path in paths:
        match = re.fullmatch(r"estofs\.(\d{8})", path.parent.name)
        if not match:
            raise ValueError(f"Cannot determine forecast date from {path}")
        groups.setdefault(match.group(1), []).append(path)
    data = _load_subsets(paths)
    times = pd.DatetimeIndex(data.MT.values)
    if len(times) < 2:
        raise ValueError("At least two forecast samples are required for processing")
    first = (times[0] - pd.Timedelta(hours=8)).ceil("D")
    last = (times[-1] - pd.Timedelta(hours=31)).floor("D")
    if start is not None:
        start, end = pd.Timestamp(start).normalize(), pd.Timestamp(end).normalize()
        if start > end:
            raise ValueError("Processing start date cannot be after end date")
        if start < first or end > last:
            raise ValueError(f"Requested days are not bracketed by source data: {first.date()} to {last.date()}")
    else:
        start, end = first, last
    if start > end:
        raise ValueError("Forecast samples do not bracket a complete fixed-PST day")
    latitudes = np.linspace(*lat_bounds, lat_count)
    longitudes = np.linspace(*lon_bounds, lon_count)
    written = []
    for day in pd.date_range(start, end, freq="D"):
        pst = pd.date_range(day, periods=24, freq="h")
        utc = (pst + pd.Timedelta(hours=8)).values
        surface = _regular_surface(data, utc, latitudes, longitudes, vdatum_grid)
        result = xr.Dataset(
            {"surf_el": (("time", "lat", "lon"), surface, {
                "long_name": "Combined tide and surge water surface elevation",
                "units": "m", "vertical_datum": "NAVD88",
            })},
            coords={
                "time": ("time", np.arange(24, dtype=np.int32), {
                    "units": f"hours since {day:%Y-%m-%d} 00:00:00",
                    "calendar": "proleptic_gregorian", "timezone": "fixed PST", "utc_offset": "-08:00",
                }),
                "lat": ("lat", latitudes, {"units": "degrees_north", "axis": "Y"}),
                "lon": ("lon", longitudes, {"units": "degrees_east", "axis": "X"}),
            },
            attrs={"source": "NOAA ESTOFS t00z CONUS west", "vertical_datum": "NAVD88",
                   "vertical_datum_grid": vdatum_grid.name,
                   "source_vertical_datum": "MSL",
                   "datum_coverage": "Cells farther than 10 km from the datum grid are missing",
                   "history": "Linear regridding; UTC to fixed PST; MSL to NAVD88",
                   "source_files": "\n".join(str(p.relative_to(input_dir)) for p in paths)},
        )
        path = processed_dir / f"estofs_interpolated_hourly_pst{day:%Y%m%d}.nc"
        _atomic_netcdf(result, path)
        written.append(path)
        logger.info("Saved daily grid: %s", path)
    point_xy = np.asarray(RTOFS_POINT_COORDS_UTM10N)
    for date, run_paths in sorted(groups.items()):
        run = _load_subsets(run_paths)
        run_times = pd.DatetimeIndex(run.MT.values)
        if len(run_times) < 2:
            raise ValueError(f"Forecast {date} has fewer than two samples")
        utc = pd.date_range(run_times[0], run_times[-1], freq="h")
        surface = _regular_surface(run, utc.values, latitudes, longitudes, vdatum_grid)
        points = _interpolate_rtofs_to_boundary(
            utc - pd.Timedelta(hours=8), latitudes, longitudes, surface,
            np.arange(1, len(point_xy) + 1), point_xy,
        )
        points.columns = [
            f"point_{i}_easting_{x:.3f}_northing_{y:.3f}_surf_el_m_NAVD88"
            for i, (x, y) in enumerate(point_xy, start=1)
        ]
        missing = [column for column in points if points[column].isna().all()]
        if missing:
            raise ValueError(f"No ESTOFS elevations available for point(s): {', '.join(missing)}")
        csv = processed_dir.parent / "csv" / f"forecast_estofs_{date}.csv"
        csv.parent.mkdir(parents=True, exist_ok=True)
        partial = csv.with_name(csv.name + ".part")
        try:
            points.to_csv(partial, index_label="time_fixed_PST", date_format="%Y-%m-%d %H:%M:%S")
            partial.replace(csv)
        finally:
            partial.unlink(missing_ok=True)
        written.append(csv)
        logger.info("Saved forecast point CSV: %s", csv)
    return written


@click.command(
    context_settings={"help_option_names": ["-h", "--help"]},
    help="Download historical ESTOFS t00z CONUS-west forecasts and process water levels.",
    epilog="""\b
Examples:
  dms download_stofs2d -d 2020-12-30 --dry-run
  dms download_stofs2d -d 2020-12-30 --vdatum-grid ..\\vdatum_crop.nc
  dms download_stofs2d --start 2020-12-30 --end 2021-01-02 --vdatum-grid ..\\vdatum_crop.nc
  dms download_stofs2d --process-only --dest ./estofs_data --vdatum-grid ..\\vdatum_crop.nc

\b
The requested archive uses historical estofs.* names, not newer stofs_2d_glo.*
products. GRIB parameter 10/3/250 is combined tide and surge in metres.
Outputs contain surface elevation only; temperature, salinity and velocity
are not supplied. Daily grids and per-run CSVs use fixed PST and NAVD88.
Cells without a datum-grid offset within 10 km are masked and reported.
""",
)
@click.option("-d", "--date", type=click.DateTime(formats=["%Y-%m-%d"]), help="Run date (UTC; default today)")
@click.option("-sdate", "--start", type=click.DateTime(formats=["%Y-%m-%d"]), help="First run date inclusive")
@click.option("-edate", "--end", type=click.DateTime(formats=["%Y-%m-%d"]), help="Last run date inclusive")
@click.option("-o", "--dest", "--output", type=click.Path(path_type=Path), default=Path("estofs_data"), show_default=True)
@click.option("-fs", "--forecast-steps", type=ForecastStepsType(), help="Lead hour or inclusive START:END[:STRIDE]")
@click.option("--lat-bounds", type=float, nargs=2, default=LAT_LON_BOUNDS["lat"], callback=_validate_bounds, metavar="SOUTH NORTH")
@click.option("--lon-bounds", type=float, nargs=2, default=LAT_LON_BOUNDS["lon"], callback=_validate_bounds, metavar="WEST EAST")
@click.option("--lat-count", type=click.IntRange(min=2), default=51, show_default=True)
@click.option("--lon-count", type=click.IntRange(min=2), default=38, show_default=True)
@click.option("--vdatum-grid", type=click.Path(path_type=Path), help="WCOFS NetCDF datum grid with navd88tomsl")
@click.option("--process-only", is_flag=True, help="Process existing subsets without contacting S3")
@click.option("--skip-processing", is_flag=True, help="Download cropped subsets only")
@click.option("--process-start", type=click.DateTime(formats=["%Y-%m-%d"]), help="First fixed-PST day inclusive")
@click.option("--process-end", type=click.DateTime(formats=["%Y-%m-%d"]), help="Last fixed-PST day inclusive")
@click.option("--dry-run", is_flag=True, help="List source files without downloading or writing")
@click.option("--overwrite", is_flag=True, help="Replace existing subsets")
@click.option("--logdir", type=click.Path(path_type=Path), default=Path("logs"), show_default=True)
@click.option("--quiet", is_flag=True)
@click.option("--debug", is_flag=True)
def download_stofs2d_cli(date, start, end, dest, forecast_steps, lat_bounds, lon_bounds,
                         lat_count, lon_count, vdatum_grid, process_only, skip_processing,
                         process_start, process_end, dry_run, overwrite, logdir, quiet, debug):
    if (start is None) != (end is None) or (start is not None and date is not None):
        raise click.UsageError("Supply --date or both --start and --end")
    if start is not None and start > end:
        raise click.UsageError("--start cannot be after --end")
    if (process_start is None) != (process_end is None):
        raise click.UsageError("Supply both --process-start and --process-end")
    if process_start is not None and process_start > process_end:
        raise click.UsageError("--process-start cannot be after --process-end")
    if process_only and (dry_run or skip_processing or date or start or forecast_steps is not None):
        raise click.UsageError("--process-only cannot be combined with download selection or --dry-run")
    if skip_processing and (process_start or process_end):
        raise click.UsageError("--skip-processing cannot be combined with processing dates")
    if not dry_run and not skip_processing:
        if vdatum_grid is None:
            raise click.UsageError("--vdatum-grid is required for NAVD88 processing")
        if not vdatum_grid.is_file():
            raise click.BadParameter("file does not exist", param_hint="--vdatum-grid")
    level, console = resolve_loglevel(debug=debug, quiet=quiet)
    configure_logging(package_name="dms_datastore", level=level, console=console,
                      logdir=None if dry_run else logdir, logfile_prefix="download_stofs2d")
    try:
        if not process_only:
            if start is None:
                start = end = date or datetime.now(timezone.utc).replace(
                    hour=0, minute=0, second=0, microsecond=0, tzinfo=None
                )
            client = _client()
            day = start
            while day <= end:
                download_run(client, day, dest, lat_bounds, lon_bounds, forecast_steps,
                             overwrite=overwrite, dry_run=dry_run)
                day += timedelta(days=1)
        if not dry_run and not skip_processing:
            process_stofs2d(dest / "subset", dest / "processed", vdatum_grid,
                            lat_bounds, lon_bounds, lat_count, lon_count, process_start, process_end)
    except (BotoCoreError, ClientError, OSError, ValueError) as error:
        raise click.ClickException(str(error)) from error


main = download_stofs2d_cli

if __name__ == "__main__":
    main()
