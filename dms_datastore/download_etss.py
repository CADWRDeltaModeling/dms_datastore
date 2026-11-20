#!/usr/bin/env python3
"""Download ETSS water-level products from NOMADS and convert to NetCDF.

The downloader targets ETSS v1.4 run folders such as etss.YYYYMMDD and
converts tide, storm surge, and storm tide GRIB2 products per cycle.
Storm surge falls back to the 2.5 km product when 625 m is unavailable.
"""

import logging
import re
import shutil
import csv
import time
import os
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import click

from dms_datastore.logging_config import configure_logging, resolve_loglevel


BASE_URL = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/petss/v1.4"
SUPPORTED_CYCLES = ("00", "06", "12")
PRODUCT_CANDIDATES = {
    "tide": ("etss.t{cycle}z.tide.con625m.grib2",),
    "stormsurge": (
        "etss.t{cycle}z.stormsurge.con625m.grib2",
        "etss.t{cycle}z.stormsurge.con2p5km.grib2",
    ),
    "stormtide": ("etss.t{cycle}z.stormtide.con625m.grib2",),
}
LAT_LON_BOUNDS = {"lat": (37.0, 39.0), "lon": (236.0, 239.0)}
RUN_DIR_PATTERN = re.compile(r"etss\.(\d{8})/")
SUBSET_NAME_PATTERN = re.compile(r"etss\.t(\d{2})z\.(tide|stormsurge|stormtide)\.(con625m|con2p5km)\.grib2\.nc$")
POINT_COORDS_UTM10N = (
    (508879.733, 4157870.920),
    (539094.454, 4140103.110),
    (503429.325, 4204146.620),
)
GRID_KEYS = (
    "gridType",
    "Ni",
    "Nj",
    "shapeOfTheEarth",
    "radius",
    "Latin1InDegrees",
    "Latin2InDegrees",
    "LoVInDegrees",
    "LaDInDegrees",
    "DxInMetres",
    "DyInMetres",
    "latitudeOfFirstGridPointInDegrees",
    "longitudeOfFirstGridPointInDegrees",
    "iScansNegatively",
    "jScansPositively",
    "jPointsAreConsecutive",
    "alternativeRowScanning",
)
logger = logging.getLogger("dms_datastore.download_etss" if __name__ == "__main__" else __name__)


class DirectoryLinks(HTMLParser):
    """Collect href values from a simple directory index page."""

    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            self.links.extend(value for key, value in attrs if key == "href" and value)


def directory_links(url):
    with urlopen(url, timeout=60) as response:
        parser = DirectoryLinks()
        parser.feed(response.read().decode("utf-8"))
    return parser.links


def source_metadata(url):
    with urlopen(Request(url, method="HEAD"), timeout=60) as response:
        size = response.headers.get("Content-Length")
        if size is None or int(size) <= 0:
            raise ValueError("No valid Content-Length for %s" % url)
        return int(size), response.headers


def download_file(url, destination, size):
    with urlopen(url, timeout=240) as response, open(destination, "wb") as stream:
        shutil.copyfileobj(response, stream, length=1024 * 1024)
    if destination.stat().st_size != size:
        raise ValueError("Incomplete transfer: %s" % url)


def parse_cycle(value):
    token = value.strip().lower()
    if token.endswith("z"):
        token = token[:-1]
    if token.startswith("t"):
        token = token[1:]
    if token in ("006", "06"):
        return "06"
    if token in ("012", "12"):
        return "12"
    if token in ("000", "00"):
        return "00"
    raise ValueError("Unsupported cycle %r. Use 00, 06, or 12." % value)


def parse_cycles(cycles):
    if not cycles:
        return SUPPORTED_CYCLES
    normalized = []
    for cycle in cycles:
        parsed = parse_cycle(cycle)
        if parsed not in normalized:
            normalized.append(parsed)
    return tuple(normalized)


def resolve_cycle_targets(available, cycle):
    selected = []
    missing = []
    for product, candidates in PRODUCT_CANDIDATES.items():
        resolved = None
        for template in candidates:
            filename = template.format(cycle=cycle)
            if filename in available:
                resolved = filename
                break
        if resolved is None:
            missing.append("%s:[%s]" % (product, ", ".join(candidates)))
        else:
            selected.append(resolved)
    if missing:
        raise ValueError(
            "Requested ETSS files are not published for t%sz: %s"
            % (cycle, "; ".join(missing))
        )
    return tuple(selected)


def read_grib_message(stream):
    """Read raw or Fortran-record GRIB2 without FILE* locking issues on Windows."""
    magic = stream.read(4)
    if not magic:
        return None
    record_marker = None
    if magic != b"GRIB":
        record_marker = magic
        magic = stream.read(4)
        if len(record_marker) != 4 or magic != b"GRIB":
            raise ValueError("Invalid GRIB header or Fortran record marker")
    header = magic + stream.read(12)
    if len(header) != 16 or header[7] != 2:
        raise ValueError("Expected a complete GRIB edition 2 header")
    length = int.from_bytes(header[8:16], "big")
    remaining = Path(stream.name).stat().st_size - stream.tell()
    if length < 20 or length - 16 > remaining:
        raise ValueError("Invalid or truncated GRIB2 message length")
    if record_marker is not None and length not in (
        int.from_bytes(record_marker, "little"),
        int.from_bytes(record_marker, "big"),
    ):
        raise ValueError("Fortran record length does not match GRIB2 message")
    message = header + stream.read(length - 16)
    if len(message) != length or message[-4:] != b"7777":
        raise ValueError("Truncated GRIB2 message or missing end marker")
    if record_marker is not None and stream.read(4) != record_marker:
        raise ValueError("Invalid or truncated Fortran record end marker")
    return message


def full_grid_coordinates(grid):
    import numpy as np
    from pyproj import Proj

    if grid["gridType"] != "lambert":
        raise ValueError("Expected ETSS Lambert grid, got %s" % grid["gridType"])
    if grid["shapeOfTheEarth"] != 1:
        raise ValueError("Expected spherical Lambert ETSS grid")
    if grid["jPointsAreConsecutive"] or grid["alternativeRowScanning"]:
        raise ValueError("Unsupported GRIB scan ordering")

    projection = Proj(
        proj="lcc",
        lat_1=grid["Latin1InDegrees"],
        lat_2=grid["Latin2InDegrees"],
        lat_0=grid["LaDInDegrees"],
        lon_0=grid["LoVInDegrees"],
        R=grid["radius"],
    )
    x0, y0 = projection(
        grid["longitudeOfFirstGridPointInDegrees"],
        grid["latitudeOfFirstGridPointInDegrees"],
    )
    dx = grid["DxInMetres"] * (-1 if grid["iScansNegatively"] else 1)
    dy = grid["DyInMetres"] * (1 if grid["jScansPositively"] else -1)
    xx, yy = np.meshgrid(
        x0 + np.arange(grid["Ni"]) * dx,
        y0 + np.arange(grid["Nj"]) * dy,
    )
    longitude, latitude = projection(xx, yy, inverse=True)
    longitude %= 360
    return latitude, longitude


def _validate_lat_lon_bounds(lat_bounds, lon_bounds):
    south, north = lat_bounds
    west, east = lon_bounds
    if not (-90 <= south < north <= 90):
        raise ValueError("Latitude bounds must satisfy -90 <= SOUTH < NORTH <= 90")
    if not (0 <= west < east <= 360):
        raise ValueError("Longitude bounds must satisfy 0 <= WEST < EAST <= 360")


def _subset_box_indexers(latitude, longitude, lat_bounds, lon_bounds):
    import numpy as np

    _validate_lat_lon_bounds(lat_bounds, lon_bounds)
    south, north = lat_bounds
    west, east = lon_bounds
    longitude = longitude % 360
    inside = (
        (latitude >= south)
        & (latitude <= north)
        & (longitude >= west)
        & (longitude <= east)
    )
    rows, columns = np.nonzero(inside)
    if not rows.size:
        raise ValueError("No ETSS grid cells fall within the latitude/longitude box")
    y_slice = slice(int(rows.min()), int(rows.max()) + 1)
    x_slice = slice(int(columns.min()), int(columns.max()) + 1)
    return y_slice, x_slice, inside[y_slice, x_slice]


def convert_grib_to_netcdf(source, destination, metadata, lat_bounds, lon_bounds):
    """Convert one ETSS GRIB2 file into one NetCDF file."""
    import eccodes as ec
    import numpy as np
    from netCDF4 import Dataset, date2num

    count = 0
    first_grid = None
    previous_time = None
    reference = None

    partial = destination.with_name(destination.name + ".part")
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(source, "rb") as stream, Dataset(partial, "w", format="NETCDF4") as output:
            output.setncatts(metadata)
            output.setncatts(
                {
                    "Conventions": "CF-1.8",
                    "source": "NOAA ETSS con625m GRIB2",
                    "history": "Converted from native GRIB2 fields with UTC timestamps",
                }
            )
            while True:
                encoded = read_grib_message(stream)
                if encoded is None:
                    break
                message = ec.codes_new_from_message(encoded)
                del encoded
                try:
                    run = datetime.strptime(
                        "%08d%04d"
                        % (
                            ec.codes_get(message, "dataDate"),
                            ec.codes_get(message, "dataTime"),
                        ),
                        "%Y%m%d%H%M",
                    )
                    valid = datetime.strptime(
                        "%08d%04d"
                        % (
                            ec.codes_get(message, "validityDate"),
                            ec.codes_get(message, "validityTime"),
                        ),
                        "%Y%m%d%H%M",
                    )
                    if reference is None:
                        reference = run
                    if run != reference:
                        raise ValueError("GRIB contains more than one initialization time")
                    if previous_time is not None and valid <= previous_time:
                        raise ValueError("GRIB validity times are not unique and increasing")
                    grid = {key: ec.codes_get(message, key) for key in GRID_KEYS}
                    if first_grid is None:
                        first_grid = grid
                        latitude, longitude = full_grid_coordinates(grid)
                        y_slice, x_slice, mask = _subset_box_indexers(
                            latitude, longitude, lat_bounds, lon_bounds
                        )
                        latitude = latitude[y_slice, x_slice]
                        longitude = longitude[y_slice, x_slice]
                        output.createDimension("MT", None)
                        output.createDimension("Y", latitude.shape[0])
                        output.createDimension("X", latitude.shape[1])
                        latitude_var = output.createVariable("Latitude", "f8", ("Y", "X"))
                        latitude_var[:] = latitude
                        latitude_var.units = "degrees_north"
                        latitude_var.standard_name = "latitude"
                        longitude_var = output.createVariable("Longitude", "f8", ("Y", "X"))
                        longitude_var[:] = longitude
                        longitude_var.units = "degrees_east"
                        longitude_var.standard_name = "longitude"
                        times = output.createVariable("MT", "f8", ("MT",))
                        times.units = "hours since 1970-01-01 00:00:00 +00:00"
                        times.calendar = "proleptic_gregorian"
                        times.standard_name = "time"
                        times.timezone = "UTC"
                        date_var = output.createVariable("Date", "f8")
                        date_var.units = times.units
                        date_var.calendar = times.calendar
                        date_var.standard_name = "forecast_reference_time"
                        date_var.assignValue(date2num(run, times.units, times.calendar))
                        value_var = output.createVariable(
                            "water_level",
                            "f4",
                            ("MT", "Y", "X"),
                            fill_value=-9999.0,
                            zlib=True,
                            complevel=4,
                        )
                        units = ec.codes_get(message, "units")
                        value_var.setncatts(
                            {
                                "long_name": ec.codes_get(message, "name"),
                                "units": units,
                                "coordinates": "Latitude Longitude Date",
                                "GRIB_shortName": ec.codes_get(message, "shortName"),
                                "GRIB_stepType": ec.codes_get(message, "stepType"),
                            }
                        )
                        for key in ("forecastTime", "startStep", "endStep"):
                            step_var = output.createVariable("GRIB_" + key, "i4", ("MT",))
                            step_var.units = "hours"
                        output.GRIB_grid = str(grid)
                    elif grid != first_grid:
                        raise ValueError("GRIB grid changed between forecast fields")

                    values = ec.codes_get_values(message)
                    field = values.reshape(grid["Nj"], grid["Ni"]).copy()
                    del values
                    missing_value = ec.codes_get(message, "missingValue")
                    field[field == missing_value] = np.nan
                    field = field[y_slice, x_slice]
                    field = np.where(mask, field, np.nan)
                    value_var[count] = np.ma.masked_invalid(field)
                    times[count] = date2num(valid, times.units, times.calendar)
                    for key in ("forecastTime", "startStep", "endStep"):
                        output.variables["GRIB_" + key][count] = ec.codes_get(message, key)
                    count += 1
                    previous_time = valid
                finally:
                    ec.codes_release(message)
            if count == 0:
                raise ValueError("No GRIB records were decoded from %s" % source)
        partial.replace(destination)
    except ec.CodesInternalError as error:
        raise ValueError("Cannot decode ETSS GRIB %s: %s" % (source, error)) from error
    finally:
        partial.unlink(missing_ok=True)
    logger.info("Saved %d fields: %s", count, destination)
    return count


def subset_matches(path, metadata):
    from netCDF4 import Dataset

    if not path.is_file():
        return False
    try:
        with Dataset(path) as data:
            required = {"water_level", "Latitude", "Longitude", "Date", "MT"}
            return (
                all(str(getattr(data, key, "")) == str(value) for key, value in metadata.items())
                and required.issubset(set(data.variables))
                and len(data.dimensions["MT"]) > 0
            )
    except (OSError, KeyError) as error:
        logger.warning("Cannot reuse %s: %s; rebuilding", path, error)
        return False


def project_csv_coordinates(longitude, latitude):
    import numpy as np
    from pyproj import network
    from pyproj.aoi import AreaOfInterest
    from pyproj.transformer import TransformerGroup

    longitude = (longitude + 180) % 360 - 180
    if not np.isfinite(longitude).all() or not np.isfinite(latitude).all():
        raise ValueError("CSV projection requires finite longitude/latitude coordinates")
    enabled = network.is_network_enabled()
    try:
        network.set_network_enabled(False)
        group = TransformerGroup(
            "EPSG:4326", "EPSG:26910", always_xy=True, allow_ballpark=False,
            area_of_interest=AreaOfInterest(
                float(longitude.min()), float(latitude.min()),
                float(longitude.max()), float(latitude.max()),
            ),
        )
        if not group.transformers:
            raise ValueError("No locally available non-ballpark EPSG:26910 transformation")
        transformer = group.transformers[0]
        return transformer.transform(longitude, latitude, errcheck=True)
    finally:
        network.set_network_enabled(enabled)


def write_point_csv(subset_path, csv_dir):
    import numpy as np
    from netCDF4 import Dataset, num2date
    from pyproj.exceptions import ProjError
    from scipy.interpolate import LinearNDInterpolator
    from scipy.spatial import Delaunay

    subset_path = Path(subset_path)
    source_key = f"{subset_path.parent.name}/{subset_path.name.replace('.nc', '')}"
    run = re.fullmatch(r"etss\.(\d{8})", subset_path.parent.name)
    product = SUBSET_NAME_PATTERN.fullmatch(subset_path.name)
    if run is None or product is None:
        raise ValueError(f"Cannot identify ETSS run/product in {subset_path}")

    with Dataset(subset_path) as data:
        if "water_level" not in data.variables:
            raise ValueError(f"{subset_path} is missing water_level")
        latitude = np.asarray(data["Latitude"][:])
        longitude = np.asarray(data["Longitude"][:])
        water_level = data["water_level"]
        if water_level.shape[1:] != latitude.shape or longitude.shape != latitude.shape:
            raise ValueError("ETSS water-level and coordinate grid shapes do not match")
        times = num2date(
            data["MT"][:], data["MT"].units, getattr(data["MT"], "calendar", "standard")
        )
        if not len(times) or any(b <= a for a, b in zip(times, times[1:])):
            raise ValueError("CSV input times must be nonempty, unique and increasing")
        wet = np.zeros(latitude.shape, dtype=bool)
        for index in range(len(times)):
            wet |= np.isfinite(np.ma.filled(water_level[index], np.nan))
        if wet.sum() < 3:
            raise ValueError(
                f"ETSS has fewer than three valid water-level cells in {subset_path} "
                f"({int(wet.sum())} cells across {len(times)} selected times). "
                "Use --skip-csv to skip point extraction."
            )
        try:
            xx, yy = project_csv_coordinates(longitude, latitude)
        except ProjError as error:
            raise ValueError(
                f"Cannot project ETSS coordinates to EPSG:26910 for {subset_path}: {error}"
            ) from error
        wet &= np.isfinite(xx) & np.isfinite(yy)
        if wet.sum() < 3:
            raise ValueError(
                f"Fewer than three ETSS wet cells have finite projected coordinates in {subset_path}. "
                "Use --skip-csv to skip point extraction."
            )
        triangulation = Delaunay(np.column_stack((xx[wet], yy[wet])))
        points = np.asarray(POINT_COORDS_UTM10N)
        if np.any(triangulation.find_simplex(points) < 0):
            raise ValueError("A CSV point lies outside ETSS wet-grid coverage; extrapolation disabled")
        values = np.empty((len(times), len(points)))
        for index in range(len(times)):
            field = np.ma.filled(water_level[index], np.nan)
            values[index] = LinearNDInterpolator(triangulation, field[wet])(points)
        missing = np.flatnonzero(~np.isfinite(values).any(axis=0)) + 1
        if len(missing):
            raise ValueError(f"No ETSS water levels are available for CSV point(s): {missing.tolist()}")
        if not np.isfinite(values).all():
            logger.warning("Missing ETSS point values in %s are written as empty CSV cells", subset_path)

        units = getattr(data["water_level"], "units", "unknown")

    csv_dir = Path(csv_dir)
    csv_dir.mkdir(parents=True, exist_ok=True)
    path = csv_dir / (
        f"forecast_etss_{run.group(1)}_t{product.group(1)}z_"
        f"{product.group(2)}_{product.group(3)}.csv"
    )
    partial = path.with_name(path.name + ".part")
    columns = [
        f"point_{index}_easting_{easting:.3f}_northing_{northing:.3f}_water_level_{units}"
        for index, (easting, northing) in enumerate(points, start=1)
    ]
    try:
        with partial.open("w", newline="", encoding="ascii") as stream:
            writer = csv.writer(stream)
            writer.writerow(["time_fixed_PST", *columns])
            for time, row in zip(times, values):
                writer.writerow([
                    (time - timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S"),
                    *(float(value) if np.isfinite(value) else "" for value in row),
                ])
        partial.replace(path)
    finally:
        partial.unlink(missing_ok=True)
    logger.info("Saved ETSS point time series from %s to: %s", source_key, path)
    return path


def export_existing_csvs(input_dir, csv_dir):
    paths = sorted(Path(input_dir).rglob("etss.t*z.*.grib2.nc"))
    if not paths:
        raise ValueError(f"No ETSS subset NetCDF files found in {input_dir}")
    return [write_point_csv(path, csv_dir) for path in paths]


def _subset_var_name(path):
    match = SUBSET_NAME_PATTERN.fullmatch(path.name)
    if match is None:
        raise ValueError(f"Unexpected ETSS subset filename: {path.name}")
    cycle, product, grid = match.groups()
    return f"water_level_t{cycle}z_{product}_{grid}"


def _decode_netcdf_time_index(values, units, calendar):
    import numpy as np
    import pandas as pd
    from netCDF4 import num2date

    def _ensure_list(obj):
        if isinstance(obj, (list, tuple, pd.Index)):
            return list(obj)
        if isinstance(obj, np.ndarray):
            if obj.ndim == 0:
                return [obj.item()]
            return obj.tolist()
        try:
            return list(obj)
        except TypeError:
            return [obj]

    decoded = num2date(
        values,
        units,
        calendar,
        only_use_cftime_datetimes=False,
        only_use_python_datetimes=False,
    )
    decoded_list = _ensure_list(decoded)
    try:
        converted = pd.to_datetime(decoded_list)
        if isinstance(converted, pd.Timestamp):
            return pd.DatetimeIndex([converted])
        return pd.DatetimeIndex(converted)
    except (TypeError, ValueError):
        return pd.DatetimeIndex(
            pd.to_datetime(
                [
                    value.isoformat() if hasattr(value, "isoformat") else str(value)
                    for value in decoded_list
                ]
            )
        )


def _subset_run_time(data):
    values = data["Date"][:]
    run_times = _decode_netcdf_time_index(
        values,
        data["Date"].units,
        getattr(data["Date"], "calendar", "standard"),
    )
    if len(run_times) == 0:
        raise ValueError("Date coordinate is empty in ETSS subset file")
    return run_times[0]


def _retry_windows_path_io(action, path, attempts=20, delay_seconds=0.25):
    last_error = None
    for _ in range(attempts):
        try:
            action()
            return
        except OSError as error:
            if getattr(error, "winerror", None) != 32:
                raise
            last_error = error
            time.sleep(delay_seconds)
    raise last_error


def _is_hdf_error(error):
    return "NetCDF: HDF error" in str(error)


def _is_retryable_write_error(error):
    if _is_hdf_error(error):
        return True
    if isinstance(error, PermissionError):
        return True
    if getattr(error, "winerror", None) == 32:
        return True
    if getattr(error, "errno", None) == 13:
        return True
    return False


def _estimate_processed_day_bytes(day_buffers):
    total = 0
    for payload in day_buffers.values():
        if not any(choice is not None for choice in payload["selected"]):
            continue
        ny, nx = payload["lat"].shape
        total += ny * nx * 24 * 4  # float32 water level
        total += ny * nx * 8 * 2   # float64 lat/lon
    return total


def _process_one_etss_day(paths, day, output_path):
    import numpy as np
    import pandas as pd
    from netCDF4 import Dataset

    utc_hours = pd.date_range(day, periods=24, freq="h") + pd.Timedelta(hours=8)
    day_buffers = {}
    for path in paths:
        path = Path(path)
        var_name = _subset_var_name(path)
        try:
            with Dataset(path) as data:
                required = {"MT", "Date", "Latitude", "Longitude", "water_level"}
                missing = required.difference(data.variables)
                if missing:
                    raise ValueError(f"{path} is missing ETSS variables: {sorted(missing)}")
                run_time = _subset_run_time(data)
                times = _decode_netcdf_time_index(
                    data["MT"][:],
                    data["MT"].units,
                    getattr(data["MT"], "calendar", "standard"),
                )
                lat = np.asarray(data["Latitude"][:], dtype=float)
                lon = np.asarray(data["Longitude"][:], dtype=float)
                values = data["water_level"]
                if values.shape[1:] != lat.shape or lon.shape != lat.shape:
                    raise ValueError(f"ETSS water-level and grid shapes mismatch in {path}")
                y_dim = f"Y_{var_name}"
                x_dim = f"X_{var_name}"
                if var_name not in day_buffers:
                    day_buffers[var_name] = {
                        "latest_run": np.array([pd.Timestamp.min] * 24, dtype=object),
                        "selected": [None] * 24,
                        "lat": lat,
                        "lon": lon,
                        "y_dim": y_dim,
                        "x_dim": x_dim,
                        "units": getattr(values, "units", "unknown"),
                    }
                else:
                    if (
                        day_buffers[var_name]["lat"].shape != lat.shape
                        or day_buffers[var_name]["lon"].shape != lon.shape
                        or not np.allclose(day_buffers[var_name]["lat"], lat, equal_nan=True)
                        or not np.allclose(day_buffers[var_name]["lon"], lon, equal_nan=True)
                    ):
                        raise ValueError(f"Grid changed within ETSS variable family {var_name}")

                for index, valid_time in enumerate(times):
                    offset = (valid_time - utc_hours[0]) / pd.Timedelta(hours=1)
                    if offset < 0 or offset >= 24 or not float(offset).is_integer():
                        continue
                    hour_index = int(offset)
                    if run_time >= day_buffers[var_name]["latest_run"][hour_index]:
                        day_buffers[var_name]["latest_run"][hour_index] = run_time
                        day_buffers[var_name]["selected"][hour_index] = (path, index)
        except (OSError, RuntimeError) as error:
            if _is_hdf_error(error):
                logger.warning("Skipping unreadable ETSS subset file due to HDF error: %s", path)
                continue
            raise

    if not day_buffers:
        return False
    if not any(any(choice is not None for choice in payload["selected"]) for payload in day_buffers.values()):
        return False

    output_path.parent.mkdir(parents=True, exist_ok=True)
    required_bytes = _estimate_processed_day_bytes(day_buffers)
    free_bytes = shutil.disk_usage(output_path.parent).free
    # NetCDF4 compression still needs temporary chunks/metadata; require margin.
    if free_bytes < required_bytes * 1.2:
        required_gb = required_bytes / (1024 ** 3)
        free_gb = free_bytes / (1024 ** 3)
        raise ValueError(
            f"Insufficient disk space for {output_path.name}: "
            f"need about {required_gb:.2f} GiB free (raw estimate), "
            f"only {free_gb:.2f} GiB available in {output_path.parent}"
        )

    partial_candidates = []
    written_partial = None
    try:
        for attempt in range(3):
            partial = output_path.with_name(
                f"{output_path.name}.{os.getpid()}.{int(time.time() * 1000)}.{attempt}.part"
            )
            partial_candidates.append(partial)
            try:
                with Dataset(partial, "w", format="NETCDF4") as output:
                    output.createDimension("time", None)
                    time_var = output.createVariable("time", "i4", ("time",))
                    time_var[:] = np.arange(24, dtype=np.int32)
                    time_var.setncatts(
                        {
                            "timezone": "modified from utc to pst",
                            "utc_offset": "-08:00",
                            "units": f"hours since {day:%Y-%m-%d} 00:00:00",
                            "calendar": "proleptic_gregorian",
                        }
                    )
                    output.setncatts(
                        {
                            "institution": "NOAA",
                            "source": "NOAA ETSS subset forecasts",
                            "history": "Selected hourly ETSS samples by fixed PST day; duplicate valid times prefer latest run initialization",
                            "Conventions": "CF-1.8",
                        }
                    )
                    for var_name, payload in sorted(day_buffers.items()):
                        if not any(choice is not None for choice in payload["selected"]):
                            continue
                        y_dim = payload["y_dim"]
                        x_dim = payload["x_dim"]
                        lat_name = f"Latitude_{var_name}"
                        lon_name = f"Longitude_{var_name}"
                        output.createDimension(y_dim, payload["lat"].shape[0])
                        output.createDimension(x_dim, payload["lat"].shape[1])
                        lat_var = output.createVariable(lat_name, "f8", (y_dim, x_dim))
                        lon_var = output.createVariable(lon_name, "f8", (y_dim, x_dim))
                        lat_var[:, :] = payload["lat"]
                        lon_var[:, :] = payload["lon"]
                        lat_var.setncatts({"long_name": "Latitude", "standard_name": "latitude", "units": "degrees_north"})
                        lon_var.setncatts({"long_name": "Longitude", "standard_name": "longitude", "units": "degrees_east"})
                        water_var = output.createVariable(
                            var_name,
                            "f4",
                            ("time", y_dim, x_dim),
                            zlib=True,
                            complevel=1,
                            fill_value=np.float32(-9999.0),
                            chunksizes=(1, min(512, payload["lat"].shape[0]), min(512, payload["lat"].shape[1])),
                        )
                        water_var.setncatts(
                            {
                                "long_name": f"ETSS water level ({var_name})",
                                "units": payload["units"],
                                "coordinates": f"{lat_name} {lon_name}",
                            }
                        )
                        for hour_index, source in enumerate(payload["selected"]):
                            if source is None:
                                continue
                            source_path, source_index = source
                            try:
                                with Dataset(source_path) as source_data:
                                    slice_values = np.ma.filled(
                                        source_data["water_level"][source_index, :, :],
                                        np.float32(-9999.0),
                                    ).astype(np.float32, copy=False)
                            except (OSError, RuntimeError) as error:
                                if _is_hdf_error(error):
                                    logger.warning(
                                        "Skipping one ETSS hour due to source HDF read error: %s [index=%s]",
                                        source_path,
                                        source_index,
                                    )
                                    continue
                                raise
                            water_var[hour_index, :, :] = slice_values
                        del water_var
                        del lat_var
                        del lon_var
                    output.sync()
                written_partial = partial
                break
            except (OSError, RuntimeError) as error:
                if attempt < 2 and _is_retryable_write_error(error):
                    logger.warning("Retrying ETSS daily write after temporary error for %s: %s", output_path, error)
                    time.sleep(0.5)
                    if partial.exists():
                        try:
                            _retry_windows_path_io(lambda: partial.unlink(), partial)
                        except OSError:
                            pass
                    continue
                raise
        if written_partial is None:
            raise RuntimeError(f"Failed to write ETSS processed file after retries: {output_path}")
        _retry_windows_path_io(lambda: written_partial.replace(output_path), written_partial)
    finally:
        for partial in partial_candidates:
            if partial.exists():
                try:
                    _retry_windows_path_io(lambda: partial.unlink(), partial)
                except OSError as error:
                    logger.warning("Could not remove temporary file %s: %s", partial, error)
    logger.info("Saved ETSS processed daily file: %s", output_path)
    return True


def process_etss(input_dir, processed_dir=None, start=None, end=None):
    import pandas as pd
    from netCDF4 import Dataset

    input_dir = Path(input_dir)
    if (start is None) != (end is None):
        raise ValueError("Processing start and end dates must be supplied together")
    paths = sorted(input_dir.rglob("etss.t*z.*.grib2.nc"))
    if not paths:
        raise ValueError(f"No ETSS subset NetCDF files found under {input_dir}")
    if processed_dir is None:
        processed_dir = input_dir.parent / "processed"
    processed_dir = Path(processed_dir)

    min_day = None
    max_day = None
    for path in paths:
        try:
            with Dataset(path) as data:
                times = _decode_netcdf_time_index(
                    data["MT"][:],
                    data["MT"].units,
                    getattr(data["MT"], "calendar", "standard"),
                ) - pd.Timedelta(hours=8)
        except (OSError, RuntimeError) as error:
            if _is_hdf_error(error):
                logger.warning("Skipping unreadable ETSS subset file due to HDF error: %s", path)
                continue
            raise
        if len(times) == 0:
            continue
        first = times[0].normalize()
        last = times[-1].normalize()
        min_day = first if min_day is None else min(min_day, first)
        max_day = last if max_day is None else max(max_day, last)
    if min_day is None or max_day is None:
        raise ValueError("No ETSS time samples available for daily processing")

    if start is not None:
        start = pd.Timestamp(start).normalize()
        end = pd.Timestamp(end).normalize()
        if start > end:
            raise ValueError("Processing start date cannot be after end date")
    else:
        start, end = min_day, max_day

    written = []
    for day in pd.date_range(start, end, freq="D"):
        output = processed_dir / f"etss_interpolated_hourly_pst{day:%Y%m%d}.nc"
        if _process_one_etss_day(paths, day, output):
            written.append(output)
    if not written:
        raise ValueError("No ETSS processed daily files were generated in requested range")
    return written


def list_available_dates():
    links = directory_links(BASE_URL + "/")
    return sorted(
        {
            match.group(1)
            for link in links
            for match in [RUN_DIR_PATTERN.fullmatch(link)]
            if match is not None
        },
        reverse=True,
    )


def download_etss_for_date(
    output_dir,
    run_date,
    cycles,
    lat_bounds,
    lon_bounds,
    dry_run=False,
    overwrite=False,
    skip_csv=False,
    csv_dir=None,
):
    prefix = "etss.%s" % run_date.strftime("%Y%m%d")
    run_url = "%s/%s" % (BASE_URL, prefix)
    available = set(directory_links(run_url + "/"))

    targets = []
    for cycle in cycles:
        targets.extend(resolve_cycle_targets(available, cycle))

    downloaded = []
    for filename in targets:
        source_url = "%s/%s" % (run_url, filename)
        size, modified = source_metadata(source_url)
        logger.info("%s (%.1f MiB transfer)", source_url, size / 1024 ** 2)
        if dry_run:
            continue
        destination = Path(output_dir) / "subset" / prefix / (filename + ".nc")
        metadata = {
            "subset_source": BASE_URL,
            "subset_source_key": "%s/%s" % (prefix, filename),
            "subset_source_size": str(size),
            "subset_source_modified": modified,
            "subset_schema_version": "1",
            "subset_lat_bounds": ",".join(str(value) for value in lat_bounds),
            "subset_lon_bounds": ",".join(str(value) for value in lon_bounds),
        }
        if not overwrite and subset_matches(destination, metadata):
            logger.info("Already downloaded with matching source and selection: %s", destination)
            if not skip_csv:
                write_point_csv(destination, csv_dir or Path(output_dir) / "csv")
            downloaded.append(destination)
            continue
        with TemporaryDirectory(prefix="etss_") as temporary:
            local_grib = Path(temporary) / filename
            download_file(source_url, local_grib, size)
            convert_grib_to_netcdf(
                local_grib,
                destination,
                metadata,
                lat_bounds=lat_bounds,
                lon_bounds=lon_bounds,
            )
        if not skip_csv:
            write_point_csv(destination, csv_dir or Path(output_dir) / "csv")
        downloaded.append(destination)
    return downloaded


@click.command(
    context_settings={"help_option_names": ["-h", "--help"]},
    epilog="""\
\b
Examples:
  python download_etss.py -d 2026-11-18 --dry-run
  python download_etss.py -d 2026-11-18 --cycle 00 --cycle 006 --dest ./etss_data
  python download_etss.py --start 2026-11-17 --end 2026-11-18 --cycle 12 --overwrite
  python download_etss.py --csv-only --dest ./etss_data
  python download_etss.py --process-only --dest ./etss_data
  python download_etss.py --list-dates --days-back 5

\b
Notes:
  - ETSS products do not use PETSS exceedance levels.
  - Each cycle selects tide, storm tide, and storm surge products.
  - Storm surge uses 625 m when available, otherwise 2.5 km.
  - Subsets are saved under OUTPUT/subset/etss.YYYYMMDD.
  - Point CSVs are exported unless --skip-csv is specified.
  - Daily processing uses existing subsets; --process-only does not recrop them.
""",
)
@click.option(
    "-d",
    "--date",
    type=click.DateTime(["%Y-%m-%d"]),
    help="Run date; default today UTC",
)
@click.option(
    "-sdate",
    "--start",
    type=click.DateTime(["%Y-%m-%d"]),
    help="First run date inclusive",
)
@click.option(
    "-edate",
    "--end",
    type=click.DateTime(["%Y-%m-%d"]),
    help="Last run date inclusive",
)
@click.option(
    "-o",
    "--dest",
    "--output",
    "output",
    type=click.Path(path_type=Path),
    default=Path("./etss_data"),
    show_default=True,
    help="Output directory",
)
@click.option(
    "--cycle",
    "cycles",
    multiple=True,
    help="UTC cycle; repeatable; accepts 00, 06/006, 12/012 (default all)",
)
@click.option(
    "--lat-bounds",
    type=(float, float),
    default=LAT_LON_BOUNDS["lat"],
    show_default=True,
    metavar="SOUTH NORTH",
    help="Latitude crop box in degrees north",
)
@click.option(
    "--lon-bounds",
    type=(float, float),
    default=LAT_LON_BOUNDS["lon"],
    show_default=True,
    metavar="WEST EAST",
    help="Longitude crop box in degrees east on a 0-360 convention",
)
@click.option("--list-dates", is_flag=True, help="List dates currently retained on NOMADS")
@click.option(
    "--days-back",
    type=click.IntRange(min=1),
    default=7,
    show_default=True,
    help="Maximum available dates to list",
)
@click.option("--dry-run", is_flag=True, help="List files and transfer sizes without writing data")
@click.option("--overwrite", is_flag=True, help="Replace even a matching existing subset")
@click.option("--skip-csv", is_flag=True, help="Download NetCDF subsets without point CSV export")
@click.option("--csv-only", is_flag=True, help="Export all existing ETSS subsets without network access")
@click.option("--input-dir", type=click.Path(path_type=Path), help="Subset input directory for --csv-only or --process-only; default OUTPUT/subset")
@click.option("--csv-dir", type=click.Path(path_type=Path), help="CSV destination; default OUTPUT/csv")
@click.option("--process-only", is_flag=True, help="Process existing subsets into ETSS daily files without downloading")
@click.option("--skip-processing", is_flag=True, help="Download subsets without generating ETSS processed daily files")
@click.option("--process-start", type=click.DateTime(["%Y-%m-%d"]), help="First PST date to process (inclusive)")
@click.option("--process-end", type=click.DateTime(["%Y-%m-%d"]), help="Last PST date to process (inclusive)")
@click.option("--processed-dir", type=click.Path(path_type=Path), help="Daily processed output folder")
@click.option("--logdir", type=click.Path(path_type=Path), default=Path("logs"), show_default=True, help="Directory for log files")
@click.option("--quiet", is_flag=True, help="Suppress console logging")
@click.option("--debug", is_flag=True, help="Enable debug logging")
def download_etss_cli(
    date, start, end, output, cycles, lat_bounds, lon_bounds, list_dates, days_back, dry_run, overwrite,
    skip_csv, csv_only, input_dir, csv_dir, process_only, skip_processing,
    process_start, process_end, processed_dir, logdir, quiet, debug
):
    """Download NOAA ETSS tide, storm surge, and storm tide as NetCDF."""
    if bool(start) != bool(end):
        raise click.UsageError("--start and --end must be supplied together")
    if date and start:
        raise click.UsageError("Use --date or --start/--end, not both")
    if start and start > end:
        raise click.UsageError("Start date cannot be after end date")
    if list_dates and (date or start or cycles or dry_run or overwrite):
        raise click.UsageError("--list-dates cannot be combined with download selectors")
    try:
        _validate_lat_lon_bounds(lat_bounds, lon_bounds)
    except ValueError as error:
        raise click.UsageError(str(error)) from error
    if bool(process_start) != bool(process_end):
        raise click.UsageError("--process-start and --process-end must be supplied together")
    if process_start and process_start > process_end:
        raise click.UsageError("Processing start date cannot be after end date")
    if csv_only and (skip_csv or dry_run or list_dates or date or start or cycles or overwrite):
        raise click.UsageError("--csv-only exports all existing subsets; cannot combine with download selectors or --skip-csv")
    if input_dir and not (csv_only or process_only):
        raise click.UsageError("--input-dir requires --csv-only or --process-only")
    if process_only and (dry_run or list_dates or date or start):
        raise click.UsageError("--process-only cannot use download dates, --dry-run, or --list-dates")
    if process_only and skip_processing:
        raise click.UsageError("--process-only and --skip-processing cannot be used together")

    level, console = resolve_loglevel(debug=debug, quiet=quiet)
    configure_logging(
        package_name="dms_datastore",
        level=level,
        console=console,
        logdir=None if dry_run else logdir,
        logfile_prefix="download_etss",
    )

    try:
        if list_dates:
            dates = list_available_dates()
            if not dates:
                raise ValueError("No ETSS run dates are listed on NOMADS")
            for value in dates[:days_back]:
                click.echo(datetime.strptime(value, "%Y%m%d").date())
            return
        if csv_only:
            export_existing_csvs(input_dir or output / "subset", csv_dir or output / "csv")
            return
        if process_only:
            process_etss(
                (input_dir or output / "subset"),
                processed_dir=processed_dir or output / "processed",
                start=process_start,
                end=process_end,
            )
            return

        selected_cycles = parse_cycles(cycles)
        first = start or date or datetime.now(timezone.utc)
        last = end or first
        while first.date() <= last.date():
            download_etss_for_date(
                output,
                first,
                selected_cycles,
                lat_bounds=lat_bounds,
                lon_bounds=lon_bounds,
                dry_run=dry_run,
                overwrite=overwrite,
                skip_csv=skip_csv,
                csv_dir=csv_dir,
            )
            first += timedelta(days=1)
        if not dry_run and not skip_processing:
            process_etss(
                output / "subset",
                processed_dir=processed_dir or output / "processed",
                start=process_start,
                end=process_end,
            )
    except (HTTPError, URLError, TimeoutError, OSError, ValueError, RuntimeError, ImportError) as error:
        raise click.ClickException(str(error)) from error


main = download_etss_cli


if __name__ == "__main__":
    main()