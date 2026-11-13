#!/usr/bin/env python3
"""Download NOAA RTOFS forecasts from the public Amazon S3 archive.

Source: https://registry.opendata.aws/noaa-rtofs/
Objects are stored in s3://noaa-nws-rtofs-pds/rtofs.YYYYMMDD/.
For forecasts beyond the S3 archive's available lead hours, use
through f192 (8 days) at https://nomads.ncep.noaa.gov/pub/data/nccf/com/rtofs/prod/.
Use --source nomads for operational products through forecast hour 192 (8 days).
The default --source s3 keeps using the public S3 archive.
Defaults: surface elevation and full-depth temperature and salinity within
latitude 37-39 N and longitude 236-239 E, matching download_hycom.py. Regional
3-D fields have 40 standard depth levels and six-hourly forecasts. Original
S3 files are downloaded temporarily, then cropped to the box; only requested
fields and their coordinates are saved under subset/. Use --no-subset to
retain original files, including bundled diagnostics and velocities.

After downloads, complete fixed-PST days are regridded and interpolated hourly
into processed/rtofs_interpolated_hourly_pstYYYYMMDD.nc. Use --process-only
for existing downloads or --skip-processing for download-only operation.
Each RTOFS run also produces one fixed-point NAVD88 CSV containing that run's
full hourly surface forecast.
"""

import fnmatch
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


BUCKET = "noaa-nws-rtofs-pds"
NOMADS_BASE_URL = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/rtofs/prod"
DEFAULT_PATTERNS = (
	"rtofs_glo_2ds_*_diag.nc",
	"rtofs_glo_3dz_*_6hrly_hvr_US_west.nc",
)
LEAD_TIME = re.compile(r"(?:_|\.)([fn])(\d{2,3})(?:_|\.)")
LAT_LON_BOUNDS = {"lat": (37, 39), "lon": (236, 239)}
RTOFS_POINT_COORDS_UTM10N = (
	(508879.733, 4157870.920),
	(539094.454,  4140103.110), 
	(503429.325, 4204146.620),
)


def subset_rtofs_dataset(
	data,
	lat_bounds=LAT_LON_BOUNDS["lat"],
	lon_bounds=LAT_LON_BOUNDS["lon"],
):
	"""Crop RTOFS's curvilinear grid, keeping requested fields at all depths.

	Parameters
	----------
	data : xarray.Dataset
		RTOFS NetCDF dataset with two-dimensional Latitude and Longitude.
	lat_bounds : pair of float, optional
		Inclusive south/north limits in degrees north, default (37, 39).
	lon_bounds : pair of float, optional
		Inclusive west/east limits in degrees east on a 0-360 convention,
		default (236, 239). Source longitudes may use either convention.

	Returns
	-------
	xarray.Dataset
		Smallest rectangular grid enclosing the selected cells. Cells outside
		the box are masked. Retains ssh, temperature, and salinity when present,
		along with coordinates and Date; does not subset depth or time.
	"""
	import numpy as np

	south, north = lat_bounds
	west, east = lon_bounds
	if not (-90 <= south < north <= 90):
		raise ValueError("Latitude bounds must satisfy -90 <= SOUTH < NORTH <= 90")
	if not (0 <= west < east <= 360):
		raise ValueError("Longitude bounds must satisfy 0 <= WEST < EAST <= 360")
	latitude = data["Latitude"]
	longitude = data["Longitude"] % 360
	if latitude.ndim != 2 or longitude.dims != latitude.dims:
		raise ValueError("RTOFS Latitude and Longitude must share two grid dimensions")
	inside = (
		(latitude >= south) & (latitude <= north)
		& (longitude >= west) & (longitude <= east)
	)
	rows, columns = np.nonzero(inside.values)
	if not rows.size:
		raise ValueError("No RTOFS grid cells fall within the latitude/longitude box")
	indexers = {
		latitude.dims[0]: slice(int(rows.min()), int(rows.max()) + 1),
		latitude.dims[1]: slice(int(columns.min()), int(columns.max()) + 1),
	}
	fields = [name for name in ("ssh", "temperature", "salinity") if name in data]
	if not fields:
		raise ValueError("Dataset has no ssh, temperature, or salinity fields")
	coordinates = [name for name in ("Latitude", "Longitude", "Date") if name in data]
	subset = data.set_coords(coordinates).isel(indexers)[fields]
	mask = inside.isel(indexers)
	for name in fields:
		subset[name] = subset[name].where(mask)
	subset.attrs = dict(data.attrs)
	subset.attrs["subset_lat_bounds"] = list(lat_bounds)
	subset.attrs["subset_lon_bounds"] = list(lon_bounds)
	return subset


def setup_logging(debug: bool = False) -> None:
	"""Configure CLI logging."""
	logging.basicConfig(
		level=logging.DEBUG if debug else logging.INFO,
		format="%(asctime)s - %(levelname)s - %(message)s",
		datefmt="%Y-%m-%d %H:%M:%S",
	)


def _interpolate_rtofs_grid(data, fields, utc_hours, latitudes, longitudes, max_gap_hours):
	"""Interpolate bracketed UTC samples and regrid without spatial extrapolation."""
	import numpy as np
	import xarray as xr
	from scipy.interpolate import LinearNDInterpolator
	from scipy.spatial import Delaunay

	data = data.sortby("MT")
	times = data.MT.values.astype("datetime64[ns]")
	if len(times) < 2 or np.any(np.isnat(times)) or np.any(np.diff(times) <= np.timedelta64(0, "h")):
		raise ValueError("RTOFS input times must be unique and increasing, with at least two samples")
	if utc_hours[0] < times[0] or utc_hours[-1] > times[-1]:
		raise ValueError("Input data do not bracket every requested PST hour; extrapolation is disabled")
	first = max(0, int(np.searchsorted(times, utc_hours[0], side="right")) - 1)
	last = min(len(times), int(np.searchsorted(times, utc_hours[-1], side="left")) + 1)
	data = data.isel(MT=slice(first, last))
	gap_durations = np.diff(data.MT.values)
	gap_indices = np.flatnonzero(
		gap_durations > np.timedelta64(max_gap_hours, "h")
	)
	if len(gap_indices):
		gap_descriptions = []
		for index in gap_indices:
			start = np.datetime_as_string(data.MT.values[index], unit="m").replace(
				"T", " "
			)
			end = np.datetime_as_string(data.MT.values[index + 1], unit="m").replace(
				"T", " "
			)
			duration = gap_durations[index] / np.timedelta64(1, "h")
			gap_descriptions.append(
				f"{start} UTC to {end} UTC ({duration:g} hours)"
			)
		gap_summary = (
			"a time gap exceeds"
			if len(gap_indices) == 1
			else f"{len(gap_indices)} time gaps exceed"
		)
		raise ValueError(
			f"Missing RTOFS samples: {gap_summary} {max_gap_hours} hours "
			f"for {', '.join(fields)}: {'; '.join(gap_descriptions)}"
		)
	hourly = data[fields].interp(MT=utc_hours)
	latitude = data.Latitude.values
	longitude = data.Longitude.values % 360
	if latitude.ndim != 2 or data.Longitude.dims != data.Latitude.dims:
		raise ValueError("RTOFS latitude and longitude must share two grid dimensions")
	points = np.column_stack((longitude.ravel(), latitude.ravel()))
	finite = np.isfinite(points).all(axis=1)
	triangulation = Delaunay(points[finite])
	target_lon, target_lat = np.meshgrid(longitudes, latitudes)
	result = xr.Dataset(coords={"MT": utc_hours, "lat": latitudes, "lon": longitudes})
	for field in fields:
		dimensions = [name for name in hourly[field].dims if name not in data.Latitude.dims]
		values = hourly[field].transpose(*data.Latitude.dims, *dimensions).values
		shape = values.shape[2:]
		interpolator = LinearNDInterpolator(triangulation, values.reshape(len(points), -1)[finite])
		regridded = interpolator(target_lon, target_lat).reshape(len(latitudes), len(longitudes), *shape)
		coordinates = {name: hourly[name] for name in dimensions}
		coordinates.update(lat=latitudes, lon=longitudes)
		result[field] = xr.DataArray(
			regridded, dims=("lat", "lon", *dimensions), coords=coordinates,
			attrs=dict(data[field].attrs),
		).transpose(*dimensions, "lat", "lon")
	return result


def interpolate_rtofs_day(
	surface, hydrography, day,
	lat_bounds=LAT_LON_BOUNDS["lat"], lon_bounds=LAT_LON_BOUNDS["lon"],
	lat_count: int = 51, lon_count: int = 38,
):
	"""Combine elevation and hourly full-depth fields for one fixed-PST day.

	Parameters
	----------
	surface : xarray.Dataset
		UTC elevation samples with ssh(MT, Y, X).
	hydrography : xarray.Dataset
		UTC temperature and salinity samples with dimensions MT, Depth, Y, X.
	day : datetime-like
		Calendar date in fixed Pacific Standard Time (UTC-8, without DST).
	lat_bounds, lon_bounds : pair of float, optional
		Target geographic limits, defaulting to the HYCOM download box.
	lat_count, lon_count : int, optional
		Regular target grid sizes, defaulting to 51 by 38 from hycomschema.

	Returns
	-------
	xarray.Dataset
		24 hourly PST samples with temp/salt(time, depth, lat, lon) and
		surf_el(time, lat, lon). All source depth levels are retained. No time
		or spatial extrapolation is performed; land/outside-grid cells are NaN.
	"""
	import numpy as np
	import pandas as pd
	import xarray as xr

	if lat_count < 2 or lon_count < 2:
		raise ValueError("Target latitude and longitude counts must be at least two")
	if not (-90 <= lat_bounds[0] < lat_bounds[1] <= 90 and 0 <= lon_bounds[0] < lon_bounds[1] <= 360):
		raise ValueError("Invalid latitude/longitude bounds for the regular target grid")
	day = pd.Timestamp(day).normalize()
	if day.tzinfo is not None:
		raise ValueError("Supply a timezone-naive PST calendar date")
	pst_hours = pd.date_range(day, periods=24, freq="h").values
	utc_hours = pst_hours + np.timedelta64(8, "h")
	latitudes = np.linspace(*lat_bounds, lat_count)
	longitudes = np.linspace(*lon_bounds, lon_count)
	surface_hourly = _interpolate_rtofs_grid(surface, ["ssh"], utc_hours, latitudes, longitudes, 3)
	hydro_hourly = _interpolate_rtofs_grid(
		hydrography, ["temperature", "salinity"], utc_hours, latitudes, longitudes, 6,
	)
	result = xr.merge([surface_hourly, hydro_hourly], join="exact")
	result = result.rename({"MT": "time", "Depth": "depth", "temperature": "temp", "salinity": "salt", "ssh": "surf_el"})
	result = result.assign_coords(time=pst_hours)
	result.lat.attrs = {"long_name": "Latitude", "standard_name": "latitude", "units": "degrees_north", "point_spacing": "even", "axis": "Y", "NAVO_code": 1}
	result.lon.attrs = {"long_name": "Longitude", "standard_name": "longitude", "units": "degrees_east", "modulo": "360 degrees", "axis": "X", "NAVO_code": 2}
	result.time.attrs = {"timezone": "modified from utc to pst", "utc_offset": "-08:00"}
	result.depth.attrs = dict(hydrography.Depth.attrs)
	for field, long_name, standard_name, units, code in (
		("temp", "Water Temperature", "sea_water_potential_temperature", "degC", 15),
		("salt", "Salinity", "sea_water_salinity", "psu", 16),
		("surf_el", "Water Surface Elevation", "sea_surface_elevation", "m", None),
	):
		result[field].attrs = {
			"long_name": long_name, "standard_name": standard_name,
			"units": units,
			"_CoordinateAxes": " ".join(result[field].dims) + " ",
		}
		if code is not None:
			result[field].attrs["NAVO_code"] = code
	result.temp.attrs["comment"] = "RTOFS potential temperature; not converted to in-situ temperature"
	result.attrs = {
		"institution": hydrography.attrs.get("institution", "National Centers for Environmental Prediction"),
		"source": "NOAA RTOFS HYCOM forecast",
		"history": "Linear temporal interpolation, curvilinear-to-regular linear regridding, UTC to fixed PST",
		"field_type": "instantaneous", "Conventions": "CF-1.6 NAVO_netcdf_v1.1",
		"subset_lat_bounds": list(lat_bounds), "subset_lon_bounds": list(lon_bounds),
	}
	return result


def _convert_rtofs_surface_to_navd88(surface, latitude, longitude, vdatum_grid):
	"""Convert MSL-referenced RTOFS elevations using the NOAA datum grid."""
	import numpy as np
	import xarray as xr
	from pyproj import Transformer
	from scipy.spatial import cKDTree

	vdatum_grid = Path(vdatum_grid)
	if not vdatum_grid.is_file():
		raise FileNotFoundError(f"WCOFS vertical-datum grid not found: {vdatum_grid}")
	with xr.open_dataset(vdatum_grid, decode_times=False) as datum_data:
		required = {"latitude", "longitude", "navd88tomsl"}
		missing = required.difference(datum_data.variables)
		if missing:
			raise ValueError(
				f"{vdatum_grid} is missing datum variables: {sorted(missing)}"
			)
		datum_latitude = np.asarray(datum_data["latitude"].values, dtype=float)
		datum_longitude = np.asarray(datum_data["longitude"].values, dtype=float)
		navd88_to_msl = np.asarray(
			datum_data["navd88tomsl"].values, dtype=float
		).copy()
		fill_value = datum_data["navd88tomsl"].attrs.get("filledvalue")
		datum_description = datum_data["navd88tomsl"].attrs.get("long_name", "")

	normalized_description = datum_description.lower()
	if not (
		"navd88 minus mean sea level" in normalized_description
		or (
			"north american vertical datum of 1988" in normalized_description
			and "minus mean sea level" in normalized_description
		)
	):
		raise ValueError(
			f"{vdatum_grid} navd88tomsl metadata does not identify "
			"'NAVD88 minus mean sea level'"
		)
	if not (
		datum_latitude.ndim == datum_longitude.ndim == navd88_to_msl.ndim == 2
		and datum_latitude.shape == datum_longitude.shape == navd88_to_msl.shape
	):
		raise ValueError(f"Invalid coordinate or offset dimensions in {vdatum_grid}")
	if fill_value is not None:
		navd88_to_msl[navd88_to_msl == float(fill_value)] = np.nan

	latitude = np.asarray(latitude, dtype=float)
	longitude = np.asarray(longitude, dtype=float)
	surface = np.asarray(surface, dtype=float)
	if latitude.ndim != 1 or longitude.ndim != 1:
		raise ValueError("RTOFS latitude and longitude must be 1-D axes")
	grid_lon, grid_lat = np.meshgrid(longitude, latitude)
	if surface.ndim != 3 or surface.shape[1:] != grid_lat.shape:
		raise ValueError("RTOFS surface shape does not match its coordinate axes")

	valid_target = (
		np.isfinite(grid_lat)
		& np.isfinite(grid_lon)
		& np.isfinite(surface).any(axis=0)
	)
	valid_datum = (
		np.isfinite(datum_latitude)
		& np.isfinite(datum_longitude)
		& np.isfinite(navd88_to_msl)
	)
	if not valid_datum.any():
		raise ValueError(f"No valid NAVD88 datum offsets in {vdatum_grid}")

	transformer = Transformer.from_crs("EPSG:4326", "EPSG:26910", always_xy=True)
	datum_x, datum_y = transformer.transform(
		datum_longitude[valid_datum], datum_latitude[valid_datum]
	)
	datum_values = navd88_to_msl[valid_datum]
	valid_projected = np.isfinite(datum_x) & np.isfinite(datum_y)
	datum_points = np.column_stack(
		(np.asarray(datum_x)[valid_projected], np.asarray(datum_y)[valid_projected])
	)
	datum_values = datum_values[valid_projected]
	if not len(datum_points):
		raise ValueError(f"No valid projected datum-grid points in {vdatum_grid}")

	target_lon = np.where(grid_lon > 180.0, grid_lon - 360.0, grid_lon)
	target_x, target_y = transformer.transform(target_lon, grid_lat)
	if np.any(valid_target & ~(np.isfinite(target_x) & np.isfinite(target_y))):
		raise ValueError("Could not project all wet RTOFS grid cells to EPSG:26910")
	target_points = np.column_stack(
		(np.asarray(target_x)[valid_target], np.asarray(target_y)[valid_target])
	)
	if not len(target_points):
		raise ValueError("RTOFS data has no valid wet grid cells to convert")

	neighbor_count = min(4, len(datum_points))
	distances, neighbors = cKDTree(datum_points).query(
		target_points, k=neighbor_count
	)
	if neighbor_count == 1:
		distances = distances[:, np.newaxis]
		neighbors = neighbors[:, np.newaxis]
	if np.any(distances[:, 0] > 10000.0):
		count = int(np.count_nonzero(distances[:, 0] > 10000.0))
		raise ValueError(
			f"No nearby datum-grid coverage for {count} wet RTOFS cell(s) "
			f"within 10 km in {vdatum_grid}"
		)

	neighbor_values = datum_values[neighbors]
	offsets = np.empty(len(target_points), dtype=float)
	exact = distances[:, 0] <= 1e-3
	offsets[exact] = neighbor_values[exact, 0]
	if (~exact).any():
		weights = 1.0 / np.square(distances[~exact])
		offsets[~exact] = np.sum(
			weights * neighbor_values[~exact], axis=1
		) / weights.sum(axis=1)

	offset_grid = np.full(grid_lat.shape, np.nan, dtype=float)
	offset_grid[valid_target] = offsets
	if not np.isfinite(offset_grid[valid_target]).all():
		raise ValueError("Could not determine a NAVD88 offset for every wet RTOFS cell")
	# navd88tomsl is NAVD88 minus MSL; subtract it from MSL-referenced zeta.
	return surface - offset_grid[np.newaxis, :, :]


def _write_rtofs_point_timeseries(
	time_index, latitude, longitude, surface, csv_dir, forecast_date,
):
	"""Write one forecast's NAVD88 surface time series at the fixed UTM points."""
	import numpy as np
	import pandas as pd

	from compare_subtide_schism_ocean import _interpolate_rtofs_to_boundary

	latitude = np.asarray(latitude, dtype=float)
	longitude = np.asarray(longitude, dtype=float)
	surface = np.asarray(surface, dtype=float)
	if latitude.ndim != 1 or longitude.ndim != 1:
		raise ValueError("RTOFS point extraction requires 1-D latitude and longitude axes")
	if surface.ndim != 3 or surface.shape[1:] != (len(latitude), len(longitude)):
		raise ValueError("RTOFS surface shape does not match point-extraction coordinates")
	time_index = pd.DatetimeIndex(time_index, name="time")
	if len(time_index) != surface.shape[0]:
		raise ValueError("RTOFS time coordinate does not match surface time dimension")
	if time_index.tz is not None:
		time_index = time_index.tz_convert("Etc/GMT+8").tz_localize(None)

	point_xy = np.asarray(RTOFS_POINT_COORDS_UTM10N, dtype=float)
	point_indices = np.arange(1, len(point_xy) + 1)
	point_series = _interpolate_rtofs_to_boundary(
		time_index, latitude, longitude, surface, point_indices, point_xy
	)
	columns = [
		(
			f"point_{index}_easting_{easting:.3f}_northing_{northing:.3f}"
				"_surf_el_m_NAVD88"
		)
		for index, (easting, northing) in enumerate(point_xy, start=1)
	]
	point_series.columns = columns
	missing_points = [
		column for column in columns if not point_series[column].notna().any()
	]
	if missing_points:
		raise ValueError(
			"No RTOFS surface elevations are available for point(s): "
			+ ", ".join(missing_points)
		)

	csv_dir = Path(csv_dir)
	csv_dir.mkdir(parents=True, exist_ok=True)
	csv_path = csv_dir / f"forecast_rtofs_{forecast_date:%Y%m%d}.csv"
	point_series.to_csv(
		csv_path,
		index_label="time_fixed_PST",
		date_format="%Y-%m-%d %H:%M:%S",
	)
	logging.info("Saved NAVD88 RTOFS point time series to: %s", csv_path)
	return csv_path


def _write_rtofs_run_point_timeseries(
	surface_paths, processed_dir, forecast_date, vdatum_grid,
	lat_bounds, lon_bounds, lat_count, lon_count,
):
	"""Interpolate one run's full forecast and write its fixed-point CSV."""
	import numpy as np
	import pandas as pd

	surface = _load_rtofs_series(
		surface_paths, ["ssh"], lat_bounds, lon_bounds
	)
	utc_times = pd.date_range(
		pd.Timestamp(surface.MT.values[0]),
		pd.Timestamp(surface.MT.values[-1]),
		freq="h",
	)
	if len(utc_times) < 2:
		raise ValueError(
			f"Forecast {forecast_date:%Y-%m-%d} has fewer than two surface times"
		)
	latitudes = np.linspace(*lat_bounds, lat_count)
	longitudes = np.linspace(*lon_bounds, lon_count)
	hourly_surface = _interpolate_rtofs_grid(
		surface,
		["ssh"],
		utc_times.values,
		latitudes,
		longitudes,
		3,
	)["ssh"].values
	navd88_surface = _convert_rtofs_surface_to_navd88(
		hourly_surface, latitudes, longitudes, vdatum_grid
	)
	pst_times = utc_times - pd.Timedelta(hours=8)
	csv_dir = Path(processed_dir).parent / "csv"
	return _write_rtofs_point_timeseries(
		pst_times,
		latitudes,
		longitudes,
		navd88_surface,
		csv_dir,
		forecast_date,
	)


def _group_surface_paths_by_run(surface_paths):
	"""Group surface files by their dated RTOFS forecast-run directory."""
	grouped = {}
	for path in surface_paths:
		match = re.fullmatch(r"rtofs\.(\d{8})", path.parent.name)
		if match is None:
			raise ValueError(
				f"Cannot determine forecast initialization date from {path.parent}"
			)
		forecast_date = datetime.strptime(match.group(1), "%Y%m%d")
		grouped.setdefault((path.parent, forecast_date), []).append(path)
	return grouped


def _load_rtofs_series(paths, fields, lat_bounds, lon_bounds):
	"""Load a consistent cropped grid; newer runs win at duplicate valid times."""
	import numpy as np
	import pandas as pd
	import xarray as xr

	samples = []
	for path in sorted(paths):
		with xr.open_dataset(path) as source:
			data = subset_rtofs_dataset(source, lat_bounds, lon_bounds)
			data = data[fields].drop_vars("Date", errors="ignore").load()
		if samples:
			for coordinate in ("Latitude", "Longitude", "Depth"):
				if coordinate in samples[0]:
					if coordinate not in data or data[coordinate].shape != samples[0][coordinate].shape or not np.allclose(data[coordinate], samples[0][coordinate], equal_nan=True):
						raise ValueError(f"Inconsistent RTOFS {coordinate} grid in {path}")
		samples.append(data)
	combined = xr.concat(samples, dim="MT", data_vars="all", coords="minimal", compat="equals", join="exact")
	unique = ~pd.Index(combined.MT.values).duplicated(keep="last")
	return combined.isel(MT=unique).sortby("MT")


def process_rtofs(
	input_dir: Path, processed_dir: Path = None, start=None, end=None,
	lat_bounds=LAT_LON_BOUNDS["lat"], lon_bounds=LAT_LON_BOUNDS["lon"],
	lat_count: int = 51, lon_count: int = 38, region: str = "US_west",
	vdatum_grid: Path = None,
):
	"""Write complete hourly PST days combining downloaded RTOFS fields.

	Parameters
	----------
	input_dir : Path
		Directory containing rtofs.YYYYMMDD subdirectories, or one run directory.
		Both geographic subsets and original NetCDF files can be processed.
	processed_dir : Path, optional
		Destination, defaulting to a processed folder beside subset, or under
		input_dir when it is not named subset.
	start, end : datetime-like, optional
		Inclusive PST calendar dates, supplied together. By default, only
		complete days bracketed by both elevation and 3-D data are written.
	lat_bounds, lon_bounds : pair of float, optional
		Target box matching the HYCOM workflow by default.
	lat_count, lon_count : int, optional
		Regular target grid sizes, default 51 by 38 from hycomschema.
	region : {'US_west', 'US_east', 'alaska'}, optional
		Regional 3-D product to combine with the global elevation product.
	vdatum_grid : Path, optional
		NOAA WCOFS datum NetCDF containing latitude, longitude, and
		navd88tomsl. Defaults to wcofs_vdatums.nc beside processed_dir.

	Returns
	-------
	list of Path
		Files named rtofs_interpolated_hourly_pstYYYYMMDD.nc, with unlimited
		24-sample time, double temp/salt/surf_el, 1-D lat/lon, and all depths.
		One NAVD88 point time-series CSV per dated input forecast run is saved
		under processed_dir.parent / "csv"; each CSV contains the run's full
		hourly surface forecast.

	Notes
	-----
	PST is fixed UTC-8, not daylight-saving Pacific civil time. Duplicate
	valid times prefer the newest model run. Regridding preserves missing
	land values and does not extrapolate beyond the source grid. Existing
	processed days are replaced atomically after successful processing.
	"""
	import numpy as np
	import pandas as pd

	input_dir = Path(input_dir)
	if region not in {"US_west", "US_east", "alaska"}:
		raise ValueError("region must be US_west, US_east, or alaska")
	if (start is None) != (end is None):
		raise ValueError("Processing start and end dates must be supplied together")
	surface_paths = list(input_dir.rglob("rtofs_glo_2ds_*_diag.nc"))
	hydro_paths = list(input_dir.rglob(f"rtofs_glo_3dz_*_6hrly_hvr_{region}.nc"))
	if not surface_paths or not hydro_paths:
		raise ValueError(f"Need both elevation diagnostics and {region} 3-D NetCDF files under {input_dir}")
	surface = _load_rtofs_series(surface_paths, ["ssh"], lat_bounds, lon_bounds)
	hydrography = _load_rtofs_series(hydro_paths, ["temperature", "salinity"], lat_bounds, lon_bounds)
	first_utc = max(pd.Timestamp(surface.MT.values[0]), pd.Timestamp(hydrography.MT.values[0]))
	last_utc = min(pd.Timestamp(surface.MT.values[-1]), pd.Timestamp(hydrography.MT.values[-1]))
	first_day = (first_utc - pd.to_timedelta(8, unit="h")).ceil("d")
	last_day = (last_utc - pd.to_timedelta(31, unit="h")).floor("d")
	if first_day > last_day:
		raise ValueError("Downloaded fields do not bracket any complete 24-hour PST day")
	if start is not None:
		start, end = pd.Timestamp(start).normalize(), pd.Timestamp(end).normalize()
		if start > end:
			raise ValueError("Processing start date cannot be after end date")
		if start < first_day or end > last_day:
			raise ValueError(f"Requested PST dates need more input data; complete coverage is {first_day.date()} through {last_day.date()}")
	else:
		start, end = first_day, last_day
		logging.info("Complete PST days: %s through %s; incomplete edge days are skipped", start.date(), end.date())
	if processed_dir is None:
		processed_dir = (input_dir.parent if input_dir.name == "subset" else input_dir) / "processed"
	processed_dir = Path(processed_dir)
	if vdatum_grid is None:
		vdatum_candidates = (
			processed_dir.parent / "wcofs_vdatums.nc",
			processed_dir.parent / "wcofs_vdatum.nc",
			input_dir / "wcofs_vdatums.nc",
			input_dir / "wcofs_vdatum.nc",
		)
		vdatum_grid = next(
			(path for path in vdatum_candidates if path.is_file()), None
		)
		if vdatum_grid is None:
			raise FileNotFoundError(
				"No WCOFS datum grid found; pass --vdatum-grid with the NetCDF "
				"containing latitude, longitude, and navd88tomsl"
			)
	vdatum_grid = Path(vdatum_grid)
	if not vdatum_grid.is_file():
		raise FileNotFoundError(f"WCOFS vertical-datum grid not found: {vdatum_grid}")
	processed_dir.mkdir(parents=True, exist_ok=True)
	written = []
	for day in pd.date_range(start, end, freq="d"):
		data = interpolate_rtofs_day(surface, hydrography, day, lat_bounds, lon_bounds, lat_count, lon_count)
		data["surf_el"] = (
			data["surf_el"].dims,
			_convert_rtofs_surface_to_navd88(
				data["surf_el"].values,
				data["lat"].values,
				data["lon"].values,
				vdatum_grid,
			),
			dict(data["surf_el"].attrs),
		)
		data["surf_el"].attrs.update(
			vertical_datum="NAVD88",
			comment=(
				"RTOFS sea-surface elevation assumed referenced to MSL and "
				f"converted to NAVD88 using {vdatum_grid.name}."
			),
		)
		data.attrs["vertical_datum"] = "NAVD88"
		data.attrs["vertical_datum_grid"] = vdatum_grid.name
		data = data.assign_coords(time=("time", np.arange(24, dtype=np.int32), dict(data.time.attrs)))
		data.time.attrs.update(units=f"hours since {day:%Y-%m-%d} 00:00:00", calendar="proleptic_gregorian")
		data.attrs["source_files"] = "\n".join(str(path.relative_to(input_dir)) for path in sorted(surface_paths + hydro_paths))
		filename = processed_dir / f"rtofs_interpolated_hourly_pst{day:%Y%m%d}.nc"
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
		logging.info("Saved 24 hourly PST samples to: %s", filename)
	for (_, forecast_date), run_surface_paths in _group_surface_paths_by_run(
		surface_paths
	).items():
		_write_rtofs_run_point_timeseries(
			run_surface_paths,
			processed_dir,
			forecast_date,
			vdatum_grid,
			lat_bounds,
			lon_bounds,
			lat_count,
			lon_count,
		)
	return written


class S3Downloader:
	"""Access NOAA's public S3 archive without AWS credentials."""

	def __init__(self):
		self.bucket = BUCKET
		self._s3_client = None

	@property
	def s3(self):
		"""Lazily create an unsigned S3 client."""
		if self._s3_client is None:
			self._s3_client = boto3.client(
				"s3",
				config=Config(signature_version=UNSIGNED, region_name="us-east-1"),
			)
		return self._s3_client

	def list_files(self, prefix: str):
		"""Yield object metadata, including all pages of a listing."""
		paginator = self.s3.get_paginator("list_objects_v2")
		for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
			yield from page.get("Contents", [])

	def location(self, key: str) -> str:
		"""Return a display URL for a source object."""
		return f"s3://{self.bucket}/{key}"

	def source_identity(self) -> str:
		return f"s3://{self.bucket}"

	def download_file(self, key: str, output_path: Path, size: int) -> bool:
		"""Download to a temporary file and replace the destination on success."""
		temporary_path = output_path.with_name(output_path.name + ".part")
		try:
			output_path.parent.mkdir(parents=True, exist_ok=True)
			logging.info("Downloading: %s", self.location(key))
			self.s3.download_file(self.bucket, key, str(temporary_path))
			if temporary_path.stat().st_size != size:
				raise ValueError(f"Downloaded size does not match S3 object: {key}")
			temporary_path.replace(output_path)
			logging.info("Saved to: %s", output_path)
			return True
		except (BotoCoreError, ClientError, OSError, ValueError) as error:
			logging.error("Download failed for %s: %s", key, error)
			return False
		finally:
			temporary_path.unlink(missing_ok=True)


	def download_subset_file(
		self, key: str, output_path: Path, size: int,
		lat_bounds, lon_bounds, overwrite: bool = False,
	) -> bool:
		"""Download, crop, and atomically save a NetCDF subset; remove the source.

		Existing subsets are reused only when the source key, source size, and
		geographic bounds match. The full source is transferred from S3 even
		though only the cropped fields are retained locally.
		"""
		import xarray as xr

		if not key.endswith(".nc"):
			raise ValueError("Geographic subsetting requires NetCDF files; use --no-subset for binaries")
		metadata = {
			"subset_source_key": key,
			"subset_source_size": str(size),
			"subset_source": self.source_identity(),
			"subset_version": 2,
		}
		if not overwrite and output_path.is_file():
			try:
				with xr.open_dataset(output_path, decode_times=False) as existing:
					if (
						all(existing.attrs.get(name) == value for name, value in metadata.items())
						and list(existing.attrs.get("subset_lat_bounds", [])) == list(lat_bounds)
						and list(existing.attrs.get("subset_lon_bounds", [])) == list(lon_bounds)
					):
						logging.info("Already subsetted: %s", output_path)
						return True
			except (OSError, ValueError):
				logging.warning("Cannot read existing subset; rebuilding: %s", output_path)
		partial_path = output_path.with_name(output_path.name + ".part")
		try:
			output_path.parent.mkdir(parents=True, exist_ok=True)
			with TemporaryDirectory(prefix="rtofs_", dir=output_path.parent) as directory:
				source_path = Path(directory) / Path(key).name
				if not self.download_file(key, source_path, size):
					return False
				with xr.open_dataset(source_path) as data:
					subset = subset_rtofs_dataset(data, lat_bounds, lon_bounds)
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
			logging.info("Saved geographic subset to: %s", output_path)
			return True
		except (OSError, ValueError, KeyError) as error:
			logging.error("Subsetting failed for %s: %s", key, error)
			return False
		finally:
			partial_path.unlink(missing_ok=True)


class NomadsDownloader(S3Downloader):
	"""Download the same RTOFS NetCDF products from NOAA NOMADS over HTTPS."""

	def __init__(self):
		super().__init__()
		self.bucket = "NOMADS"

	def location(self, key: str) -> str:
		return f"{NOMADS_BASE_URL}/{key}"

	def source_identity(self) -> str:
		return NOMADS_BASE_URL

	def list_files(self, prefix: str):
		"""Read NOMADS' plain-text ls-l file list for one run date."""
		from urllib.error import HTTPError, URLError
		from urllib.request import urlopen

		date_dir = prefix.strip("/")
		url = f"{NOMADS_BASE_URL}/{date_dir}/ls-l"
		try:
			with urlopen(url, timeout=60) as response:
				lines = response.read().decode("ascii").splitlines()
		except (HTTPError, URLError, TimeoutError) as error:
			if isinstance(error, HTTPError) and error.code == 404:
				return
			raise OSError(f"Cannot list NOMADS directory {url}: {error}") from error
		for line in lines:
			parts = line.split(maxsplit=3)
			if len(parts) != 4 or not parts[2].isdigit():
				continue
			filename = parts[3]
			if filename.endswith(".nc"):
				yield {"Key": f"{date_dir}/{filename}", "Size": int(parts[2])}

	def download_file(self, key: str, output_path: Path, size: int) -> bool:
		"""Stream a NOMADS file to disk and atomically replace it on success."""
		from urllib.error import HTTPError, URLError
		from urllib.request import urlopen

		temporary_path = output_path.with_name(output_path.name + ".part")
		try:
			output_path.parent.mkdir(parents=True, exist_ok=True)
			logging.info("Downloading: %s", self.location(key))
			with urlopen(self.location(key), timeout=120) as response, open(temporary_path, "wb") as stream:
				while chunk := response.read(1024 * 1024):
					stream.write(chunk)
			if temporary_path.stat().st_size != size:
				raise ValueError(f"Downloaded size does not match NOMADS listing: {key}")
			temporary_path.replace(output_path)
			logging.info("Saved to: %s", output_path)
			return True
		except (HTTPError, URLError, TimeoutError, OSError, ValueError) as error:
			logging.error("NOMADS download failed for %s: %s", key, error)
			return False
		finally:
			temporary_path.unlink(missing_ok=True)

	def list_available_dates(self, days_back: int = 7):
		"""Return recent dates whose daily RTOFS NetCDF listing is available."""
		from urllib.error import HTTPError, URLError
		from urllib.request import Request, urlopen

		available = []
		day = datetime.now(timezone.utc).date()
		for offset in range(max(days_back * 3, days_back)):
			date = day - timedelta(days=offset)
			key = f"rtofs.{date:%Y%m%d}/rtofs_glo_2ds_f000_diag.nc"
			try:
				request = Request(self.location(key), method="HEAD")
				with urlopen(request, timeout=30):
					available.append(date)
			except HTTPError as error:
				if error.code != 404:
					raise OSError(f"Cannot check NOMADS object {key}: {error}") from error
			except (URLError, TimeoutError) as error:
				raise OSError(f"Cannot check NOMADS object {key}: {error}") from error
			if len(available) >= days_back:
				break
		return available


def download_rtofs(
	output_dir: Path,
	s3_downloader: S3Downloader,
	date: datetime = None,
	pattern=None,
	forecast_steps: range = None,
	run_type: str = "forecast",
	dry_run: bool = False,
	overwrite: bool = False,
	region: str = "US_west",
	lat_bounds=LAT_LON_BOUNDS["lat"],
	lon_bounds=LAT_LON_BOUNDS["lon"],
	subset: bool = True,
) -> bool:
	"""Download matching RTOFS files for one daily model run.

	Parameters
	----------
	output_dir : Path
		Parent directory; cropped files use subset/rtofs.YYYYMMDD folders.
		Original files use rtofs.YYYYMMDD folders when subset is False.
	s3_downloader : S3Downloader
		Public S3 downloader instance.
	date : datetime, optional
		Run date, defaulting to today's UTC date.
	pattern : str or sequence of str, optional
		Shell-style filename patterns. Defaults to surface elevation diagnostics
		and regional full-depth temperature and salinity (40 depth levels).
		Each pattern must match at least one file after applying time filters.
	forecast_steps : range, optional
		Lead hours to select, not file indices. None selects all available hours.
		Regional 3-D temperature and salinity are published at six-hour intervals.
	run_type : {'forecast', 'nowcast', 'all'}, optional
		Select f-prefixed forecasts, n-prefixed nowcasts, or both.
	dry_run : bool, optional
		List selected files without downloading or creating directories.
	overwrite : bool, optional
		Replace existing files even when their source metadata matches.
	region : {'US_west', 'US_east', 'alaska'}, optional
		Region for the default 3-D product. Ignored when pattern is supplied.
		Elevation diagnostics use the global grid regardless of region.
	lat_bounds : pair of float, optional
		Inclusive south/north latitude limits, default (37, 39).
	lon_bounds : pair of float, optional
		Inclusive west/east longitude limits on a 0-360 convention,
		default (236, 239).
	subset : bool, optional
		Save only ssh, temperature, salinity, and their coordinates within the
		box, keeping all depths. False retains original files without cropping.

	Returns
	-------
	bool
		True if each requested product matches and all selected transfers succeed.
		Matching existing subsets or original files count as successful.
	"""
	if run_type not in {"forecast", "nowcast", "all"}:
		raise ValueError("run_type must be forecast, nowcast, or all")
	if region not in {"US_west", "US_east", "alaska"}:
		raise ValueError("region must be US_west, US_east, or alaska")
	if not (-90 <= lat_bounds[0] < lat_bounds[1] <= 90):
		raise ValueError("Latitude bounds must satisfy -90 <= SOUTH < NORTH <= 90")
	if not (0 <= lon_bounds[0] < lon_bounds[1] <= 360):
		raise ValueError("Longitude bounds must satisfy 0 <= WEST < EAST <= 360")
	if pattern is None:
		patterns = (DEFAULT_PATTERNS[0], f"rtofs_glo_3dz_*_6hrly_hvr_{region}.nc")
	else:
		patterns = (pattern,) if isinstance(pattern, str) else tuple(pattern)
	if not patterns:
		raise ValueError("At least one filename pattern is required")
	if date is None:
		date = datetime.now(timezone.utc)
	prefix = f"rtofs.{date:%Y%m%d}/"
	selected = []
	matched_patterns = set()
	try:
		for obj in s3_downloader.list_files(prefix):
			filename = Path(obj["Key"]).name
			matching = {candidate for candidate in patterns if fnmatch.fnmatchcase(filename, candidate)}
			if not matching:
				continue
			match = LEAD_TIME.search(filename)
			if match is None:
				continue
			kind, hour = match.groups()
			if run_type != "all" and kind != {"forecast": "f", "nowcast": "n"}[run_type]:
				continue
			if forecast_steps is not None and int(hour) not in forecast_steps:
				continue
			selected.append(obj)
			matched_patterns.update(matching)
	except (BotoCoreError, ClientError, OSError) as error:
		logging.error("Cannot list %s: %s", s3_downloader.location(prefix), error)
		return False

	missing_patterns = set(patterns) - matched_patterns
	if missing_patterns:
		logging.error("Missing requested products for %s: %s", prefix, ", ".join(sorted(missing_patterns)))
		logging.error("Check run availability and lead hours; regional 3-D fields use six-hour intervals")
		return False

	logging.info(
		"%s: selected %d files (%.2f GiB)",
		prefix, len(selected), sum(obj["Size"] for obj in selected) / 1024**3,
	)
	if subset:
		logging.info("Subset box: latitude %s, longitude %s; full source files must be transferred", lat_bounds, lon_bounds)
	success = True
	for obj in selected:
		output_path = Path(output_dir) / "subset" / obj["Key"] if subset else Path(output_dir) / obj["Key"]
		if dry_run:
			logging.info("%s (%d bytes)", s3_downloader.location(obj["Key"]), obj["Size"])
		elif subset:
			if not s3_downloader.download_subset_file(
				obj["Key"], output_path, obj["Size"], lat_bounds, lon_bounds, overwrite,
			):
				success = False
		elif not overwrite and output_path.is_file() and output_path.stat().st_size == obj["Size"]:
			logging.info("Already downloaded: %s", output_path)
		elif not s3_downloader.download_file(obj["Key"], output_path, obj["Size"]):
			success = False
	return success


def list_available_dates(s3_downloader: S3Downloader, days_back: int = 7) -> None:
	"""Log the most recent available run dates using a paginated S3 listing."""
	if isinstance(s3_downloader, NomadsDownloader):
		for date in s3_downloader.list_available_dates(days_back):
			logging.info("%s", date)
		return
	dates = set()
	paginator = s3_downloader.s3.get_paginator("list_objects_v2")
	for page in paginator.paginate(Bucket=BUCKET, Prefix="rtofs.", Delimiter="/"):
		for entry in page.get("CommonPrefixes", []):
			match = re.fullmatch(r"rtofs\.(\d{8})/", entry["Prefix"])
			if match:
				dates.add(match.group(1))
	if not dates:
		logging.warning("No RTOFS dates found")
	for date in sorted(dates, reverse=True)[:days_back]:
		logging.info("%s", datetime.strptime(date, "%Y%m%d").date())


def parse_forecast_steps(value: str) -> range:
	"""Parse a single lead hour or inclusive START:END[:STRIDE] range."""
	try:
		parts = [int(part) for part in value.split(":")]
		if len(parts) == 1:
			start = end = parts[0]
			stride = 1
		elif len(parts) in (2, 3):
			start, end = parts[:2]
			stride = parts[2] if len(parts) == 3 else 1
		else:
			raise ValueError
		if start < 0 or end < start or stride < 1:
			raise ValueError
		return range(start, end + 1, stride)
	except ValueError as error:
		raise click.BadParameter(
			"Use a nonnegative hour or START:END[:STRIDE], with END >= START and STRIDE > 0"
		) from error


class ForecastStepsType(click.ParamType):
	name = "hours"

	def convert(self, value, param, ctx):
		if value is None:
			return None
		try:
			return parse_forecast_steps(value)
		except click.BadParameter as error:
			self.fail(str(error), param, ctx)


def _validate_bounds(ctx, param, value):
	if value is None:
		return value
	if param.name == "lat_bounds" and not (-90 <= value[0] < value[1] <= 90):
		raise click.BadParameter("must satisfy -90 <= SOUTH < NORTH <= 90")
	if param.name == "lon_bounds" and not (0 <= value[0] < value[1] <= 360):
		raise click.BadParameter("must satisfy 0 <= WEST < EAST <= 360")
	return value


@click.command(
	context_settings={"help_option_names": ["-h", "--help"]},
	help="Download NOAA RTOFS data from public S3 or NOMADS and write daily hourly-PST NetCDF files.",
	epilog="""\
Examples:
  python download_rtofs.py -d 2026-10-11 -fs 6:24:6 --dry-run
  python download_rtofs.py -sdate 2026-10-01 -edate 2026-10-07 -fs 6:192:6 -o ./data
  python download_rtofs.py -d 2026-10-11 --lat-bounds 37 39 --lon-bounds 236 239 -fs 6:24:6
  python download_rtofs.py -d 2026-10-14 --source nomads -fs 96:192:6 --dry-run
  python download_rtofs.py --list-dates
  python download_rtofs.py --process-only -o ./data
  python download_rtofs.py --process-only -o ./data --process-start 2026-10-12 --process-end 2026-10-13
  python download_rtofs.py -d 2026-10-12 -o ./data --skip-processing

Forecast steps are lead hours, not WCOFS-style file indices.
Only published files are selected; each requested product must match.
Defaults: surface elevation (ssh), full-depth temperature, and salinity.
Temperature and salinity: US West Coast, 40 standard depth levels, six-hourly.
Elevation: global source grid. Use -fs 6:192:6 for matching forecast hours.
Use --source nomads for operational products through forecast hour 192 (8 days).
The default --source s3 keeps using the public S3 archive.
Default box: latitude 37-39 N, longitude 236-239 E (124-121 W).
Only ssh, temperature, salinity and coordinates are saved, at all depths.
Cropped output: OUTPUT/subset/rtofs.YYYYMMDD/; temporary sources are removed.
S3 transfers whole files before cropping; --dry-run sizes are transfer sizes.
Use --no-subset to retain original files without geographic/variable filtering.
Processing combines temp, salt, and surf_el on a regular 51x38 lat/lon grid.
All depths are retained; temporal and spatial interpolation are linear.
Each processed file has 24 hours in fixed PST (UTC-8, without daylight saving).
Incomplete edge days are skipped; no time/spatial extrapolation is performed.
Processed output: OUTPUT/processed/rtofs_interpolated_hourly_pstYYYYMMDD.nc.
Point time series: one OUTPUT/csv/forecast_rtofs_YYYYMMDD.csv per input forecast
run, containing all hourly forecast times at the three configured UTM zone 10N
locations. RTOFS surface elevation is assumed MSL-referenced and converted to
NAVD88 using OUTPUT/wcofs_vdatums.nc by default. Override with --vdatum-grid.
Processing dates are PST dates, distinct from UTC forecast run dates.
Use --process-only with downloaded files; --skip-processing disables processing.""",
)
@click.option("-d", "--date", type=click.DateTime(formats=["%Y-%m-%d"]), help="Run date (YYYY-MM-DD; default: today UTC)")
@click.option("-sdate", "--start", type=click.DateTime(formats=["%Y-%m-%d"]), help="First run date (inclusive)")
@click.option("-edate", "--end", type=click.DateTime(formats=["%Y-%m-%d"]), help="Last run date (inclusive)")
@click.option("-o", "--output", type=click.Path(path_type=Path), default=Path("./rtofs_data"), show_default=True, help="Output directory")
@click.option("-fs", "--forecast-steps", type=ForecastStepsType(), help="Lead hours: HOUR or START:END[:STRIDE] (default: all available)")
@click.option("--pattern", multiple=True, help="Filename glob to download; repeat this option for multiple patterns")
@click.option("--region", type=click.Choice(["US_west", "US_east", "alaska"]), default="US_west", show_default=True, help="Default 3-D product region; elevation remains global")
@click.option("--source", type=click.Choice(["s3", "nomads"]), default="s3", show_default=True, help="RTOFS source; NOMADS operational forecasts extend through f192")
@click.option("--lat-bounds", type=float, nargs=2, default=LAT_LON_BOUNDS["lat"], metavar="SOUTH NORTH", callback=_validate_bounds, help="Latitude box")
@click.option("--lon-bounds", type=float, nargs=2, default=LAT_LON_BOUNDS["lon"], metavar="WEST EAST", callback=_validate_bounds, help="Longitude box in degrees east, 0-360")
@click.option("--no-subset", is_flag=True, help="Retain original NOAA files without cropping or filtering fields")
@click.option("--run-type", type=click.Choice(["forecast", "nowcast", "all"]), default="forecast", show_default=True, help="Product time type")
@click.option("--list-dates", is_flag=True, help="List recent available run dates and exit")
@click.option("--days-back", type=click.IntRange(min=1), default=7, show_default=True, help="Number of available dates to list")
@click.option("--process-only", is_flag=True, help="Process existing downloads without contacting NOAA")
@click.option("--skip-processing", is_flag=True, help="Download only, without generating daily processed files")
@click.option("--input-dir", type=click.Path(path_type=Path), help="Processing input folder")
@click.option("--processed-dir", type=click.Path(path_type=Path), help="Daily processed output folder")
@click.option("--process-start", type=click.DateTime(formats=["%Y-%m-%d"]), help="First PST calendar date to process (inclusive)")
@click.option("--process-end", type=click.DateTime(formats=["%Y-%m-%d"]), help="Last PST calendar date to process (inclusive)")
@click.option("--vdatum-grid", type=click.Path(path_type=Path), help="WCOFS datum NetCDF containing latitude, longitude, and navd88tomsl; defaults to <output>/wcofs_vdatums.nc")
@click.option("--lat-count", type=click.IntRange(min=2), default=51, show_default=True, help="Regular processed-grid latitude count")
@click.option("--lon-count", type=click.IntRange(min=2), default=38, show_default=True, help="Regular processed-grid longitude count")
@click.option("--dry-run", is_flag=True, help="List selected source files and sizes without downloading")
@click.option("--overwrite", is_flag=True, help="Download again even if local file size matches")
@click.option("--debug", is_flag=True, help="Enable debug logging")
def main(
	date, start, end, output, forecast_steps, pattern, region, source,
	lat_bounds, lon_bounds, no_subset, run_type, list_dates, days_back,
	process_only, skip_processing, input_dir, processed_dir, process_start,
	process_end, vdatum_grid, lat_count, lon_count, dry_run, overwrite, debug,
):
	"""Download forecast runs and/or combine complete hourly PST days."""
	if bool(start) != bool(end):
		raise click.UsageError("--start and --end must be supplied together")
	if date and start:
		raise click.UsageError("Use --date or --start/--end, not both")
	if start and start > end:
		raise click.UsageError("Start date cannot be after end date")
	if bool(process_start) != bool(process_end):
		raise click.UsageError("--process-start and --process-end must be supplied together")
	if process_start and process_start > process_end:
		raise click.UsageError("Processing start date cannot be after end date")
	if process_only and (dry_run or list_dates or date or start):
		raise click.UsageError(
			"--process-only cannot use download dates, --dry-run, or --list-dates; "
			"use --process-start/--process-end for PST dates"
		)
	if process_only and skip_processing:
		raise click.UsageError("--process-only and --skip-processing cannot be used together")

	setup_logging(debug)
	downloader = NomadsDownloader() if source == "nomads" else S3Downloader()
	processing_options = {
		"input_dir": input_dir or (output if no_subset else output / "subset"),
		"processed_dir": processed_dir or output / "processed",
		"start": process_start, "end": process_end,
		"lat_bounds": lat_bounds, "lon_bounds": lon_bounds,
		"lat_count": lat_count, "lon_count": lon_count, "region": region,
		"vdatum_grid": vdatum_grid,
	}
	try:
		if process_only:
			process_rtofs(**processing_options)
			return
		if list_dates:
			list_available_dates(downloader, days_back)
			return
		date = start or date or datetime.now(timezone.utc)
		end_date = end or date
		success = True
		while date <= end_date:
			if not download_rtofs(
				output, downloader, date=date, pattern=pattern or None,
				forecast_steps=forecast_steps, run_type=run_type,
				dry_run=dry_run, overwrite=overwrite,
				region=region,
				lat_bounds=lat_bounds, lon_bounds=lon_bounds,
				subset=not no_subset,
			):
				success = False
			date += timedelta(days=1)
		if success and not skip_processing and not dry_run:
			process_rtofs(**processing_options)
		logging.info("%s", "Completed successfully" if success else "Some downloads failed")
		if not success:
			raise click.exceptions.Exit(1)
	except (BotoCoreError, ClientError, OSError, ImportError, ValueError) as error:
		logging.error("RTOFS download/processing failed: %s", error)
		raise click.ClickException(str(error)) from error


if __name__ == "__main__":
	main()
