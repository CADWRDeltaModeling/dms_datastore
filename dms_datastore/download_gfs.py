"""Download NOAA GFS 0.25-degree forecasts as daily Pacific SCHISM sflux files.

State variables are interpolated hourly after lead 120. Precipitation and
radiation averages are de-averaged into separate forecast intervals and held
constant within each interval, preserving their water/energy integrals.
Pacific calendar days use America/Los_Angeles, including daylight saving time.
Use --source s3 for historical runs from the public NOAA AWS archive; the
default NOMADS source supports server-side geographic filtering for recent runs.
"""

import logging
import re
from contextlib import closing
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import click
import numpy as np
import pytz


LAT_BOUNDS = (37.36, 39.0)
LON_BOUNDS = (-123.023, -121.16)
PACIFIC = pytz.timezone("America/Los_Angeles")
FLUX_NAMES = ("prate", "dlwrf", "dswrf")
STATE_NAMES = ("stmp", "spfh", "uwind", "vwind", "prmsl")
FIELD_NAMES = STATE_NAMES + FLUX_NAMES
FILTER_URL = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"
S3_BUCKET = "noaa-gfs-bdp-pds"
SOURCE_DESCRIPTIONS = {
	"nomads": "NOAA NOMADS GFS pgrb2.0p25; native spatial grid",
	"s3": f"NOAA AWS S3 {S3_BUCKET} GFS pgrb2.0p25; native spatial grid",
}
GRIB_NAMES = {
	"2t": "stmp", "t": "stmp", "2sh": "spfh", "sh": "spfh",
	"10u": "uwind", "u": "uwind", "10v": "vwind", "v": "vwind",
	"prmsl": "prmsl", "msl": "prmsl", "prate": "prate",
	"dlwrf": "dlwrf", "sdlwrf": "dlwrf", "dswrf": "dswrf", "sdswrf": "dswrf",
}
FIELD_ATTRIBUTES = {
	"stmp": {"units": "K", "long_name": "2m_above_ground/Temperature"},
	"spfh": {"units": "kg kg-1", "long_name": "2m_above_ground/Specific Humidity",
			 "standard_name": "specific_humidity"},
	"uwind": {"units": "m/s", "long_name": "10m_above_ground/UGRD",
			  "standard_name": "eastward_wind"},
	"vwind": {"units": "m/s", "long_name": "10m_above_ground/VGRD",
			  "standard_name": "northward_wind"},
	"prmsl": {"units": "Pa", "long_name": "Pressure reduced to MSL",
			  "standard_name": "air_pressure_at_sea_level"},
	"prate": {"units": "kg m-2 s-1", "long_name": "Precipitation rate"},
	"dlwrf": {"units": "W m-2", "long_name": "Downward long-wave radiation flux"},
	"dswrf": {"units": "W m-2", "long_name": "Downward short-wave radiation flux"},
}


def forecast_hours(max_hour=384):
	"""Return native GFS leads (hourly to 120, then every three hours).

	Parameters
	----------
	max_hour : int, optional
		Last native lead to download, from 1 through 384.

	Returns
	-------
	list of int
		Positive forecast leads; analysis lacks the required flux averages.
	"""
	if not 1 <= max_hour <= 384 or (max_hour > 120 and max_hour % 3):
		raise ValueError("Last lead must be 1..120 or a multiple of 3 up to 384")
	return list(range(1, min(max_hour, 120) + 1)) + list(range(123, max_hour + 1, 3))


def complete_sflux_days(initialization, first_hour, last_hour):
	"""Map complete Pacific dates to integer UTC leads at hourly interval starts.

	Both state interpolation and flux coverage must extend to the next local
	midnight. Leading/trailing partial days are omitted, never extrapolated.
	"""
	if initialization.tzinfo is None or first_hour >= last_hour:
		raise ValueError("Need a timezone-aware initialization and increasing coverage")
	first_date = (initialization + timedelta(hours=int(first_hour))).astimezone(PACIFIC).date()
	last_date = (initialization + timedelta(hours=int(last_hour))).astimezone(PACIFIC).date()
	result = {}
	valid_date = first_date
	while valid_date <= last_date:
		midnight = PACIFIC.localize(datetime.combine(valid_date, time.min))
		next_midnight = PACIFIC.localize(datetime.combine(valid_date + timedelta(days=1), time.min))
		start = int((midnight.astimezone(timezone.utc) - initialization).total_seconds() / 3600)
		stop = int((next_midnight.astimezone(timezone.utc) - initialization).total_seconds() / 3600)
		if first_hour <= start and stop <= last_hour:
			result[valid_date] = list(range(start, stop))
		valid_date += timedelta(days=1)
	return result


def separate_flux_intervals(starts, ends, averages):
	"""Recover non-overlapping means from GFS growing-window mean fluxes.

	Parameters
	----------
	starts, ends : sequence of int
		Beginning/end of each average in hours since initialization.
	averages : array_like
		Mean fluxes, with forecast time on the first axis.

	Returns
	-------
	tuple
		Separate interval starts, ends, and mean values. Tiny negative rates
		from GRIB packing are clipped to zero and reported.
	"""
	starts = np.asarray(starts, dtype=int)
	ends = np.asarray(ends, dtype=int)
	averages = np.asarray(averages, dtype="float64")
	if not len(ends) or len(starts) != len(ends) or averages.shape[0] != len(ends):
		raise ValueError("Need one start, end, and flux field per record")
	if np.any(ends <= starts) or np.any(np.diff(ends) <= 0) or not np.isfinite(averages).all():
		raise ValueError("Invalid flux intervals or non-finite values")
	intervals = starts.copy()
	values = averages.copy()
	for index in range(1, len(ends)):
		if starts[index] == starts[index - 1]:
			intervals[index] = ends[index - 1]
			total = averages[index] * (ends[index] - starts[index])
			previous = averages[index - 1] * (ends[index - 1] - starts[index - 1])
			values[index] = (total - previous) / (ends[index] - ends[index - 1])
		elif starts[index] != ends[index - 1]:
			raise ValueError("Gap or overlap in GFS flux averaging windows")
	if np.any(values < 0):
		logging.warning("Clipping %d negative de-averaged flux values (minimum %g)",
						np.count_nonzero(values < 0), values.min())
		values = np.maximum(values, 0)
	return intervals, ends, values


def validate_bounds(lat_bounds, lon_bounds):
	"""Validate an ordered, non-dateline-crossing signed-longitude box."""
	if not -90 <= lat_bounds[0] < lat_bounds[1] <= 90:
		raise ValueError("Latitude bounds must increase within -90..90")
	if not -180 <= lon_bounds[0] < lon_bounds[1] <= 180:
		raise ValueError("Longitude bounds must increase within -180..180")


def gfs_url(initialization, hour, lat_bounds=LAT_BOUNDS, lon_bounds=LON_BOUNDS):
	"""Build a NOMADS request for only the required levels, fields, and box."""
	validate_bounds(lat_bounds, lon_bounds)
	parameters = {
		"file": f"gfs.t{initialization:%H}z.pgrb2.0p25.f{hour:03d}",
		"dir": f"/gfs.{initialization:%Y%m%d}/{initialization:%H}/atmos",
		"subregion": "", "leftlon": lon_bounds[0], "rightlon": lon_bounds[1],
		"bottomlat": lat_bounds[0], "toplat": lat_bounds[1],
	}
	for variable in ("PRMSL", "TMP", "SPFH", "UGRD", "VGRD", "PRATE", "DSWRF", "DLWRF"):
		parameters[f"var_{variable}"] = "on"
	for level in ("2_m_above_ground", "10_m_above_ground", "mean_sea_level", "surface"):
		parameters[f"lev_{level}"] = "on"
	return FILTER_URL + "?" + urlencode(parameters)


def s3_byte_ranges(index_text, object_size, hour):
	"""Select and merge byte ranges for the eight required fields in a GFS index.

	Parameters
	----------
	index_text : str
		Contents of the GRIB object's .idx companion.
	object_size : int
		Full GRIB object size in bytes, used to bound the last record.
	hour : int
		Forecast lead; only instantaneous states and averaged fluxes are selected.

	Returns
	-------
	list of tuple
		Inclusive start/end offsets, merging adjacent selected GRIB records.
	"""
	fields = {
		("PRMSL", "mean sea level"): "prmsl", ("TMP", "2 m above ground"): "stmp",
		("SPFH", "2 m above ground"): "spfh", ("UGRD", "10 m above ground"): "uwind",
		("VGRD", "10 m above ground"): "vwind", ("PRATE", "surface"): "prate",
		("DSWRF", "surface"): "dswrf", ("DLWRF", "surface"): "dlwrf",
	}
	records = []
	for line in index_text.splitlines():
		if not line.strip():
			continue
		parts = line.split(":", 6)
		if len(parts) < 6:
			raise ValueError("Malformed S3 GRIB index record")
		records.append((int(parts[1]), parts[3], parts[4], parts[5]))
	offsets = [record[0] for record in records]
	if (not offsets or offsets[0] != 0 or offsets[-1] >= object_size
			or any(first >= second for first, second in zip(offsets, offsets[1:]))):
		raise ValueError("Invalid S3 GRIB index byte offsets")
	selected, ranges = set(), []
	for position, (offset, variable, level, forecast) in enumerate(records):
		name = fields.get((variable, level))
		if name is None:
			continue
		if name in FLUX_NAMES:
			matches = re.fullmatch(rf"\d+-{hour} hour ave fcst", forecast)
		else:
			matches = forecast == f"{hour} hour fcst"
		if not matches:
			continue
		if name in selected:
			raise ValueError(f"Duplicate selected S3 index field: {name}")
		selected.add(name)
		end = offsets[position + 1] - 1 if position + 1 < len(offsets) else object_size - 1
		if ranges and ranges[-1][1] + 1 == offset:
			ranges[-1] = (ranges[-1][0], end)
		else:
			ranges.append((offset, end))
	missing = set(FIELD_NAMES) - selected
	if missing:
		raise ValueError(f"Missing S3 index fields: {', '.join(sorted(missing))}")
	return ranges


def create_s3_client():
	"""Create an unsigned public S3 client; AWS credentials are not required."""
	import boto3
	from botocore import UNSIGNED
	from botocore.config import Config

	return boto3.client("s3", config=Config(signature_version=UNSIGNED))


def download_s3_fields(initialization, hour, client):
	"""Fetch indexed forcing messages from public S3 without the full GRIB file.

	S3 subsets retain the global field grids. Geographic cropping is performed
	by read_sflux_fields, giving the same native-grid output as NOMADS.
	"""
	key = f"gfs.{initialization:%Y%m%d}/{initialization:%H}/atmos/gfs.t{initialization:%H}z.pgrb2.0p25.f{hour:03d}"
	size = client.head_object(Bucket=S3_BUCKET, Key=key)["ContentLength"]
	with closing(client.get_object(Bucket=S3_BUCKET, Key=key + ".idx")["Body"]) as stream:
		index_text = stream.read().decode("utf-8")
	ranges = s3_byte_ranges(index_text, size, hour)
	parts = []
	for start, end in ranges:
		response = client.get_object(Bucket=S3_BUCKET, Key=key, Range=f"bytes={start}-{end}")
		with closing(response["Body"]) as stream:
			payload = stream.read()
		if (response.get("ContentRange") != f"bytes {start}-{end}/{size}"
				or len(payload) != end - start + 1):
			raise ValueError(f"Incomplete or incorrect S3 byte range for {key}")
		parts.append(payload)
	logging.info("S3 f%03d: downloaded %.2f MiB of %.2f MiB",
				 hour, sum(map(len, parts)) / 2**20, size / 2**20)
	return b"".join(parts)


def read_sflux_fields(payload, initialization, hour, lat_bounds=LAT_BOUNDS, lon_bounds=LON_BOUNDS):
	"""Decode and validate selected GRIB messages from either source using ecCodes.

	Select instantaneous state fields and averaged surface fluxes explicitly.
	Return fields, per-flux averaging windows, and cropped 2-D lat/lon arrays.
	"""
	from eccodes import (codes_get, codes_get_array, codes_get_message_size,
						 codes_new_from_message, codes_release)

	if not payload.startswith(b"GRIB"):
		raise ValueError("Download did not return GRIB data (check run availability)")
	fields, windows = {}, {}
	grid_lat = grid_lon = None
	offset = 0
	while offset < len(payload):
		handle = codes_new_from_message(payload[offset:])
		try:
			message_size = codes_get_message_size(handle)
			if message_size <= 0 or offset + message_size > len(payload):
				raise ValueError("Truncated GRIB message")
			offset += message_size
			name = GRIB_NAMES.get(codes_get(handle, "shortName"))
			level_type = codes_get(handle, "typeOfLevel")
			level = codes_get(handle, "level")
			step_type = codes_get(handle, "stepType")
			if name in ("stmp", "spfh"):
				selected = level_type == "heightAboveGround" and level == 2 and step_type == "instant"
			elif name in ("uwind", "vwind"):
				selected = level_type == "heightAboveGround" and level == 10 and step_type == "instant"
			elif name == "prmsl":
				selected = level_type == "meanSea" and step_type == "instant"
			else:
				selected = name in FLUX_NAMES and level_type == "surface" and step_type == "avg"
			if not selected:
				continue
			if name in fields:
				raise ValueError(f"Duplicate selected GRIB field: {name}")
			if (codes_get(handle, "dataDate") != int(initialization.strftime("%Y%m%d"))
					or codes_get(handle, "dataTime") != initialization.hour * 100
					or codes_get(handle, "endStep") != hour
					or codes_get(handle, "stepUnits") != 1):
				raise ValueError(f"Unexpected initialization/lead/time units for {name}")
			shape = (codes_get(handle, "Nj"), codes_get(handle, "Ni"))
			lat = codes_get_array(handle, "latitudes").reshape(shape)
			lon = (codes_get_array(handle, "longitudes").reshape(shape) + 180) % 360 - 180
			if grid_lat is None:
				mask = ((lat >= lat_bounds[0]) & (lat <= lat_bounds[1])
						& (lon >= lon_bounds[0]) & (lon <= lon_bounds[1]))
				rows, columns = np.where(mask)
				if not len(rows):
					raise ValueError("No native GFS grid points inside requested box")
				row_slice = slice(rows.min(), rows.max() + 1)
				column_slice = slice(columns.min(), columns.max() + 1)
				grid_lat, grid_lon = lat, lon
			elif (lat.shape != grid_lat.shape or not np.allclose(lat, grid_lat)
				  or not np.allclose(lon, grid_lon)):
				raise ValueError("GRIB fields do not share a grid")
			values = codes_get_array(handle, "values").reshape(shape)[row_slice, column_slice]
			if (codes_get(handle, "numberOfMissing") or not np.isfinite(values).all()):
				raise ValueError(f"Missing values in {name}")
			fields[name] = values.astype("float32")
			if name in FLUX_NAMES:
				windows[name] = (int(codes_get(handle, "startStep")), hour)
		finally:
			codes_release(handle)
	missing = set(FIELD_NAMES) - set(fields)
	if missing:
		raise ValueError(f"Missing GFS fields: {', '.join(sorted(missing))}")
	return fields, windows, grid_lat[row_slice, column_slice], grid_lon[row_slice, column_slice]


def download_forecast(initialization, hours, cache_dir, lat_bounds=LAT_BOUNDS,
					  lon_bounds=LON_BOUNDS, overwrite=False, process_only=False,
					  source="nomads"):
	"""Fetch selected GRIB fields; validate before atomic replacement.

	Parameters
	----------
	initialization : datetime
		Timezone-aware UTC run initialization.
	hours : sequence of int
		Native forecast leads to retrieve.
	cache_dir : path-like
		Root for run/box-specific GRIB cache directories.
	lat_bounds, lon_bounds : pair of float, optional
		Geographic subset box, in signed degrees.
	overwrite : bool, optional
		Replace existing cached GRIB subsets.
	process_only : bool, optional
		Read existing subsets without accessing NOAA.
	source : {"nomads", "s3"}, optional
		NOMADS crops on the server. Public S3 fetches only indexed forcing
		messages, retaining global grids in its cache and cropping on decode.
		S3 caches are separate, under cache_dir/s3; no AWS credentials needed.

	Returns
	-------
	list of tuple
		Decoded fields, windows, latitude, longitude for each requested lead.
	"""
	import hashlib

	if source not in SOURCE_DESCRIPTIONS:
		raise ValueError("Source must be nomads or s3")
	validate_bounds(lat_bounds, lon_bounds)
	box_id = hashlib.sha256(repr((tuple(lat_bounds), tuple(lon_bounds))).encode()).hexdigest()[:12]
	cache_root = Path(cache_dir) / "s3" if source == "s3" else Path(cache_dir)
	directory = cache_root / f"gfs.{initialization:%Y%m%d%H}" / box_id
	directory.mkdir(parents=True, exist_ok=True)
	records = []
	s3_client = None
	for hour in hours:
		path = directory / f"gfs.t{initialization:%H}z.pgrb2.0p25.f{hour:03d}.subset.grib2"
		if path.exists() and (not overwrite or process_only):
			logging.info("Using cached f%03d", hour)
			payload = path.read_bytes()
		else:
			if process_only:
				raise FileNotFoundError(f"Missing cached subset: {path}")
			logging.info("Downloading %s %s f%03d", source.upper(), initialization.strftime("%Y-%m-%d %HZ"), hour)
			if source == "s3":
				if s3_client is None:
					s3_client = create_s3_client()
				payload = download_s3_fields(initialization, hour, s3_client)
			else:
				request = Request(gfs_url(initialization, hour, lat_bounds, lon_bounds),
								  headers={"User-Agent": "GFS-SCHISM-downloader/1.0"})
				with urlopen(request, timeout=180) as response:
					payload = response.read()
					length = response.headers.get("Content-Length")
					if length is not None and len(payload) != int(length):
						raise ValueError(f"Incomplete GFS response for f{hour:03d}")
		record = read_sflux_fields(payload, initialization, hour, lat_bounds, lon_bounds)
		if not path.exists() or (overwrite and not process_only):
			partial = path.with_name(path.name + ".part")
			try:
				partial.write_bytes(payload)
				partial.replace(path)
			finally:
				partial.unlink(missing_ok=True)
		records.append(record)
	return records


def hourly_fields(hours, records):
	"""Convert consecutive native forecasts to hourly states and interval means.

	Return integer UTC leads, hourly fields, latitude, and longitude. State
	values refer to interval starts; flux values are means over the following
	hour. Do not extrapolate or bridge missing native forecasts.
	"""
	from scipy.interpolate import interp1d

	hours = list(hours)
	if len(records) != len(hours) or len(hours) < 2:
		raise ValueError("Need at least two matching native forecast records")
	expected = [lead for lead in forecast_hours(hours[-1]) if lead >= hours[0]]
	if hours != expected:
		raise ValueError("Missing or unordered native GFS forecast leads")
	lat, lon = records[0][2:]
	for record in records[1:]:
		if (record[2].shape != lat.shape or not np.allclose(record[2], lat)
				or not np.allclose(record[3], lon)):
			raise ValueError("GFS grid changed between forecast leads")
	leads = np.arange(hours[0], hours[-1], dtype=int)
	result = {}
	for name in STATE_NAMES:
		values = np.stack([record[0][name] for record in records])
		result[name] = interp1d(hours, values, axis=0, bounds_error=True)(leads).astype("float32")
	for name in FLUX_NAMES:
		windows = [record[1][name] for record in records]
		starts, ends, values = separate_flux_intervals(
			[window[0] for window in windows], [window[1] for window in windows],
			np.stack([record[0][name] for record in records]))
		indices = np.searchsorted(ends, leads, side="right")
		if np.any(indices >= len(ends)) or np.any(starts[indices] > leads):
			raise ValueError(f"Incomplete hourly flux coverage: {name}")
		result[name] = values[indices].astype("float32")
	return leads, result, lat, lon


def write_sflux(output_path, initialization, valid_date, fields, lat, lon,
				lat_bounds=LAT_BOUNDS, lon_bounds=LON_BOUNDS, source="nomads"):
	"""Write one complete Pacific day using the RRFS/hrrr3 sflux schema.

	Parameters
	----------
	output_path : path-like
		NetCDF destination, replaced atomically only after a successful write.
	initialization : datetime
		UTC forecast initialization, saved as a global attribute.
	valid_date : datetime.date
		Pacific calendar date; contains 23, 24, or 25 hourly records.
	fields : dict
		Eight float arrays of shape (time, ny_grid, nx_grid).
	lat, lon : array_like
		Two-dimensional native GFS coordinates in signed degrees.
	lat_bounds, lon_bounds : pair of float, optional
		Requested geographic subset bounds.
	source : {"nomads", "s3"}, optional
		Download source recorded in the global attributes.

	Returns
	-------
	pathlib.Path
		Written NetCDF file.
	"""
	import xarray as xr

	midnight = PACIFIC.localize(datetime.combine(valid_date, time.min))
	next_midnight = PACIFIC.localize(datetime.combine(valid_date + timedelta(days=1), time.min))
	count = int((next_midnight.astimezone(timezone.utc) - midnight.astimezone(timezone.utc)).total_seconds() / 3600)
	if set(fields) != set(FIELD_NAMES) or lat.shape != lon.shape or lat.ndim != 2:
		raise ValueError("Need eight sflux fields and matching 2-D coordinates")
	for name, values in fields.items():
		if values.shape != (count,) + lat.shape or not np.isfinite(values).all():
			raise ValueError(f"Invalid daily shape/values for {name}")
	dimensions = ("time", "ny_grid", "nx_grid")
	offset = midnight.strftime("%z")
	offset = offset[:3] + ":" + offset[3:]
	dataset = xr.Dataset(
		{name: (dimensions, np.asarray(fields[name], dtype="float32")) for name in FIELD_NAMES},
		coords={"time": np.round(np.arange(count, dtype="float32") / 24, 6),
				"lat": (("ny_grid", "nx_grid"), lat.astype("float32")),
				"lon": (("ny_grid", "nx_grid"), lon.astype("float32"))},
	)
	dataset.time.attrs = {
		"long_name": "Time", "standard_name": "time",
		"base_date": [valid_date.year, valid_date.month, valid_date.day, 0, 0],
		"units": f"days since {valid_date:%Y-%m-%d} 00:00:00 {offset}",
		"timezone": "America/Los_Angeles",
	}
	dataset.lat.attrs = {"units": "degrees_north", "long_name": "Latitude", "standard_name": "latitude"}
	dataset.lon.attrs = {"units": "degrees_east", "long_name": "Longitude", "standard_name": "longitude"}
	for name in FIELD_NAMES:
		dataset[name].attrs = dict(FIELD_ATTRIBUTES[name])
		if name in FLUX_NAMES:
			dataset[name].attrs.update({"cell_methods": "time: mean",
									   "time_interval": "Mean over the hour starting at time"})
	dataset.attrs = {
		"creator_institution": "NOAA",
		"publisher_institution": "California Department of Water Resources",
		"program": "GFS", "title": "Subsetted forecast atmospheric data for Bay-Delta SCHISM",
		"time_of_preparation": datetime.now().strftime("%Y%m%d"),
		"forecast_start_time_utc": initialization.strftime("%Y-%m-%dT%H:%M:%SZ"),
		"valid_date_pacific": valid_date.isoformat(), "valid_timezone": "America/Los_Angeles",
		"geospatial_lat_min": lat_bounds[0], "geospatial_lat_max": lat_bounds[1],
		"geospatial_lon_min": lon_bounds[0], "geospatial_lon_max": lon_bounds[1],
		"source": SOURCE_DESCRIPTIONS[source], "download_source": source,
		"temporal_processing": "Hourly states: linear interpolation; fluxes: de-averaged interval means, held constant within intervals; negative packing residuals clipped to zero",
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
	logging.info("Saved %s (%d hourly records)", output_path, count)
	return output_path


def process_gfs(initialization, hours, records, output_dir, lat_bounds=LAT_BOUNDS,
				lon_bounds=LON_BOUNDS, overwrite=False, source="nomads"):
	"""Convert a run to complete Pacific-day files, without accessing NOAA.

	Files are OUTPUT/YYYY/gfs_YYYYMMDDHH.nc, where YYYYMMDD is the valid
	Pacific date and HH is the UTC cycle. Existing outputs are reused only if
	their initialization, bounding box, source, and schema match this request.
	The optional source ("nomads" or "s3") is stored in the NetCDF metadata.
	"""
	import xarray as xr

	if source not in SOURCE_DESCRIPTIONS:
		raise ValueError("Source must be nomads or s3")
	leads, fields, lat, lon = hourly_fields(hours, records)
	days = complete_sflux_days(initialization, leads[0], leads[-1] + 1)
	if not days:
		raise ValueError("Forecast coverage contains no complete Pacific days")
	paths = []
	for valid_date, day_leads in days.items():
		path = Path(output_dir) / f"{valid_date:%Y}" / f"gfs_{valid_date:%Y%m%d}{initialization:%H}.nc"
		expected_attributes = {
			"forecast_start_time_utc": initialization.strftime("%Y-%m-%dT%H:%M:%SZ"),
			"geospatial_lat_min": lat_bounds[0], "geospatial_lat_max": lat_bounds[1],
			"geospatial_lon_min": lon_bounds[0], "geospatial_lon_max": lon_bounds[1],
		}
		if path.exists() and not overwrite:
			with xr.open_dataset(path, decode_times=False) as existing:
				matching = (all(existing.attrs.get(key) == value for key, value in expected_attributes.items())
							and existing.attrs.get("download_source", "nomads") == source
							and set(FIELD_NAMES).issubset(existing.data_vars)
							and existing.sizes.get("time") == len(day_leads))
			if not matching:
				raise ValueError(f"Existing output belongs to another run/box, source, or is incomplete; use --overwrite: {path}")
			logging.info("Already exists: %s", path)
		else:
			indices = np.asarray(day_leads) - leads[0]
			write_sflux(path, initialization, valid_date,
						{name: values[indices] for name, values in fields.items()},
						lat, lon, lat_bounds, lon_bounds, source=source)
		paths.append(path)
	return paths


def _date_as_date(ctx, param, value):
	return value.date() if value is not None else None


def _validate_bounds(ctx, param, value):
	try:
		validate_bounds(value if param.name == "lat_bounds" else LAT_BOUNDS,
						value if param.name == "lon_bounds" else LON_BOUNDS)
	except ValueError as error:
		raise click.BadParameter(str(error)) from error
	return value


def _validate_cycles(ctx, param, value):
	invalid = sorted(set(value) - {0, 6, 12, 18})
	if invalid:
		raise click.BadParameter(f"must be selected from 0, 6, 12, 18; got {invalid}")
	return value


def _validate_max_hour(ctx, param, value):
	try:
		forecast_hours(value)
	except ValueError as error:
		raise click.BadParameter(str(error)) from error
	return value


@click.command(
	context_settings={"help_option_names": ["-h", "--help"]},
	help="Download NOAA GFS forecasts and write daily Pacific SCHISM sflux files.",
	epilog="""\
Examples:
  python download_gfs.py -d 2026-10-19 -c 0 -o ./data
  python download_gfs.py -d 2026-07-23 --source s3 -o ./data
  python download_gfs.py -sdate 2026-10-19 -edate 2026-10-20 -c 0 -c 12

The default 384-hour run covers at least 15 complete Pacific days.
NOMADS retains only recent runs; S3 provides archived runs where available.
S3 transfers global forcing fields, then crops locally (larger downloads).
Cached subsets support --process-only; use the same --source as the cache.""",
)
@click.option("-d", "--date", type=click.DateTime(formats=["%Y-%m-%d"]), callback=_date_as_date, help="UTC run date, YYYY-MM-DD")
@click.option("-sdate", "--start", type=click.DateTime(formats=["%Y-%m-%d"]), callback=_date_as_date, help="First UTC run date (inclusive)")
@click.option("-edate", "--end", type=click.DateTime(formats=["%Y-%m-%d"]), callback=_date_as_date, help="Last UTC run date (inclusive)")
@click.option(
	"-c", "--cycles", "cycles", multiple=True, type=int, default=(0,),
	callback=_validate_cycles,
	help="UTC initialization cycle: 0, 6, 12, or 18. Repeat to select multiple (default: 0).",
)
@click.option("--max-hour", type=int, default=384, show_default=True, callback=_validate_max_hour, help="Last native forecast lead")
@click.option("--source", type=click.Choice(["nomads", "s3"]), default="nomads", show_default=True, help="Download service; NOMADS is for recent runs, S3 for archived runs")
@click.option("--lat-bounds", type=float, nargs=2, default=LAT_BOUNDS, metavar="SOUTH NORTH", callback=_validate_bounds, help="Latitude bounds")
@click.option("--lon-bounds", type=float, nargs=2, default=LON_BOUNDS, metavar="WEST EAST", callback=_validate_bounds, help="Longitude bounds")
@click.option("-o", "--output-dir", type=click.Path(path_type=Path), default=Path("./data"), show_default=True, help="Output root")
@click.option("--process-only", is_flag=True, help="Process cached GRIB subsets without network access")
@click.option("--dry-run", is_flag=True, help="Show forecast leads and Pacific days without downloading")
@click.option("--overwrite", is_flag=True, help="Replace cached GRIB and daily NetCDF files")
@click.option("--debug", is_flag=True, help="Enable debug logging and tracebacks")
def main(date, start, end, cycles, max_hour, source, lat_bounds, lon_bounds,
		 output_dir, process_only, dry_run, overwrite, debug):
	"""Download one or more GFS runs and write daily SCHISM atmospheric forcing."""
	if date is None and start is None:
		raise click.UsageError("Provide --date or --start/--end")
	if (start is None) != (end is None):
		raise click.UsageError("Use --start and --end together, or --date alone")
	if date is not None and (start is not None or end is not None):
		raise click.UsageError("Use --date or --start/--end, not both")
	start_date = date or start
	end_date = date or end
	if start_date > end_date:
		raise click.UsageError("Start date must not follow end date")
	if process_only and dry_run:
		raise click.UsageError("--process-only cannot be combined with --dry-run")

	logging.basicConfig(level=logging.DEBUG if debug else logging.INFO,
						format="%(asctime)s %(levelname)s %(message)s")
	try:
		hours = forecast_hours(max_hour)
		valid_run_date = start_date
		while valid_run_date <= end_date:
			for cycle in sorted(set(cycles)):
				initialization = datetime.combine(valid_run_date, time(cycle), tzinfo=timezone.utc)
				days = complete_sflux_days(initialization, hours[0], hours[-1])
				if not days:
					raise ValueError("Requested forecast has no complete Pacific day; increase --max-hour")
				logging.info("%s %s: %d native files, f%03d..f%03d, %d complete Pacific days",
							 source.upper(), initialization.isoformat(), len(hours), hours[0], hours[-1], len(days))
				for day, day_hours in days.items():
					logging.info("  %s: %d hourly records", day, len(day_hours))
				if not dry_run:
					records = download_forecast(initialization, hours, output_dir / "grib",
												lat_bounds, lon_bounds,
												overwrite, process_only, source=source)
					process_gfs(initialization, hours, records, output_dir,
								lat_bounds, lon_bounds, overwrite, source=source)
			valid_run_date += timedelta(days=1)
	except Exception as error:
		logging.error("GFS download/processing failed: %s", error, exc_info=debug)
		raise click.ClickException(str(error)) from error


if __name__ == "__main__":
	main()
