#!/usr/bin/env python
""" Script to download NOAA water level data
"""
import sys  # noqa
import requests
import bs4
import calendar
import datetime as dtm
import re
import os
import concurrent.futures
from dms_datastore.process_station_variable import (
    process_station_list,
    stationfile_or_stations,
)
from dms_datastore import dstore_config
from .logging_config import logger

__all__ = ["noaa_download"]

default_stationlist = {
    "9414290": "San Francisco",
    "9414750": "Alameda",
    "9414523": "Redwood City",
    "9414575": "Coyote Creek",
    "9414863": "Richmond",
    "9415144": "Port Chicago",
    "9413450": "Monterey",
    "9415020": "Point Reyes",
    "9415102": "Martinez-Amorco Pier",
}


name_to_id = dict((default_stationlist[k], k) for k in default_stationlist)


def retrieve_csv(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.content


def retrieve_table(url):
    done = False
    while not done:
        try:
            soup = bs4.BeautifulSoup(requests.get(url))
        except requests.exceptions.RequestException as e:
            logger.info((f"Request error: {e}"))
            logger.info("Failed to retrieve {}".format(url))
            logger.info("Try again...")

    table = soup.table.pre.pre.string
    return table


def write_table(table, fname, first):
    f = open(fname, "a")
    # Remove the Error line
    table = table[: table.find("Error")]
    if table[-1] != "\n":
        table += "\n"
    if first:
        f.write(table)
    else:
        pos = table.find("\n")
        f.write(table[pos + 1 :])
    f.flush()
    f.close()


def write_header(fname, headers):
    f = open(fname, "w")
    for key in headers:
        value = headers[key]
        buf = f"# {key}: {value}\n"
        f.write(buf)
    f.flush()
    f.close()


def faulty_output(text):
    return "Please make sure" in text or len(text) < 300


def download_station_data(
    row, dest_dir, start, end, param, overwrite, endfile, skips, verbose
):
    agency_id = row.agency_id
    station = row.station_id
    param = row.src_var_id
    station_name = row["name"]
    paramname = row.param
    subloc = row.subloc

    product_info = {
        "water_level": {
            "agency": "noaa",
            "unit": "meters",
            "datum": "NAVD",
            "station_id": f"{agency_id}",
            "station_name": f"{station_name}",
            "param": "elev",
            "timezone": "LST",
            "source": "http://tidesandcurrents.noaa.gov/",
        },
        "predictions": {
            "agency": "noaa",
            "unit": "meters",
            "datum": "NAVD",
            "station_id": "{}".format(agency_id),
            "station_name": f"{station_name}",
            "param": "predictions",
            "timezone": "LST",
            "source": "http://tidesandcurrents.noaa.gov/",
        },
        "water_temperature": {
            "agency": "noaa",
            "unit": "Celcius",
            "station_id": "{}".format(agency_id),
            "station_id": f"{agency_id}",
            "station_name": f"{station_name}",
            "param": "temp",
            "timezone": "LST",
            "source": "http://tidesandcurrents.noaa.gov/",
        },
        "conductivity": {
            "agency": "noaa",
            "unit": "microS/cm",
            "station_id": "{}".format(agency_id),
            "station_name": f"{station_name}",
            "item": "ec",
            "timezone": "LST",
            "source": "http://tidesandcurrents.noaa.gov/",
        },
    }

    yearname = f"{start.year}_{endfile}"
    outfname = f"noaa_{station}_{agency_id}_{paramname}_{yearname}.csv"
    outfname = outfname.lower()
    path = os.path.join(dest_dir, outfname)
    if os.path.exists(path) and not overwrite:
        logger.info(
            f"Skipping existing station because file exists: {station} variable {param}"
        )
        skips.append(path)
        return

    if not param in product_info:
        raise ValueError("Product not supported: {}".format(param))
    first = True
    headers = product_info[param]
    headers["station_name"] = station_name
    app = (
        "NOS.COOPS.TAC.PHYSOCEAN"
        if param in ("conductivity", "temperature")
        else "NOS.COOPS.TAC.WL"
    )

    for year in range(start.year, end.year + 1):
        month_start = start.month if year == start.year else 1
        month_end = end.month if year == end.year else 12
        for month in range(month_start, month_end + 1):
            day_start = start.day if year == start.year and month == start.month else 1
            day_end = (
                end.day
                if year == end.year and month == end.month
                else calendar.monthrange(year, month)[1]
            )
            date_start = "{:4d}{:02d}{:02d}".format(year, month, day_start)
            date_end = "{:4d}{:02d}{:02d}".format(year, month, day_end)
            base_url = headers["source"]

            datum = "NAVD"
            datum_str = (
                f"&datum={datum}" if param in ("water_level", "predictions") else ""
            )
            url = f"https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?product={param}&application={app}&begin_date={date_start}&end_date={date_end}&station={agency_id}&time_zone=LST&units=metric{datum_str}&format=csv"

            # logger.info(f"Retrieving {url}\n station {agency_id} from {date_start} to {date_end}".format(url,agency_id, date_start, date_end))
            # logger.info("URL: {}".format(url))

            try:
                raw_table = retrieve_csv(url).decode()
                if faulty_output(raw_table):
                    if verbose:
                        logger.info(f"No good data produced for {station},{paramname}")
                    continue
            except:
                if verbose:
                    logger.info(
                        f"Error reported in retrieval for {station},{paramname}"
                    )
                raw_table = "\n"

            if raw_table[0] == "\n":
                datum = "STND"
                datum_str = (
                    f"&datum={datum}" if param in ("water_level", "predictions") else ""
                )
                url = f"https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?product={param}&application={app}&begin_date={date_start}&end_date={date_end}&station={agency_id}&time_zone=LST&units=metric&{datum_str}&format=csv"
                # logger.info("Retrieving Station {}, from {} to {}...".format(agency_id, date_start, date_end))
                # logger.info("URL: {}".format(url))
                try:
                    raw_table = retrieve_csv(url).decode()
                    if faulty_output(raw_table):
                        if verbose:
                            logger.info("No good data produced [2] for ")
                        continue
                except:
                    if verbose:
                        logger.info("Error in second retrieval")
                    continue
            if first:
                headers["datum"] = datum
                write_header(path, headers)
            write_table(raw_table, path, first)
            first = False


def noaa_download(stations, dest_dir, start, end=None, param=None, overwrite=False):
    """Download stage data from NOAA tidesandcurrents, and save it to
    NOAA CSV format.

    Parameters
    ----------
    station_id : int or str
        station id

    dest_dir : str
    Destination directory for download

    start : datetime.datetime
        first date to download

    end : datetime.datetime
        last date to download, inclusive of the date

    param : str
        production, it is either water_level or predictions.

    """
    verbose = False

    if end is None:
        end = dtm.datetime.now()
        endfile = 9999
    else:
        endfile = end.year
    if not os.path.exists(dest_dir):
        os.mkdir(dest_dir)
    skips = []

    # Use ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        # Schedule the download tasks and handle them asynchronously
        futures = []
        for ndx, row in stations.iterrows():
            future = executor.submit(
                download_station_data,
                row,
                dest_dir,
                start,
                end,
                param,
                overwrite,
                endfile,
                skips,
                verbose,
            )
            futures.append(future)

        # Optionally, handle the results of the tasks
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()  # This line can be used to handle results or exceptions from the tasks
            except Exception as e:
                logger.error(f"Exception occurred during download: {e}")


def create_arg_parser():
    """Create an ArgumentParser"""
    import argparse

    parser = argparse.ArgumentParser(
        description="""Script to download NOAA 6 minute water level data """
    )
    parser.add_argument(
        "--start", default=None, required=False, type=str, help="First date to download"
    )
    parser.add_argument(
        "--end",
        default=None,
        required=False,
        type=str,
        help="Last date to download, inclusive",
    )
    parser.add_argument(
        "--syear", default=None, required=False, type=int, help="First year to download"
    )
    parser.add_argument(
        "--eyear",
        default=None,
        required=False,
        type=int,
        help="Last year to download, inclusive to end" " of the year.",
    )
    parser.add_argument(
        "--param",
        default=None,
        required=False,
        type=str,
        help="Product to download: water_level, predictions, water_temperature, conductivity.",
    )

    parser.add_argument(
        "--stations",
        default=None,
        nargs="*",
        required=False,
        help="Id or name of one or more stations.",
    )
    parser.add_argument("stationfile", nargs="*", help="CSV-format station file.")

    parser.add_argument(
        "--dest",
        dest="dest_dir",
        default="noaa_download",
        help="Destination directory for downloaded files.",
    )
    parser.add_argument(
        "--list", default=False, action="store_true", help="List known station ids."
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing files (if False they will be skipped, presumably for speed",
    )
    return parser


def list_stations():
    """Show NOAA station ID's in our study area."""
    logger.info("Available stations:")
    for key in default_stationlist.keys():
        logger.info("{}: {}".format(key, default_stationlist[key]))


def assure_datetime(dtime, isend=False):
    if isinstance(dtime, dtm.datetime):
        return dtime
    elif isinstance(dtime, str):
        if len(dtime) > 4:
            return dtm.datetime(list(*map(int, re.split(r"[^\d]", dtime))))
        elif len(dtime) == 4:
            return (
                dtm.datetime(int(dtime), 12, 31, 23, 59)
                if isend
                else dtm.datetime(int(dtime), 1, 1)
            )
        else:
            raise ValueError("Could not coerce string to date: {}".format(dtime))
    elif isinstance(dtime, int):
        return (
            dtm.datetime(dtime, 12, 31, 23, 59) if isend else dtm.datetime(dtime, 1, 1)
        )


def main():
    """Main function"""
    parser = create_arg_parser()
    args = parser.parse_args()
    dest_dir = args.dest_dir
    overwrite = args.overwrite
    param = args.param
    if args.list:
        logger.info(
            "listing is deprecated. Try 'station_info noaa' to get a list of noaa stations"
        )
        return
    else:
        stationfile = stationfile_or_stations(args.stationfile, args.stations)
        slookup = dstore_config.config_file("station_dbase")
        vlookup = dstore_config.config_file("variable_mappings")
        df = process_station_list(
            stationfile,
            param=param,
            station_lookup=slookup,
            agency_id_col="agency_id",
            param_lookup=vlookup,
            source="noaa",
        )

        if args.start:
            start = dtm.datetime(*list(map(int, re.split(r"[^\d]", args.start))))
        else:
            start = None
        if args.end:
            end = dtm.datetime(*list(map(int, re.split(r"[^\d]", args.end))))
        else:
            end = None

        syear = args.syear
        eyear = args.eyear
        if syear or eyear:
            # from numpy import VisibleDeprecationWarning
            # raise VisibleDeprecationWarning("The syear and eyear arguments are deprecated. Please use start and end.")
            if syear and start:
                raise ValueError("syear and start are mutually exclusive")
            if eyear and end:
                raise ValueError("eyear and end are mutually exclusive")
            if syear:
                start = dtm.datetime(syear, 1, 1)
            if eyear:
                end = dtm.datetime(eyear + 1, 1, 1)

        if not start is None and not end is None:
            if start > end:
                raise ValueError(
                    "start {} is after end {}".format(
                        start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")
                    )
                )

        if args.stations:
            # stations explicitly input
            stage_stations = args.stations
        else:
            # stations in file
            stage_stations = []
            for line in args.stationfile:
                if not line.startswith("#") and len(line) > 1:
                    sid = line.strip().split()[0]
                    logger.info("station id={}".format(sid))
                    stage_stations.append(sid)

        return noaa_download(df, dest_dir, start, end, param, overwrite)


if __name__ == "__main__":
    main()
