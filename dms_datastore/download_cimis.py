"""Module for CIMIS data

* Downloads data from FTP site. Each year is a separate file for hourly data
* Downloads data for current year. Each month is a separate file for hourly data
* Loads data for a station_number from data that is downloaded
* Map to show the stations
"""

import os
import glob
import os
import datetime
import dateutil
import tqdm

import pandas as pd
import dask.dataframe

import logging
import click
from functools import lru_cache

logging.basicConfig(level=logging.ERROR)
VARTYPES = [
    "Reference ETo   (in)   (mm)",
    "Precipitation   (in)   (mm)",
    "Solar Radiation   (Ly/day)   (W/m²)",
    "Vapor Pressure   (mBars)   (kPa)",
    "Air Temperature   (°F)   (°C)",
    "Relative Humidity   (%)",
    "Dew Point   (°F)   (°C)",
    "Wind Speed   (mph)   (m/s)",
    "Wind Direction   (0-360)",
    "Soil Temperature   (°F)   (°C)",
]

import paramiko


class CIMIS:
    def __init__(self, base_dir=".", password="xxx"):
        self.set_base_dir(base_dir)
        # Create an SSH client
        ssh = paramiko.SSHClient()
        # Load system SSH keys
        # ssh.load_system_host_keys()
        # Add the server's SSH key automatically if missing
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        # Connect to the server
        ssh.connect(
            hostname="sftpcimis.water.ca.gov",
            username="sftpcimis",
            password=password,
        )  #
        # Open an SFTP session
        self.ssh = ssh
        self.sftp = ssh.open_sftp()

    def close(self):
        self.ssh.close()

    def set_base_dir(self, base_dir):
        """
        Use this to set the base directory under which downloaded and cached data for CIMIS will reside locally.add()
        The default is ".", i.e. whereever this script is running

        :param base_dir: base directory under which downloaded and cached resides
        :type base_dir: str, default "."
        """
        self.CIMIS_BASE_DIR = base_dir
        self.CIMIS_DOWNLOAD_DIR = os.path.join(self.CIMIS_BASE_DIR, "cimis_downloaded")
        self.CIMIS_CACHE_DIR = os.path.join(self.CIMIS_BASE_DIR, "cimis_cache")

    def ensure_dir(self, dir):
        """
        ensures directory exists otherwise creates it.

        :param dir: directory
        :type dir: str
        """
        if not os.path.exists(dir):
            os.mkdir(dir)

    def parse_year_from_string(self, name):
        import re

        re.compile(".*(\\d+).*")
        matches = re.match("\\D*(\\d+).*", name)
        return matches.groups()[0]

    def unzip(self, file, dir):
        import zipfile

        lzip = zipfile.ZipFile(file, mode="r")
        lzip.extractall(dir)

    def download(self, remotefile, dir=None):
        """
        download a file from the CIMIS FTP site to CIMIS_DOWNLOAD_DIR and returns the local file path

        :param remotefile: remote file path
        :type remotefile: str
        :return: local file path
        """
        if dir is None:
            dir = self.CIMIS_DOWNLOAD_DIR
        localfile = os.path.join(dir, str.split(remotefile, "/")[-1])
        self.ensure_dir(os.path.dirname(localfile))
        try:
            self.sftp.get(remotefile, localfile)
        except Exception as ex:
            logging.error(f"Error downloading {remotefile}: {ex}")
            raise ex
        return localfile

    def download_zipped(self, year, hourly=True):
        """
        download daily data using the FTP site : 'ftp://ftpcimis.water.ca.gov/pub2/annual/dailyStns{year}.zip

        :param year: year to download, e.g. 2020
        :type year: int
        :param base_dir: base directory to download file into
        :type base_dir: str
        :return: full path to file downloaded into
        :rtype: str
        """
        if hourly:
            interval = "hourly"
        else:
            interval = "daily"
        try:
            lfile = self.download(
                f"/pub2/annual/{interval}Stns{year}.zip",
                dir=os.path.join(self.CIMIS_DOWNLOAD_DIR, str(year)),
            )
            self.unzip(
                lfile,
                os.path.join(self.CIMIS_DOWNLOAD_DIR, str(year)),
            )
        except Exception as ex:
            logging.warning(f"Error downloading {interval} station {year}: {ex}")

    def download_unzipped(self, year, stations, hourly=True):
        """
        download daily data using the FTP site : 'ftp://ftpcimis.water.ca.gov/pub2/annual/{year}daily{station}.csv
        """
        if hourly:
            interval = "hourly"
        else:
            interval = "daily"
        for station in stations:
            try:
                self.download(
                    f"/pub2/annual/{year}{interval}{station:03d}.csv",
                    dir=os.path.join(self.CIMIS_DOWNLOAD_DIR, str(year)),
                )
            except Exception as ex:
                logging.warning(f"Error downloading {interval} station {station}: {ex}")

    @lru_cache(maxsize=128)
    def get_columns_for_year(self, y, hourly=True):
        if y >= 2014:
            units_file = self.download("/pub2/readme-ftp-Revised5units.txt")
            if hourly:
                skiprows = 96
                nrows = 24
            else:  # for daily
                skiprows = 60
                nrows = 31
            df = pd.read_csv(
                units_file,
                skiprows=skiprows,
                nrows=nrows,
                encoding="cp1252",
            )
            df2 = df.iloc[:, 0].str.split("\t", n=1, expand=True)
        else:
            units_file = self.download("pub2/readme (prior to June 2014)units.txt")
            if hourly:
                skiprows = 86
                nrows = 24
            else:  # for daily
                skiprows = 50
                nrows = 31
            df = pd.read_csv(
                units_file,
                skiprows=skiprows,
                nrows=nrows,
                encoding="cp1252",
            )
            df2 = df.iloc[:, 0].str.split("\\s+\\d+\\.\\s+", n=1, expand=True)
        return df2.iloc[:, 1].str.strip().values

    def get_months_and_current_year(self):
        # Get the current month
        current_month = datetime.datetime.now().month
        current_year = datetime.datetime.now().year
        # List of all months in 3-letter lowercase codes
        months_so_far = [
            datetime.datetime(current_year, i, 1).strftime("%b").lower()
            for i in range(1, current_month + 1)
        ]
        # get current year and ensure directory
        current_year = datetime.date.today().year
        return months_so_far, current_year

    def download_current_year(self, hourly=True):
        if hourly:
            interval = "hourly"
        else:
            interval = "daily"
        month_list, current_year = self.get_months_and_current_year()
        self.ensure_dir(os.path.join(self.CIMIS_DOWNLOAD_DIR, str(current_year)))
        for month in month_list[:-1]:
            try:
                lfile = self.download(
                    f"/pub2/monthly/{interval}Stns{month}.zip",
                    dir=os.path.join(self.CIMIS_DOWNLOAD_DIR, str(current_year)),
                )
                self.unzip(
                    lfile, os.path.join(self.CIMIS_DOWNLOAD_DIR, str(current_year))
                )
            except Exception as ex:
                logging.warning(f"Error downloading station {month}: {ex}")
                raise ex

    def download_current_month(self, stations, hourly=True):
        if hourly:
            interval = "hourly"
        else:
            interval = "daily"
        month_list, current_year = self.get_months_and_current_year()
        self.ensure_dir(os.path.join(self.CIMIS_DOWNLOAD_DIR, str(current_year)))
        for station in stations:
            try:
                self.download(
                    f"/pub2/{interval}/{interval}{station:03d}.csv",
                    dir=os.path.join(self.CIMIS_DOWNLOAD_DIR, str(current_year)),
                )
            except Exception as ex:
                logging.warning(f"Error downloading station {station}: {ex}")

    @lru_cache(maxsize=128)
    def get_stations_info(
        self,
        file="/pub2/CIMIS Stations List (January20).xlsx",
    ):
        localfile = self.download(file)
        dfstations = pd.read_excel(localfile)
        dfstations = dfstations.dropna()
        return dfstations.astype(
            {"Station Number": "int", "Connect": "str", "Disconnect": "str"}
        )

    def to_datetime(self, str):
        try:
            return pd.to_datetime(str)
        except ValueError as ve:
            if ve.args[0].startswith("hour must be in 0..23"):
                return pd.to_datetime(str.replace("2400", "2300")) + pd.Timedelta(
                    1, "h"
                )

    def guess_dtypes(self, cols):
        dtypes = {c: "int" if c.find("Hour") >= 0 else "str" for c in cols[0:3]}
        dtypes_odd = {c: "str" if c.find("QC") >= 0 else "float64" for c in cols[3:]}
        return {**dtypes, **dtypes_odd}

    def load_station(self, station_number, load_current_year=True, hourly=True):
        if hourly:
            interval = "hourly"
        else:
            interval = "daily"

        before_2014 = [
            "%s/%d/*%s%03d.csv"
            % (self.CIMIS_DOWNLOAD_DIR, year, interval, station_number)
            for year in range(1982, 2014)
        ]
        # load before 2014
        colsb = self.get_columns_for_year(2013, hourly)
        try:
            ddfb = dask.dataframe.read_csv(
                before_2014,
                header=None,
                names=colsb,
                na_values=["--", "       --", "  #######", "    #####"],
                dtype=self.guess_dtypes(colsb),
            )
            dfb = ddfb.compute()
        except Exception as err:
            logging.warning(
                f"Station Number {station_number}: No data found before 2014"
            )
            dfb = None
        # load after 2014
        _, current_year = self.get_months_and_current_year()
        after_2014 = [
            "%s/%d/*%s%03d.csv"
            % (self.CIMIS_DOWNLOAD_DIR, year, interval, station_number)
            for year in range(2014, current_year)
        ]
        colsa = self.get_columns_for_year(2014, hourly)
        try:
            ddfa = dask.dataframe.read_csv(
                after_2014,
                header=None,
                names=colsa,
                na_values=["--", "       --", "  #######", "    #####"],
                dtype=self.guess_dtypes(colsa),
            )
            dfa = ddfa.compute()
        except Exception as err:
            logging.warning(
                f"Station Number {station_number}: No data found after 2014"
            )
            dfa = None
        # concat
        df = pd.concat([df for df in [dfb, dfa] if df is not None])
        df = self._with_timeindex(df, hourly)
        if load_current_year:
            try:
                dfc = self.load_station_for_current_year(
                    station_number, hourly, self.CIMIS_DOWNLOAD_DIR
                )
            except Exception as ex:
                # add logging here to capture exception
                logging.warning(ex)
                dfc = None
            if dfc is not None:
                df = pd.concat([df, dfc])
            try:
                dfd = self.load_station_for_current_month(
                    station_number, hourly, self.CIMIS_DOWNLOAD_DIR
                )
            except Exception as ex:
                # add logging here to capture exception
                logging.warning(ex)
                dfd = None
            if dfd is not None:
                df = pd.concat([df, dfd])
        return df

    def _with_timeindex(self, df, hourly):
        if hourly:
            df["time"] = df["Date"] + df["Hour   (PST)"].map(" {:04d}".format)
            # make time index
            pat = "(?P<month>\d+)/(?P<day>\d+)/(?P<year>\d+) (?P<hour>\d{2})(?P<minute>\d{2})"
            dft = df["time"].str.extract(pat, expand=True)
            dft.columns = ["month", "day", "year", "h", "m"]
            dft = dft[["year", "month", "day", "h", "m"]]
            df.index = pd.to_datetime(dft, "coerce")
        else:
            df.index = pd.to_datetime(df["Date"], "coerce")
        return df

    def get_vartypes(self, df):
        """return list of columns of variable types"""
        return list(df.columns[df.columns.str.match("(?!^QC)")])[4:]

    def get_vartype(self, vartype, df):
        return df.loc[:, vartype]

    def load_station_for_current_year(self, station_number, hourly, dir=None):
        if hourly:
            interval = "hourly"
        else:
            interval = "daily"
        if dir is None:
            dir = self.CIMIS_DOWNLOAD_DIR
        months, current_year = self.get_months_and_current_year()
        files_for_year = [
            f"{dir}/{current_year}/{month}{interval}{station_number:03d}.csv"
            for month in months[:-1]
        ]
        # files_for_year = [f"{dir}/{current_year}/{interval}{station_number:03d}.csv"]
        cols = self.get_columns_for_year(current_year, hourly)
        ddfa = dask.dataframe.read_csv(
            files_for_year,
            header=None,
            names=cols,
            na_values=["--", "       --", "  #######", "    #####"],
            dtype=self.guess_dtypes(cols),
        )
        dfa = ddfa.compute()
        return self._with_timeindex(dfa, hourly).sort_index()

    def load_station_for_current_month(self, station_number, hourly, dir=None):
        if hourly:
            interval = "hourly"
        else:
            interval = "daily"
        if dir is None:
            dir = self.CIMIS_DOWNLOAD_DIR
        months, current_year = self.get_months_and_current_year()
        current_month = months[-1]
        files_for_month = [f"{dir}/{current_year}/{interval}{station_number:03d}.csv"]
        cols = self.get_columns_for_year(current_year, hourly)
        ddfa = dask.dataframe.read_csv(
            files_for_month,
            header=None,
            names=cols,
            na_values=["--", "       --", "  #######", "    #####"],
            dtype=self.guess_dtypes(cols),
        )
        dfa = ddfa.compute()
        return self._with_timeindex(dfa, hourly).sort_index()

    def cache_stations(self, dfstations):
        import os

        self.ensure_dir(self.CIMIS_CACHE_DIR)
        failed_stations = []
        for station_number in dfstations["Station Number"]:
            try:
                self.cache_station(station_number)
            except:
                logging.error(f"Failing for station_number: {station_number}")
                failed_stations.append(station_number)
        return failed_stations

    def expired(self, mtime, expires="1D"):
        return pd.Timestamp.now() - pd.Timestamp.fromtimestamp(mtime) > pd.to_timedelta(
            expires
        )

    def needs_creation(self, fname):
        return not os.path.exists(fname)

    def needs_updating(self, fname):
        return self.expired(os.path.getmtime(fname))

    def cache_station(self, station_number):
        self.ensure_dir(self.CIMIS_CACHE_DIR)
        fname = os.path.join(self.CIMIS_CACHE_DIR, f"cimis_{station_number}.pkl")
        if self.needs_creation(fname) or self.needs_updating(fname):
            df = self.load_station(station_number)
            pd.to_pickle(df, fname)

    def load_station_from_cache(self, station_number):
        fname = os.path.join(self.CIMIS_CACHE_DIR, f"cimis_{station_number}.pkl")
        if self.needs_creation(fname) or self.needs_updating(fname):
            self.cache_station(station_number)
        return pd.read_pickle(fname)

    def cache_to_pkl(self, dfstations):
        failed_stations = self.cache_stations(dfstations)
        if len(failed_stations) > 0:
            print("Could not cache the following stations: ")
            print(dfstations[dfstations["Station Number"].isin(failed_stations)])


def download_all_data(hourly=True, partial=False):
    """
    Download all CIMIS data from the FTP site. Each year is a separate file for hourly data
    Each month is a separate file for hourly data

    :param hourly: download hourly data (default is True)
    :param partial: download only partial data (default is False) (only downloads last couple of years)
    """
    password = os.environ.get("CIMIS_PASSWORD", default="xxx")
    cx = CIMIS(password=password)
    if hourly:
        interval = "hourly"
    else:
        interval = "daily"
    dfcat = cx.get_stations_info()
    dfcat["Connect"] = pd.to_datetime(dfcat["Connect"])
    min_year = dfcat["Connect"].dt.year.min()
    dfcat.to_csv("cimis_stations.csv", index="Station Number")
    current_year = pd.to_datetime("today").year
    active_stations = list(dfcat[dfcat["Status"] == "Active"]["Station Number"])

    if not partial:
        for year in range(min_year, current_year - 2):
            print(f"Downloading zipped {interval} data for year", year)
            cx.download_zipped(year, hourly)

    for year in range(current_year - 2, current_year):
        print(f"Downloading unzipped {interval} data for year", year)
        cx.download_unzipped(year, active_stations, hourly)

    cx.download_current_year(hourly)
    cx.download_current_month(active_stations, hourly)

    for station in tqdm.tqdm(dfcat["Station Number"], total=len(dfcat)):
        try:
            dfs = cx.load_station(station, True, hourly)
            dfs.to_csv(f"cimis_{interval}_{station:03d}.csv", index="Date")
        except Exception as e:
            logging.error(f"Error loading station {station}: {e}")
            continue


def merge_with_existing(existing_dir, new_dir, hourly=True):
    if hourly:
        interval = "hourly"
    else:
        interval = "daily"

    new_files = glob.glob(os.path.join(new_dir, f"cimis_{interval}_*.csv"))
    for file in tqdm.tqdm(new_files):
        # get name of file
        file_name = os.path.basename(file)
        # get file in existing directory of same name
        existing_file = os.path.join(existing_dir, file_name)
        # if file exists in existing directory, merge with new file
        dfn = pd.read_csv(file, index_col=0, parse_dates=True)
        if os.path.exists(existing_file):
            dfe = pd.read_csv(existing_file, index_col=0, parse_dates=True)
            # Combine the two DataFrames and remove duplicates
            combined = pd.concat([dfe, dfn]).drop_duplicates(
                keep="last"
            )  # Keeps the last occurrence
            combined.to_csv(existing_file)
        else:
            logging.warning(f"File {existing_file} does not exist so writing new file")
            dfn.to_csv(existing_file)

def download_cimis(hourly, existing_dir, download, partial):
    """
    Download CIMIS data
    
    Environment variable CIMIS_PASSWORD must be set to the password for the CIMIS FTP site
    """
    if download:
        download_all_data(hourly=hourly, partial=partial)
    if existing_dir is not None:
        merge_with_existing(existing_dir, ".", hourly=hourly)

@click.command()
@click.option(
    "--hourly", is_flag=True, default=True, help="Download hourly data (default is True)"
)
@click.option("--existing-dir", default=None, help="Directory to merge new data into")
@click.option(
    "--download",
    is_flag=True,
    default=True,
    help="Download data (default is True)",
)
@click.option(
    "--partial",
    is_flag=True,
    default=False,
    help="Set partial download to True if provided (default is False)",
)
def download_cimis_cli(hourly, existing_dir, download, partial):
    """
    DCLI for downloading CIMIS data
    """
    
    download_cimis(hourly, existing_dir, download, partial)


if __name__ == "__main__":
    download_cimis_cli()
