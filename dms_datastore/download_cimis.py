"""Module for CIMIS data 

    * Downloads data from FTP site. Each year is a separate file for hourly data
    * Downloads data for current year. Each month is a separate file for hourly data
    * Loads data for a station_number from data that is downloaded
    * Map to show the stations
"""

import logging
import os
import datetime
import dateutil

import pandas as pd
import dask.dataframe

import logging

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
        self.sftp = ssh.open_sftp()

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
        self.sftp.get(remotefile, localfile)
        return localfile

    def download_hourly(self, year):
        """
        download hourly data using the FTP site : 'ftp://ftpcimis.water.ca.gov/pub2/annual/hourlyStns{year}.zip

        :param year: year to download, e.g. 2020
        :type year: int
        :param base_dir: base directory to download file into
        :type base_dir: str
        :return: full path to file downloaded into
        :rtype: str
        """
        return self.download("/pub2/annual/hourlyStns%d.zip" % year)

    def download_hourly_unzipped(self, year, stations):
        """
        download hourly data using the FTP site : 'ftp://ftpcimis.water.ca.gov/pub2/annual/{year}hourly{station}.zip

        :param year: year to download, e.g. 2020
        :type year: int
        :param base_dir: base directory to download file into
        :type base_dir: str
        :return: full path to file downloaded into
        :rtype: str
        """
        for station in stations:
            try:
                self.download(
                    f"/pub2/annual/{year}hourly{station:03d}.csv",
                    dir=os.path.join(self.CIMIS_DOWNLOAD_DIR, str(year)),
                )
            except Exception as ex:
                logging.warning(f"Error downloading station {station}: {ex}")

    def parse_year_from_string(self, name):
        import re

        re.compile(".*(\\d+).*")
        matches = re.match("\\D*(\\d+).*", name)
        return matches.groups()[0]

    def unzip(self, file, dir):
        import zipfile

        lzip = zipfile.ZipFile(file, mode="r")
        lzip.extractall(dir)

    def get_columns_for_year(self, y):
        if y >= 2014:
            units_file = self.download("/pub2/readme-ftp-Revised5units.txt")
            df = pd.read_csv(
                units_file,
                skiprows=96,
                nrows=24,
                encoding="cp1252",
            )
            df2 = df.iloc[:, 0].str.split("\t", n=1, expand=True)
        else:
            units_file = self.download("pub2/readme (prior to June 2014)units.txt")
            df = pd.read_csv(
                units_file,
                skiprows=86,
                nrows=24,
                encoding="cp1252",
            )
            df2 = df.iloc[:, 0].str.split("\\s+\\d+\\.\\s+", n=1, expand=True)
        return df2.iloc[:, 1].str.strip().values

    def download_all(self, start, end):
        import tqdm

        self.ensure_dir(self.CIMIS_DOWNLOAD_DIR)
        for year in tqdm.tqdm(range(start, end)):
            lfile = self.download_hourly(year)
            self.unzip(
                lfile,
                os.path.join(
                    self.CIMIS_DOWNLOAD_DIR, self.parse_year_from_string(lfile)
                ),
            )

    def get_months_and_current_year(self):
        # Get the current month
        current_month = datetime.datetime.now().month

        # List of all months in 3-letter lowercase codes
        months_so_far = [
            datetime.datetime(2024, i, 1).strftime("%b").lower()
            for i in range(1, current_month + 1)
        ]
        # get current year and ensure directory
        current_year = datetime.date.today().year
        return months_so_far, current_year

    def download_current_year(self):
        month_list, current_year = self.get_months_and_current_year()
        self.ensure_dir(os.path.join(self.CIMIS_DOWNLOAD_DIR, str(current_year)))
        # urls for downloading to files
        urls = ["/pub2/monthly/hourlyStns%s.zip" % mon for mon in month_list]
        for urlpath in urls:
            self.download(urlpath)
        # unzip the downloaded files to the current year
        for lfile in [
            self.CIMIS_DOWNLOAD_DIR + "/hourlyStns%s.zip" % mon for mon in month_list
        ]:
            self.unzip(lfile, self.CIMIS_DOWNLOAD_DIR + "/%d" % (current_year))

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

    def load_station(self, station_number, load_current_year=True):

        before_2014 = [
            "%s/%d/*hourly%03d.csv" % (self.CIMIS_DOWNLOAD_DIR, year, station_number)
            for year in range(1982, 2014)
        ]
        # load before 2014
        colsb = self.get_columns_for_year(2013)
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
            "%s/%d/*hourly%03d.csv" % (self.CIMIS_DOWNLOAD_DIR, year, station_number)
            for year in range(2014, current_year)
        ]
        colsa = self.get_columns_for_year(2014)
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
        df = self._with_timeindex(df)
        if load_current_year:
            try:
                dfc = self.load_station_for_current_year(
                    station_number, self.CIMIS_DOWNLOAD_DIR
                )
            except Exception as ex:
                # add logging here to capture exception
                logging.warning(ex)
                dfc = None
            if dfc is not None:
                df = pd.concat([df, dfc])
        return df

    def _with_timeindex(self, df):
        df["time"] = df["Date"] + df["Hour   (PST)"].map(" {:04d}".format)
        # make time index
        pat = (
            "(?P<month>\d+)/(?P<day>\d+)/(?P<year>\d+) (?P<hour>\d{2})(?P<minute>\d{2})"
        )
        dft = df["time"].str.extract(pat, expand=True)
        dft.columns = ["month", "day", "year", "h", "m"]
        dft = dft[["year", "month", "day", "h", "m"]]
        df.index = pd.to_datetime(dft, "coerce")
        return df

    def get_vartypes(self, df):
        """return list of columns of variable types"""
        return list(df.columns[df.columns.str.match("(?!^QC)")])[4:]

    def get_vartype(self, vartype, df):
        return df.loc[:, vartype]

    def load_station_for_current_year(self, station_number, dir=None):
        if dir is None:
            dir = self.CIMIS_DOWNLOAD_DIR
        month_list, current_year = self.get_months_and_current_year()
        files_for_year = [
            "%s/%d/%shourly%03d.csv" % (dir, current_year, month, station_number)
            for month in month_list
        ]
        cols = self.get_columns_for_year(2014)  # format of years >= 2014
        ddfa = dask.dataframe.read_csv(
            files_for_year,
            header=None,
            names=cols,
            na_values=["--", "       --", "  #######", "    #####"],
            dtype=self.guess_dtypes(cols),
        )
        dfa = ddfa.compute()
        return self._with_timeindex(dfa).sort_index()

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


def main():
    password = os.environ.get("CIMIS_PASSWORD", default="xxx")
    cx = CIMIS(password=password)
    dfcat = cx.get_stations_info()
    dfcat["Connect"] = pd.to_datetime(dfcat["Connect"])
    min_year = dfcat["Connect"].dt.year.min()
    dfcat.to_csv("cimis_stations.csv", index="Station Number")
    # %%
    current_year = pd.to_datetime("today").year
    cx.download_all(min_year, current_year - 2)
    active_stations = list(dfcat[dfcat["Status"] == "Active"]["Station Number"])
    cx.download_hourly_unzipped(current_year - 1, active_stations)
    cx.download_current_year()
    #
    import tqdm

    for station in tqdm.tqdm(dfcat["Station Number"], total=len(dfcat)):
        try:
            dfs = cx.load_station(station)
            dfs.to_csv(f"cimis_{station:03d}.csv", index="Date")
        except Exception as e:
            print(f"Error: {e}")
            continue


if __name__ == "__main__":
    main()
