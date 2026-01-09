# -*- coding: utf-8 -*-
"""
Created on Fri Jan 20 09:07:50 2023
Download NOAA High-Resolution Rapid Refresh (HRRR) Model using AWS bucket
service
"""
import click
from dms_datastore.hrrr3 import *
import datetime


def download_hrrr(start_date, rnday, pscr, bbox):

    hr3 = HRRR(start_date=start_date, rnday=rnday, pscr=pscr, bbox=bbox)


@click.command()
@click.option('--sdate', required=True,
              help='Starting date of HRRR data, must be format like 2018-02-19')
@click.option('--dest', required=True,
              help='Path to store downloaded HRRR data')
@click.option('--edate', default=None,
              help='End date for the record to be downloaded, if not given download up to today')
@click.option('--lat_min', default=37.36, type=float,
              help='Minimal latitude of bounding box for raw data to be downloaded, default 37.36')
@click.option('--lat_max', default=39.0, type=float,
              help='Maximal latitude of bounding box for raw data to be downloaded, default 39.0')
@click.option('--lon_min', default=-123.023, type=float,
              help='Minimal longitude of bounding box for raw data to be downloaded, default -123.023')
@click.option('--lon_max', default=-121.16, type=float,
              help='Maximal longitude of bounding box for raw data to be downloaded, default -121.16')
def download_hrrr_cli(sdate, dest, edate, lat_min, lat_max, lon_min, lon_max):
    """
    Download NOAA High-Resolution Rapid Refresh (HRRR) Model using AWS bucket service.
    
    Example:
        download_hrrr --sdate 2018-02-19 --dest /path/to/modeling_data
    """
    bbox = [lon_min, lat_min, lon_max, lat_max]
    pscr = dest
    start_date = datetime.datetime.strptime(sdate, '%Y-%m-%d')
    if edate is None:
        end_date = datetime.datetime.now()
    else:
        end_date = datetime.datetime.strptime(edate, '%Y-%m-%d')
    rnday = (end_date - start_date).days + 1
    download_hrrr(start_date, rnday, pscr, bbox)


if __name__ == "__main__":
    download_hrrr_cli()
