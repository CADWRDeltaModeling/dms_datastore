# -*- coding: utf-8 -*-
"""
Created on Fri Jan 20 09:07:50 2023
Download NOAA High-Resolution Rapid Refresh (HRRR) Model using AWS bucket
service
"""
import argparse
from dms_datastore.hrrr3 import *
import datetime


def create_arg_parser():
    """ Create an argument parser
        return: argparse.ArgumentParser
    """

    # Read in the input file
    parser = argparse.ArgumentParser(
        description="""
        Download Download NOAA High-Resolution Rapid Refresh
        (HRRR) Model using AWS bucket service.
        Usage:
        download_hrrr --sdate 2018-02-19  --dest /path/to/modeling_data
                     """)
    parser.add_argument('--sdate', default=None, required=True,
                        help='starting date of HRRR data, must be \
                        format like 2018-02-19')
    parser.add_argument('--dest', default=None, required=True,
                        help='path to store downloaded HRRR data')
    parser.add_argument('--edate', default=None, required=False,
                        help="end date for the record to be downloaded,\
                            if not given download up to today")
    parser.add_argument('--lat_min', default=37.36, type=float,
                        required=False, help='Minimal latitude of bounding box\
                            for raw data to be downloaded,default 37.36')
    parser.add_argument('--lat_max', default=39.0, type=float,
                        required=False, help='Maximal latitude of bounding box\
                            for raw data to be downloaded,default 39.0')
    parser.add_argument('--lon_min', default=-123.023, type=float,
                        required=False, help='Minimal longititude of bounding\
                        box for raw data to be downloaded,default -123.023')
    parser.add_argument('--lon_max', default=-121.16, type=float,
                        required=False, help='Maximal longititude of bounding\
                            box for raw data to be downloaded,default -121.16')

    return parser


def download_hrrr(start_date, rnday, pscr, bbox):

    hr3 = HRRR(start_date=start_date, rnday=rnday, pscr=pscr, bbox=bbox)


def main():
    """ Main function
    """
    parser = create_arg_parser()
    args = parser.parse_args()

    bbox = [args.lon_min, args.lat_min, args.lon_max, args.lat_max]
    pscr = args.dest
    end_date = args.edate

    start_date = datetime.datetime.strptime(args.sdate, '%Y-%m-%d')
    if end_date is None:
        end_date = datetime.datetime.now()
    else:
        end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    rnday = (datetime.datetime.now() - start_date).days + 1
    download_hrrr(start_date, rnday, pscr, bbox)


if __name__ == "__main__":
    main()
