# -*- coding: utf-8 -*-
"""
Created on Fri Jan 20 09:07:50 2023
Download NOAA High-Resolution Rapid Refresh (HRRR) Model using AWS bucket service
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
        download_hrrr 01/01/2023  g:\temp 15  37.36  39.0 -123.023 -121.16
                     """)
    parser.add_argument('--start_date', default=None, required=True,
                        help='starting date of HRRR data, must be \
                        format like 09/19/2018')
    parser.add_argument('--destination', default=None, required=True,
                        help='path to store downloaded HRRR data')
    parser.add_argument('--rnday', default=None, type=int, required=False,
                        help="number of days of data to be downloaded,\
                            if not given download up to today")
    parser.add_argument('--latitude_min', default=37.36, type=float,
                        required=False, help='Minimal latitude of bounding box\
                            for raw data to be downloaded,default 37.36')
    parser.add_argument('--latitude_max', default=39.0, type=float,
                        required=False, help='Maximal latitude of bounding box\
                            for raw data to be downloaded,default 39.0')
    parser.add_argument('--longitude_min', default=-123.023, type=float,
                        required=False, help='Minimal longititude of bounding\
                        box for raw data to be downloaded,default -123.023')
    parser.add_argument('--longitude_max', default=-121.16, type=float,
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

    bbox = [args.longitude_min, args.latitude_min, args.longitude_max,
            args.latitude_max]
    pscr = args.destination
    rnday = args.rnday

    start_date = datetime.datetime.strptime(args.start_date, '%m/%d/%Y')
    if rnday is None:
        rnday = (datetime.datetime.now() - start_date).days + 1
    download_hrrr(start_date, rnday, pscr, bbox)


if __name__ == "__main__":
    main()
