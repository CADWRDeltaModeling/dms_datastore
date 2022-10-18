#!/usr/bin/env python
# -*- coding: utf-8 -*-import pandas as pd

import os
import argparse
import dms_datastore.dstore_config as dbconfig


def delete_from_filelist(filelist, dpath=None):
    """ Delete all files in filelist in directory 

    Parameters
    ----------

    filelist : str

    Name of file listing files to be deleted. Can contain comments starting with #

    dpath : str

    Directory relative to which listed files will be deleted. If omitted, will be taken from 
    the config variable file_deletion_list in dstore_config.yaml.


    """
    if filelist is None:
        filelist = dbconfig.config_file('file_deletion_list')

    if dpath is None:
        direct = '.'
    else:
        direct = dpath

    print(
        f"Deleting files listed in file {filelist} relative to directory {direct}")

    with open(filelist) as infile:
        for line in infile:
            if line.startswith("#"):
                continue
            if ":" in line:
                filename = line.split(":")[0]
                path = os.path.join(direct, filename)
                if os.path.exists(path):
                    print(f"Removing {path}")
                    os.remove(path)
                else:
                    print(f"Path not found: {path}")


def create_arg_parser():
    parser = argparse.ArgumentParser('Delete files contained in a list')

    parser.add_argument('--dpath', dest="dpath", default=None,
                        help='Directory where files will be located and deleted. If not, the file names must work in a relative or absolute sense.')
    parser.add_argument('--filelist', default=None,
                        help='Text file listing files to delete.')
    return parser


def main():
    parser = create_arg_parser()
    args = parser.parse_args()
    dpath = args.dpath
    filelist = args.filelist
    delete_from_filelist(filelist, dpath)


if __name__ == "__main__":
    main()
