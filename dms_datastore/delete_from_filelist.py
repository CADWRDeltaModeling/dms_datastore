#!/usr/bin/env python
# -*- coding: utf-8 -*-import pandas as pd

import os
import click
import dms_datastore.dstore_config as dbconfig
from dms_datastore.logging_config import configure_logging, resolve_loglevel   
import logging
logger = logging.getLogger(__name__)


def delete_from_filelist(filelist, dpath=None):
    """Delete all files in filelist in directory

    Parameters
    ----------

    filelist : str

    Name of file listing files to be deleted. Can contain comments starting with #

    dpath : str

    Directory relative to which listed files will be deleted. If omitted, will be taken from
    the config variable file_deletion_list in dstore_config.yaml.


    """
    if filelist is None:
        filelist = dbconfig.config_file("file_deletion_list")

    if dpath is None:
        direct = "."
    else:
        direct = dpath

    logger.info(
        f"Deleting files listed in file {filelist} relative to directory {direct}"
    )

    with open(filelist) as infile:
        for line in infile:
            if line.startswith("#"):
                continue
            if ":" in line:
                filename = line.split(":")[0]
                path = os.path.join(direct, filename)
                if os.path.exists(path):
                    logger.info(f"Removing {path}")
                    os.remove(path)
                else:
                    logger.info(f"Path not found: {path}")


@click.command()
@click.option(
    "--dpath",
    default=None,
    help="Directory where files will be located and deleted. If not, the file names must work in a relative or absolute sense.",
)
@click.option("--filelist", default=None, help="Text file listing files to delete.")
def delete_from_filelist_cli(dpath, filelist):
    """CLI for deleting files listed in a text file."""

    delete_from_filelist(filelist, dpath)


if __name__ == "__main__":
    delete_from_filelist_cli()
