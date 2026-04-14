#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import pandas as pd
import click
from dms_datastore import dstore_config


def station_info(search):
    """
    Lookup station metadata by partial string match on id or name.

    Arguments:
        SEARCHPHRASE: Search phrase which can be blank if using --config
    """
    if search == "config":
        print(dstore_config.configuration())
        return

    slookup = dstore_config.station_dbase()[
        ["station_id", "agency", "agency_id", "name", "x", "y", "lat", "lon"]
    ].copy()

    # Avoid ambiguity between index name and column label.
    slookup = slookup.reset_index(drop=True)

    slookup["station_id"] = slookup["station_id"].astype(str).str.lower()
    slookup["agency"] = slookup["agency"].astype(str)
    slookup["agency_id"] = slookup["agency_id"].astype(str)
    slookup["name"] = slookup["name"].astype(str)

    lsearch = search.lower()
    match_id = slookup["station_id"].str.contains(lsearch, na=False)
    match_name = slookup["name"].str.lower().str.contains(lsearch, na=False)
    match_agency_id = slookup["agency_id"].str.lower().str.contains(lsearch, na=False)
    match_agency = slookup["agency"].str.lower().str.contains(lsearch, na=False)

    matches = match_id | match_name | match_agency_id | match_agency

    print("Matches:")
    mlook = slookup.loc[
        matches, ["station_id", "agency", "agency_id", "name", "x", "y", "lat", "lon"]
    ].sort_values(by="station_id")

    if mlook.shape[0] == 0:
        print("None")
    else:
        print(mlook.to_string(index=False))
    return mlook

@click.command()
@click.option(
    "--config",
    is_flag=True,
    default=False,
    help="Print configuration and location of lookup files",
)
@click.argument("searchphrase", required=False, default="")
def station_info_cli(config, searchphrase):
    """CLI for searching station information.

    Arguments:
        SEARCHPHRASE: Search phrase which can be blank if using --config
    """
    if config:
        searchphrase = "config"
    if not searchphrase and not config:
        raise ValueError("searchphrase required")
    station_info(searchphrase)


if __name__ == "__main__":
    station_info_cli()
