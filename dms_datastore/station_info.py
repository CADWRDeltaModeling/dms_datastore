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
    station_lookup = dstore_config.config_file("station_dbase")
    if search == "config":
        print(dstore_config.configuration())
        return
    #vlookup = dstore_config.config_file("variable_mappings")
    #slookup = pd.read_csv(station_lookup,sep=",",comment="#",header=0,usecols=["id","agency",
    #                                                                           "agency_id","name",
    #                                                                           "x","y","lat","lon"]).squeeze()
    slookup = dstore_config.station_dbase()[["agency","agency_id","name","x","y","lat","lon"]]
    slookup.loc[:,"station_id"] = slookup.index.str.lower()
    lsearch = search.lower()
    match_id = slookup.station_id.str.contains(lsearch)
    match_name = slookup.name.str.lower().str.contains(lsearch)
    match_agency_id = slookup.agency_id.str.lower().str.contains(lsearch)
    match_agency = slookup.agency.str.lower().str.contains(lsearch)
    matches = match_id | match_name | match_agency_id | match_agency
    print("Matches:")
    mlook =slookup.loc[matches,["station_id","agency","agency_id","name","x","y","lat","lon"]].sort_values(axis=0,by='station_id')  #.set_index("id") 
    if mlook.shape[0] == 0: 
        print("None")
    else:
        print(mlook.to_string())
    return mlook
    
    
@click.command()
@click.option(
    '--config',
    is_flag=True,
    default=False,
    help='Print configuration and location of lookup files'
)
@click.argument('searchphrase', required=False, default='')
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
