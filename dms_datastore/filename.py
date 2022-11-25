#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re

def interpret_fname(fname):
    """ Convert filename to metadata dictionary 
    The filename follows convention [agency/source]_[station_id]_[agency_id]@[subloc]_[param]_[syear]_[eyear].csv
    or for single year sharded data just [year] rather than [syear]_[eyear]
    
    This routine is complementary in functionality to meta_to_filename.
    
    Parameters
    ----------
    fname : str
        File name to be interpreted
    
    Returns
    -------
    fname : str Dictionary of metadata gleaned from filename
    

    """    
    
    
    fname = os.path.split(fname)[1]
    meta = {}
    datere = re.compile(r"([a-z0-9]+)_([a-z0-9@]+)_([a-z0-9]+)_([a-z0-9]+).*_(\d{4})_(\d{4})(?:\..{3})")
    datere1 = re.compile(r"([a-z0-9]+)_([a-z0-9@]+)_([a-z0-9]+)_([a-z0-9]+).*_(\d{4})(?:\..{3})")
    m = datere.match(fname)
    if m is None:
        m = datere1.match(fname)
        single_date = True
    else:
        single_date = False
    if m is not None:
        meta['filename'] = m.group(0)
        meta['agency'] = m.group(1)
        station_id = m.group(2)
        if "@" in station_id: 
            station_id,subloc = station_id.split("@")
        else: 
            subloc = None
           
        meta['station_id'] = station_id
        meta['subloc'] = subloc
        meta['agency_id'] = m.group(3)
        meta['param'] = m.group(4)
        if single_date:
            meta['year'] = m.group(5)
        else:
            meta['syear'] = m.group(5)
            meta['eyear'] = m.group(6)
        return meta
    else:
        raise ValueError(f"Naming convention not matched for {fname}")

def meta_to_filename(meta):
    """ Convert dictionary of file/station/data info to a filename.
    This routine is complementary to interpret_fname.
    
    Parameters
    ----------
    meta : dict
        A dictionary of data that includes: station_id and subloc if pertinent, 
        agency (representing source), agency_id of station, param, start and end year.
        These should match conventions: id, station_id and agency should match the station
        database, param should be a standard parameter name listed in variables.csv.
        
    Returns
    -------
    filename : str
        Filename corresponding to metadata
    
    """
    station_id = meta['station_id'] if meta['subloc'] is None else f"{meta.station_id}@{meta.subloc}"
    if 'syear' in meta and 'eyear' in meta:
        year_part = f"{meta['syear']}_{meta['eyear']}"
    else:
        year_part = f"{meta['year']}"
    return  f"{meta['agency']}_{station_id}_{meta['agency_id']}"+\
            f"_{meta['param']}_{year_part}.csv"


