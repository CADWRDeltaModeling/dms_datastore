#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import glob
from functools import reduce
from filename import interpret_fname
from dstore_config import station_dbase
from dms_datastore.read_ts import read_yaml_header
import pandas as pd
#import matplotlib.pyplot as plt
#from dms_datastore.read_ts import *

def to_wildcard(fname,remove_source=True):

    pat1 = r".*_(\d{4}_\d{4})\.csv"
    re1 = re.compile(pat1)
    if re1.match(fname): 
        print("match1")
    else:
        pat2 = r".*(_\d{4})\.csv"
        re2 = re.compile(pat2)
        if re2.match(fname):
            out = fname[0:-8]+"*"+fname[-4:]
            return out      
    

def scrape_header_metadata(fname):
    yml = read_yaml_header(fname)
    return yml['unit'] if 'unit' in yml else None


def repo_inventory(fpath,full=True,by="file_pattern"):
    """Create a Pandas Dataframe containing all the unique time series in a directory
    Currently assumes yearly sharding in time.
    
    Parameters
    ----------
    fpath : str
        Path to the repository being inventoried
    full_parse : bool
        Open the file and obtain metadata (unit) from the headers
    by : ['file_pattern', 'data'] 
        For file_pattern, there is one entry per unique filename except for the years which
        are pivoted to the min_year and max_year column. If 'data', the data source is 
        converted to a wildcard and the entries describe the unique series in the data base,
        which depends on station, sublocation and variable.        
    
    Returns
    -------
    Inventory dataframe
    
    """
    if by != 'file_pattern': 
        raise NotImplementedError("Only by=file_pattern is implemented")
    station_db = station_dbase()
    print(station_db)
    allfiles = glob.glob(os.path.join(fpath,"*_*.rdb")) + glob.glob(os.path.join(fpath,"*_*.csv"))
    # Dictionary with station_id,agency_id,variable,,start,end, etc
    allmeta = [interpret_fname(fname) for fname in allfiles] 
    metadf = pd.DataFrame(allmeta)
    metadf['original_filename'] = metadf.filename
    metadf['filename'] = metadf.apply(lambda x: to_wildcard(x.filename),axis=1)
    grouped_meta = metadf.groupby(["filename"]).agg(
        {
         "station_id":'first',
         "subloc":'first',
         "param":'first',
         "agency": "first",
         "agency_id":['first'],
         "year":['min','max'],
         "original_filename":['first']   # this will be used to scrape metadata then dropped
         }
        )
    grouped_meta.columns = ['station_id','subloc','param','source',
                            'agency_id','min_year','max_year','original_filename'] 

    metastat = grouped_meta.join(station_db,on="station_id",
                            rsuffix="_dbase",how="left")
                            
    metastat.rename(mapper={"lat":"agency_lat","lon":"agency_lon"},axis='columns')
    print(metastat[['original_filename']])
    metastat['unit'] = metastat.apply(
                          lambda x: scrape_header_metadata(
                          os.path.join(fpath,x.original_filename)),axis=1)
    metastat.drop(labels=['notes','stage','flow','quality','wdl_id','cdec_id','d1641_id','original_filename'],
                  axis=1,inplace=True)

    return metastat
        
def prioritize_source(x,y,priorities=['ncro','usgs','aquarius','ccwd','cdec','ebmud']):
    yndx = priorities.index(y) if y in priorities else 100000
    xndx = priorities.index(x) if x in priorities else 100000
    return y if yndx < xndx else x
        
def repo_data_inventory(fpath,full=True,by="file_pattern"):
    """Create a Pandas Dataframe containing all the unique time series in a directory
    Currently assumes yearly sharding in time.
    
    Parameters
    ----------
    fpath : str
        Path to the repository being inventoried
    full_parse : bool
        Open the file and obtain metadata (unit) from the headers
    by : ['file_pattern', 'data'] 
        For file_pattern, there is one entry per unique filename except for the years which
        are pivoted to the min_year and max_year column. If 'data', the data source is 
        converted to a wildcard and the entries describe the unique series in the data base,
        which depends on station, sublocation and variable.        
    
    Returns
    -------
    Inventory dataframe
    
    """
    if by != 'file_pattern': 
        raise NotImplementedError("Only by=file_pattern is implemented")
    station_db = station_dbase()
    print(station_db)
    allfiles = glob.glob(os.path.join(fpath,"*_*.rdb")) + glob.glob(os.path.join(fpath,"*_*.csv"))
    # Dictionary with station_id,agency_id,variable,,start,end, etc
    allmeta = [interpret_fname(fname) for fname in allfiles] 
    metadf = pd.DataFrame(allmeta)
    metadf['original_filename'] = metadf.filename
    metadf['filename'] = metadf.apply(lambda x: to_wildcard(x.filename),axis=1)
    grouped_meta = metadf.groupby(["station_id","subloc","param"]).agg(
        {
         "agency": lambda ser: reduce(prioritize_source,ser),
         "agency_id":['first'],
         "year":['min','max'],
         "original_filename":['first']   # this will be used to scrape metadata then dropped
         }
        )
    grouped_meta.columns = ['agency','agency_id','min_year','max_year','original_filename'] 

    metastat = grouped_meta.join(station_db,on="station_id",
                            rsuffix="_dbase",how="left")
                            
    metastat.rename(mapper={"lat":"agency_lat","lon":"agency_lon"},axis='columns')
    print(metastat[['original_filename']])
    metastat['unit'] = metastat.apply(
                          lambda x: scrape_header_metadata(
                          os.path.join(fpath,x.original_filename)),axis=1)
    metastat.drop(labels=['notes','stage','flow','quality','wdl_id','cdec_id','d1641_id','original_filename'],
                  axis=1,inplace=True)

    return metastat
    
    
if __name__ == "__main__":
    test=repo_inventory("W:/continuous_station_repo_beta/formatted_1yr")
    print(test)
    test.to_csv("W:/continuous_station_repo_beta/test.csv")
    test2=repo_data_inventory("W:/continuous_station_repo_beta/formatted_1yr")
    print(test2)
    test2.to_csv("W:/continuous_station_repo_beta/test2.csv")        
   