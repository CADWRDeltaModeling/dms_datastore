#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import glob
import argparse
from functools import reduce
from dms_datastore.filename import interpret_fname
from dms_datastore.dstore_config import station_dbase
from dms_datastore.read_ts import read_yaml_header
import pandas as pd

__all__ = ['repo_file_inventory','repo_data_inventory']

def to_wildcard(fname,remove_source=False):
    """ Convert filename to a wildcard for date. 
    If remove_source, the source slot will also be wildcard
    """
    pat1 = r".*_(\d{4}_\d{4})\.\S{3}"
    re1 = re.compile(pat1)
    if re1.match(fname): 
        out = fname[0:-13] + "*" +fname[-4:]
    else:
        pat2 = r".*(_\d{4})\.\S{3}"
        re2 = re.compile(pat2)
        if re2.match(fname):
            out = fname[0:-8]+"*"+fname[-4:]
    if remove_source:
        outparts = out.split("_")
        outparts[0] = "*"
        out = "_".join(outparts)
    return out      
    

def scrape_header_metadata(fname):
    yml = read_yaml_header(fname)
    if (yml is None): 
        print(f"{fname} produced nan metadata")
        return None
    return yml['unit'] if 'unit' in yml else None


def repo_file_inventory(fpath,full=True,by="file_pattern"):
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
    allfiles = glob.glob(os.path.join(fpath,"*_*.rdb")) + glob.glob(os.path.join(fpath,"*_*.csv"))
    # Dictionary with station_id,agency_id,variable,,start,end, etc
    allmeta = [interpret_fname(fname) for fname in allfiles] 
    metadf = pd.DataFrame(allmeta)
    if metadf.empty: 
        raise ValueError("Empty inventory")
    metadf['original_filename'] = metadf['filename']  # preserves the entire filename so first file can be parsed
    metadf['filename'] = metadf.apply(lambda x: to_wildcard(x.filename),axis=1)
    double_year_format = "syear" in metadf.columns
    if "syear" in metadf.columns:
        # double year (other_data_2000_2024.csv) format
        # this was a quick patch and not sure that the groupby is needed
        grouped_meta = metadf.groupby(["filename"]).agg(
            {
            "station_id":'first',
            "subloc":'first',
            "param":'first',
            "agency": "first",
            "agency_id":['first'],
            "syear":['min'],
            "eyear":['max'],
            "original_filename":['first']   # the first file will be used to scrape metadata then dropped
            }
            )
    
    else:
        grouped_meta = metadf.groupby(["filename"]).agg(
            {
            "station_id":'first',
            "subloc":'first',
            "param":'first',
            "agency": "first",
            "agency_id":['first'],
            "year":['min','max'],
            "original_filename":['first']   # the first file will be used to scrape metadata then dropped
            }
            )
    grouped_meta.columns = ['station_id','subloc','param','source',
                            'agency_id','min_year','max_year','original_filename'] 

    metastat = grouped_meta.join(station_db,on="station_id",
                            rsuffix="_dbase",how="left")
                            
    metastat.rename(mapper={"lat":"agency_lat","lon":"agency_lon"},axis='columns')
    print(metastat[['original_filename']])
    if double_year_format:
        metastat['unit'] = None
    else:
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
    metadf['filename'] = metadf.apply(lambda x: to_wildcard(x.filename,remove_source=True),axis=1)
    metadf['source'] = metadf['agency']

    metadf.loc[:,'agency'] = station_db.loc[metadf.station_id,'agency'].to_numpy()
    double_year_format = "syear" in metadf.columns

    #meta2 = metadf.groupby(["station_id","subloc","param"]).first()
    if double_year_format:
        # todo: is a groupby necessary for double year format? are there duplicates?
        grouped_meta = metadf.groupby(["station_id","subloc","param"],dropna=False).agg(
            {
             "agency": ['first'],
             "agency_id":['first'],
             "syear":['min'],
             "eyear":['max'],
             "filename": ['first'],
             "original_filename":['first']   # this will be used to scrape metadata then dropped
             }
            )
    else:        
        grouped_meta = metadf.groupby(["station_id","subloc","param"],dropna=False).agg(
            {
             "agency": ['first'],
             "agency_id":['first'],
             "year":['min','max'],
             "filename": ['first'],
             "original_filename":['first']   # this will be used to scrape metadata then dropped
             }
            )


    grouped_meta.columns = ['agency','agency_id','min_year','max_year',
                            'filename','original_filename'] 
    metastat = grouped_meta.join(station_db,on="station_id",
                            rsuffix="_dbase",how="left")
                            
    metastat.rename(mapper={"lat":"agency_lat","lon":"agency_lon"},axis='columns')
    if double_year_format:
        metastat['unit'] = None
    else:
        metastat['unit'] = metastat.apply(
                          lambda x: scrape_header_metadata(
                          os.path.join(fpath,x.original_filename)),axis=1)
    metastat.drop(labels=['notes','stage','flow','quality','wdl_id','cdec_id','d1641_id','original_filename'],
                  axis=1,inplace=True)

    return metastat
    
    
def create_arg_parser():
    """ Create an argument parser
    """
    parser = argparse.ArgumentParser(description="Create inventory files, including a file inventory, a data inventory and an obs-links file.")
    parser.add_argument('--repo', type=str, 
                        help="directory to be catalogued")
    parser.add_argument('--out_files', default = None,
                        help="Output path for file inventory. Default is file_inventory_{todaydate}.csv ")
    parser.add_argument('--out_data', default= None,
                        help="Output path for data inventory. Default is file_inventory_{todaydate}.csv")
    parser.add_argument('--out_obslinks',default= None,
                        help="Output path for obslinks.csv file. Default is obs_links_{todaydate}.csv")
    return parser


def main():

    parser = create_arg_parser()
    args = parser.parse_args()
    out_files = args.out_files
    out_data = args.out_data
    out_obslinks = args.out_obslinks
    repo = args.repo
    
    nowstr = pd.Timestamp.now().strftime("%Y%m%d")
    # inventory based on describing every file
    if out_files is None: out_files = f"./inventory_files_{repo}_{nowstr}.csv"
    inv=repo_file_inventory(repo)
    inv.to_csv(out_files)
    # inventory based on describing unique datasets 
    # this may be bigger/smaller than the number of files based because of:
    #     multivariate data in files that are/aren't split into multiple streams
    #     data from the same instrument gathered from multiple sources, 
    #        such as period of record and real time multivariate data in files
    if out_data is None: out_data = f"./inventory_datasets_{repo}_{nowstr}.csv"
    inv2=repo_data_inventory(repo)
    inv2.to_csv(out_data)        
    db_obs = inv2.copy()
    db_obs['vdatum'] = 'NAVD88'    # todo: hardwire, not always true
    db_obs['datum_adj'] = 0.       # todo: hardwire, not always true, should be incorporated in station_dbase
    db_obs['source'] = db_obs["agency"] # move agency to source
    db_obs['agency'] = db_obs["agency_dbase"]  
    db_obs.reset_index(inplace=True)
    db_obs['variable'] = db_obs.param
    # param is the variable in the file, variable is the model variable being associated with the data
    db_obs.loc[db_obs.param == 'ec','variable'] = 'salt'
    db_obs['subloc'] = db_obs.subloc.fillna('default')
    if out_obslinks is None: out_obslinks = f"./obs_links_{repo}_{nowstr}.csv"
    db_obs.to_csv(out_obslinks,sep=",",index=False)
    
if __name__ == "__main__":
    main()