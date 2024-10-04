#!/usr/bin/env python
# -*- coding: utf-8 -*-import pandas as pd

import os
import glob
import re
import pandas as pd
import tempfile
import shutil
import matplotlib.pyplot as plt
import argparse
import yaml
from dms_datastore.logging_config import logger
from dms_datastore.read_ts import *
from dms_datastore.write_ts import *
from dms_datastore.filename import interpret_fname,meta_to_filename

def _quarantine_file(fname,quarantine_dir = "quarantine"):
    if not os.path.exists(quarantine_dir):
        os.makedirs("quarantine")
    shutil.copy(fname,"quarantine")


def usgs_scan_series_json(fname):
    hdr = read_yaml_header(fname)
    orig = yaml.safe_load(hdr['original_header'])
    subs = orig['sublocations']
    var =  orig['variable_code']
    series = [(str(s['subloc']),var,s['method_description']) for s in subs]
    return series

def usgs_scan_series(fname):
    """ Scans file and returns a list of time series id, parameter codes and 
    description for each series in the file 
    
    Parameters
    ----------
    fname : str
        file to scan
        
    Returns
    --------
    
    series : list
        list of (ts_id,param,description)
    
    """
    try:
        scan = usgs_scan_series_json(fname)
        return scan
    except:
        # This code is the old scanning code for rdb format
        descript_re = re.compile(r"(\#\s+)?\#\s+TS_ID\s+Parameter\s+Description")        
        def read_ts_data(line):
            # This method of splitting "gives up" and leaves description intact
            parts = line.strip().split(None,3)
            if len(parts) < 2 or (len(parts) == 2 and parts[1] == "#"): 
                describing=2
                return describing,(None,None,None)
            if parts[1] == "#":   # There are two comments, redo split to get description intact
                parts = line.strip().split(None,4)
            parts = [p for p in parts if p != "#"] 
            ts_id,param,descr = parts[0:3] 
            describing=1
            return describing,(ts_id,param,descr)
        
        series = []
        describing = 0  # state of the parser, =1 when entering descriptionsection and =2 when leaving
        formatted = False
        with open(fname,"r") as g:
            descrline = None
            for line in g:
                if "Parameter" in line: 
                    pass
                    #print(fname)
                    #print(line)
                if "original_header" in line:
                    formatted = True  # there may be two comments
                if descript_re.match(line):
                    describing = 1
                    continue
                elif describing == 1:
                    describing,(ts_id,param,descr) = read_ts_data(line)
                    if describing == 1: 
                        series.append([ts_id,param,descr])
                elif describing == 2:
                    break        
            if describing < 2: 
                raise ValueError(f"Time series description section not found in file {fname} using either the json or rdb assumption")
        return series



def usgs_multivariate(pat,outfile):
    """ Scans all NWIS-style files matching pattern pat and lists metadata for files that are multivariate
    
    Parameters
    ----------
    pat : str
        globbing battern to match
    outfile : str
        output file name
    """
    special_cases = [("m13","306155","upward"),
                     ("m13","306207","vertical"),
                     ("c24","287157","vertical"),
                     ("c24","287159","upward")]
    
    with open(outfile,'w',encoding='utf-8') as out:
        files = glob.glob(pat)
        data = []
        for fname in files:
            meta = interpret_fname(fname)
            try:
                ts = read_ts(fname,nrows=4000)
            except:
                logger.warning(f"Failed to read file with read_ts(): {fname}")
                continue
            if ts.shape[1] != 1:
                message = f"usgs_meta: file {fname} Columns {ts.columns}"
                logger.debug(message)
                try:
                    series = usgs_scan_series(fname)  # Extract list of series in file
                except:
                    _quarantine_file(fname)
                    logger.debug(f"Could not scan USGS file for variables: {fname}")
                    continue
                for s in series:
                    (ats_id,aparam,adescr) = s
                    out.write(message+"\n")
                    asubloc = 'default'
                    for item in special_cases:
                        if ats_id == item[1]:
                            asubloc = item[2]
                    if "upper" in adescr.lower(): asubloc = 'upper'
                    if "lower" in adescr.lower(): asubloc = 'lower'
                    if "bottom" in adescr.lower(): asubloc = 'lower'
                    if "mid" in adescr.lower(): asubloc = 'mid'                
                    yr = int(meta["year"]) if "year" in meta else int(meta["syear"])
                    data.append( (meta["station_id"],meta["agency_id"],meta["param"],yr,asubloc,ats_id,aparam,adescr) )
                    sout = ",".join(list(s))+"\n"                       
                    out.write(sout)                  
            del(ts)
            
    df = pd.DataFrame(data=data,columns=["station_id","agency_id","param","syear",
                                         "asubloc","ts_id","var_id","description"])
    df = df[~df.duplicated(subset=["ts_id"])]
    df.index.name = 'id'
    df.to_csv("usgs_subloc_meta.csv",index=False)
    return df



def process_multivariate_usgs(fpath,pat=None,rescan=True):
    """ Identify and separate or combine multivariate USGS files.
        Separate sublocations if they are known (typically the vertical ones like upper/lower) 
        Otherwise aggregates the columns and adds a value column containing their mean ignoring nans.
        Often only one is active at a time and in this case the treatment is equivalent to selecting
        the one that is active
    """
    logger.info("Entering process_multivariate_usgs")
    
    # todo: straighten out fpath and pat stuff
    tempfile.tempdir='.'
    tmpdir = tempfile.TemporaryDirectory()
   
    if pat is None: 
        pat = fpath+"/usgs*.csv"
    else:
        pat = fpath + "/" + pat #"/usgs*.csv"
    
    # This recreates or reuses  list of multivariate files. Being multivariate is something that has 
    # to be assessed over the full period of record
    if rescan:
        df = usgs_multivariate(pat,'usgs_subloc_meta_new.csv')
    else:
        df = pd.read_csv("usgs_subloc_meta.csv",header=0,dtype=str)
    df.reset_index()
    df.index.name='id'
    filenames = glob.glob(pat)
    set_of_deletions = set()
    
    for fn in filenames:
        direct,filepart = os.path.split(fn)
        meta = interpret_fname(filepart)
        station_id = meta["station_id"]
        param = meta["param"]
        logger.info(f"Working on {fn}, {station_id}, {param}")
        subdf = df.loc[(df.station_id==station_id) & (df.param==param),:]
        if subdf.empty:
            logger.debug("No entry in table indicating multivariate content, skipping")
            continue
        if len(subdf) == 1:
            raise ValueError("Dataset with only one sublocation not expected")
        
        original_header = read_yaml_header(fn)
        
        ts = read_ts(fn)
        logger.info(f"Number of sublocation metadata entries for {station_id} {param} = {len(subdf)}")
        vertical_non = [0,0]  # for counting how many subloc are vertical or not
        

        # first process all known sublocations that are meant to be kept intact,
        # dropping them as processed
        # then if one left it is default and if many use the average
        for index,row in subdf.iterrows():
            asubloc = row.asubloc[:]
            logger.info(f"Isolating sublocation {asubloc[:]}")
            if asubloc[:] in ["lower","upper","upward","vertical"]:
                # write out each sublocation as individual file
                selector = f"{row.ts_id}_value"
                try:
                    univariate=ts[selector]
                except:
                    logger.warning(f"Selector failed: {selector} columns: {ts.columns}")
                    continue

                if univariate.first_valid_index() is None:
                    ts = ts.drop([selector],axis=1)  
                    # empty for the file
                    continue
                original_header['agency_ts_id'] = row.ts_id
                original_header['agency_var_id'] = row.var_id
                original_header['sublocation'] = asubloc 
                original_header['subloc_comment'] = 'multivariate file separated, mention of other series omitted in this file may appear in original header'                
                meta['subloc'] = asubloc
                newfname = meta_to_filename(meta)
                work_dir,newfname_f = os.path.split(newfname)
                newfpath = os.path.join(tmpdir.name,newfname_f)  ## todo: hardwire
                univariate.columns=['value']
                univariate.name = 'value'
                logger.info(f"Writing to {newfpath}")
                write_ts_csv(univariate,newfpath,original_header,chunk_years=True)
                vertical_non[0] = vertical_non[0]+1  
                ts = ts.drop([selector],axis=1)                

        ncol = len(ts.columns)
        if ncol == 0:
            # No columns were left. Delete the original file as its contents have been parsed to other files
            logger.debug(f"All columns recognized for {fn}")
            set_of_deletions.add(fn)
        else:
            if ncol ==1:
                logger.debug(f"One column left for {fn}, renaming and documenting")
                ts.columns = ['value']
            else:
            
                print(f"Several sublocations for columns, averaging {fn} and labeling as value")
               # Multivariate not collapsed, but we will add a 'value' column that aggregates and note this in metadata
                ts['value']=ts.mean(axis=1)
                original_header['subloc_comment'] = "value averages sublocations"
                original_header['agency_ts_id'] = subdf.ts_id.tolist()
            if ts.first_valid_index() is None: 
                continue # No more good data. bail
            fpath_write = os.path.join(tmpdir.name,filepart)
            write_ts_csv(ts,fpath_write,metadata=original_header,chunk_years=True)
    for fdname in set_of_deletions: 
        logger.debug(f"Removing {fdname}")
        os.remove(fdname)
    shutil.copytree(tmpdir.name,fpath,dirs_exist_ok=True)
    del(tmpdir)
    logger.info("Exiting process_multivariate_usgs")
    



def create_arg_parser():
    parser = argparse.ArgumentParser()   
    parser.add_argument('--pat', dest = "pat", default ="usgs*.csv", help = 'Pattern of files to process')    
    parser.add_argument('--fpath', dest = "fpath", default=".", help = 'Directory of files to process.')
    return parser


    
def test(fname):
    #for fname in ["raw/usgs_benbr_11455780_ec_2020_9999.rdb",
    #              "formatted/usgs_benbr_11455780_ec_2021.csv",
    #              "raw/usgs_sjj_11337190_do_2020_9999.rdb",
    #              "formatted/usgs_sjj_11337190_do_2021.csv"]:
    #    print(fname)
    #    print(usgs_scan_series(fname))
    return usgs_scan_series(fname)
    

def main():
    parser = create_arg_parser()
    args = parser.parse_args()
    pat = args.pat
    fpath = args.fpath

    # recatalogs the unique series. If false an old catalog will be used, which is useful
    # for sequential debugging.
    rescan = True 
    process_multivariate_usgs(fpath=fpath,pat=pat,rescan=True)


if __name__=="__main__":    
     main()
    
    
    
    
    