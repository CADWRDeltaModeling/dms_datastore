#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import pandas as pd
import matplotlib.pyplot as plt
from dms_datastore.read_ts import read_ts,read_yaml_header
from dms_datastore import dstore_config
import glob
from vtools.functions.merge import ts_merge,ts_splice
from vtools.functions.unit_conversions import *

__all__ = ["read_ts_repo","ts_multifile_read"]


def infer_source_priority(station_id):
    """Infer the priority of provider for a given station.
       For instance, if the station_id is from NCRO, the best provider
       for that source is ncro and the backup is cdec: ["ncro","cdec"]
    """
    if 'source_priority' not in dstore_config.config:
        return None
    priorities = dstore_config.config["source_priority"]
    db = dstore_config.station_dbase()
    agency = db.loc[station_id,'agency']
    return priorities[agency] if agency in priorities else None

def fahren2cel(ts):
    tsout = fahrenheit_to_celsius(ts)
    tsout = tsout.round(2)
    return tsout


def read_ts_repo(station_id,variable,
                 subloc=None,repo=None,
                 src_priority="infer",
                 meta=False,
                 force_regular=False):
    """ Read time series data from a repository, prioritizing sources
  
    station_id : str
        Station ID as defined in csv station file (see station_dbase.csv or station_info).
        Possibly make it so that the agency or source id also works?

    variable : str
        Variable name as defined in variables.csv (not the agency name)
        
    repo : str
        Name of repository. If it is a directory, will be used directly. Otherwise
        it will be used to look up the name of the resitory using the dbase config file.
        Finally if it is None, it will look up the default config file which is 
        in the config file under the name 'repo'.
        
    """
    # Do this before adding the sublocation and creating the pattern
    if  src_priority == 'infer':
        src_priority = infer_source_priority(station_id)
    if src_priority is None:
        src_priority = "*" #dstore_config.config("source_priority")

    if subloc is not None:
        if "@" in station_id:
            raise ValueError("@ short hand and subloc are mutually exclusive")
        else:
            station_id = station_id+"@"+subloc if subloc != 'default' else station_id
    
    if repo is None:
        repository = dstore_config.config_file('repo')
    elif os.path.exists(repo):
        repository=repo
    else:
        repository = dstore_config.config_file(repo)



    pats = []
    for src in src_priority:
        pats.append(os.path.join(repository,f"{src}_{station_id}_*_{variable}_*.*"))
    retval = ts_multifile(pats,meta=meta) 
    print(type(retval))
    print("meta is",meta)
    return retval

def detect_dms_unit(fname):
    meta = read_yaml_header(fname)
    unit =  meta['unit'] if 'unit' in meta else None
    if unit in ["FNU","NTU"]:
        return"FNU",None
    elif unit in ["uS/cm","microS/cm"]:
        return "microS/cm",None
    elif unit == "meters":
        return "meters", None
    elif unit == "cfs":
        return "ft^3/s",None
    elif unit == "deg_f":
        return "deg_c",fahren2cel
    else:
        return unit, None

def ts_multifile(pats,selector=None,column_names=None,meta=False,force_regular=True):
    """ within a pattern assumes unit consistency and uses merge. between it assumes splice with earlier better"""
    if not(isinstance(pats,list)):
        pats = [pats]

    units = []
    metas = []
    some_files = False
    pats_revised = []  # for culling empty patterns
    for fp in pats:
        tsfiles = glob.glob(fp)
        if len(tsfiles) == 0: 
            print(f"No files for pattern {fp}")
            continue
        else:
            pats_revised.append(fp)
        # assume consistency within each pattern
        unit,transform = detect_dms_unit(tsfiles[0])
        units.append((unit,transform))
        example_header = read_yaml_header(tsfiles[0])
        example_header["unit"] = unit
        metas.append(example_header)
        some_files = True
    pats = pats_revised 
    if not some_files:
        print(f"No files for pats")
        return None
    bigts = [] # list of time series from each pattern in pats
    patternfreq = []
    total_series = 0
    for fp,utrans in zip(pats,units):  # loop through patterns
        tsfiles = glob.glob(fp)
        tss = []
        unit,transform = utrans
        commonfreq = None
        for tsfile in tsfiles:  # loop through files in pattern
            print(tsfile)
            # read one by one, not by pattern/wildcard
            ts = read_ts(tsfile,force_regular=force_regular)  
            if ts.shape[1] > 1:   # not sure about why we do this here
                if selector is not None:
                    ts = ts[selector].to_frame()
            if column_names is not None:
                if isinstance(column_names,str): column_names = [column_names]
                ts.columns=column_names
            # possibly apply unit transition
            ts = ts if transform is None else transform(ts)
            tss.append(ts)
            tsfreq = ts.index.freq if  hasattr(ts.index,"freq") else None
            if commonfreq is None: 
                commonfreq = tsfreq
            elif tsfreq < commonfreq: 
                print(f"frequency change detected from {commonfreq} to {tsfreq} within pattern")
                commonfreq = tsfreq
                if commonfreq == 'D':
                    severe = True
                    print("Severe")  # Need to test on CLC
        patternfreq.append(commonfreq)  
        # Series within a pattern are assumed compatible, so use merge, which will fill across series
        if len(tss) == 0:
            print(f"No series for subpattern: {fp}")
        else:
            patfull = ts_merge(tss)
            total_series = total_series + len(tss)
            if commonfreq is not None: 
                patfull = patfull.asfreq(commonfreq)
            bigts.append(patfull)
                
    #if total_series == 0: 
    #    for p in pats: 
    #        print(p)
    #    raise ValueError("Patterns produced no matches")

    # now organize freq across patterns
    cfrq = None     # this will be the common frequency
    for f in patternfreq:
        if cfrq is None:
            cfrq = f 
        elif f < cfrq:
            cfrq = f        
           
    fullout = ts_splice(bigts,transition="prefer_first")
    if cfrq is not None: fullout = fullout.asfreq(cfrq)
    retval = (metas,fullout) if meta else fullout
    return retval

def ts_multifile_read(pats,transforms=None,selector=None,column_name=None):

    if not(isinstance(pats,list)):
        pats = [pats]
    if transforms is None: transforms = [None]*len(pats)
    tss = []
    for fp,trans in zip(pats,transforms):
        tsfiles = glob.glob(fp)
        for tsfile in tsfiles:
            print(tsfile)
            ts = read_ts(tsfile)
            if ts.shape[1] > 1:
                if selector is None:
                    ts = ts.mean(axis=1).to_frame()
                else:
                    ts = ts[selector].to_frame()
            if column_name is not None:
                ts.columns=[column_name]
            ts = ts if trans is None else trans(ts)
            tss.append(ts)
        
    if len(tss) == 0: 
        for p in pats: print(p)
        raise ValueError("Patterns produced no matches")
    
    commonfreq = None
    for ts in tss:
        tsfreq = ts.index.freq if  hasattr(ts.index,"freq") else None
        if tsfreq is not None: 
            if commonfreq is None: 
                commonfreq = tsfreq
            elif tsfreq < commonfreq: 
                print(f"frequency change detected from {commonfreq} to {tsfreq}")
                commonfreq = tsfreq        
    full = ts_merge(tss)
    if commonfreq is not None: full = full.asfreq(commonfreq)
    return full    


if __name__ == "__main__":
    # NCRO example
    
    dirname = "//cnrastore-bdo/Modeling_Data/continuous_station_repo_beta/formatted_1yr" 
    rpats = ["ncro_gle_b9532000_temp*.csv","cdec_gle*temp*.csv"]
    pats =  [os.path.join(dirname,p) for p in rpats]
    ts = ts_multifile(pats)
    print(ts)
    ts.plot()
    plt.show()

    # Example for USGS
    #usgs_list = ['lib','ucs','srv','dsj','dws','sdi','fpt','lps','mld','sjj','sjg']
    #for nseries in usgs_list:
    #    print(nseries)
    #    
    #    dirname = "//cnrastore-bdo/Modeling_Data/continuous_station_repo/raw/" 
    #    pat = os.path.join(dirname,f"usgs_{nseries}_*turbidity_*.rdb")
    #    ts = ts_multifile_read(pat,column_name=nseries)
    #    print(ts)
    #    ts.plot()
    #    plt.show()


