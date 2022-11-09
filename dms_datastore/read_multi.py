#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import pandas as pd
import matplotlib.pyplot as plt
from dms_datastore.read_ts import read_ts
from dms_datastore import dstore_config
import glob
from vtools.functions.merge import ts_merge


def read_ts_repo(station_id,variable,repo=None,src_priority=None):
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
    if repo is None:
        repository = dstore_config.config_file('repo')
    elif os.path.exists(repo):
        repository=repo
    else:
        repository = dstore_config_file(repo)

    if src_priority is None:
        src_priority = "*" #dstore_config.config("source_priority")
    pats = []
    print(pats)
    for src in src_priority:
        pats.append(os.path.join(repository,f"{src}_{station_id}_*_{variable}_*.*"))
    return ts_multifile_read(pats) 
    

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
    
    dirname = "//cnrastore-bdo/Modeling_Data/continuous_station_repo/raw/" 
    rpats = ["ncro_gle_*temp*.csv","cdec_gle*temp*.csv"]
    pats =  [os.path.join(dirname,p) for p in rpats]
    ts = ts_multifile_read(pats,column_name='value')
    print(ts)
    ts.plot()
    plt.show()

    # Example for USGS
    usgs_list = ['lib','ucs','srv','dsj','dws','sdi','fpt','lps','mld','sjj','sjg']
    for nseries in usgs_list:
        print(nseries)
        
        dirname = "//cnrastore-bdo/Modeling_Data/continuous_station_repo/raw/" 
        pat = os.path.join(dirname,f"usgs_{nseries}_*turbidity_*.rdb")
        ts = ts_multifile_read(pat,column_name=nseries)
        print(ts)
        ts.plot()
        plt.show()


