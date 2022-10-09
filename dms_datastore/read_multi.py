#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import pandas as pd
import matplotlib.pyplot as plt
from dms_datastore.read_ts import *
import glob
from vtools.functions.merge import ts_merge

def ts_multifile_read(pats,transforms=None,selector=None,column_name=None):

    if not(isinstance(pats,list)):
        pats = [pats]
    print("pats",pats)
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


