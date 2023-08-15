#!/usr/bin/env python
# -*- coding: utf-8 -*-


import string
import numpy as np
import pandas as pd
import datetime as dtm
import pandas as pd
import sys
from vtools.functions.unit_conversions import cfs_to_cms,CFS2CMS
from vtools.data.vtime import *
import matplotlib.pyplot as plt
from vtools.functions.filter import *
from vtools.functions.interpolate import *
from vtools.functions.period_op import *
from scipy.ndimage.filters import gaussian_filter1d
from dms_datastore.read_ts import *
from vtools.functions.merge import *

def process_bbid(bbid_csv,interval,stime,etime):
    """Read and smooth BBID data, possibly folding together with DSS file"""
    
    # Read in whatever length period we have -- if it is longer
    # things like gaussian filter won't have edge effects at beginning.
    #if bbid_csv:
    bbid = read_ts(bbid_csv,start=None,end=None).to_period()
    print("CDEC BBID end: %s freq: %s" % (bbid.index[-1],bbid.index.freq))
    bbid = bbid.interpolate(limit=60)
   
    bbid_fine = rhistinterp(bbid+5.,interval,lowbound=0.0,p=8.) - 5.

    print("Smoothing and refining BBID")
    bbid_fine=bbid_fine.clip(lower=0.0)[stime:etime]
    bbid_fine.plot(drawstyle="steps-post")
    plt.legend(["BBID fine"])
    plt.title("BBID data for %s to %s" % (stime,etime))
    plt.show()

   
    write_csv("prepared_bbid_cfs.csv",bbid_fine,{"unit":"cfs"})
    bbid_cms = cfs_to_cms(bbid_fine)
    write_csv("prepared_bbid_cms.csv",bbid_cms,{"unit":"m^3s^-1"})
    return bbid_fine

def write_cdec2(fname,ts,headers):
    import os.path
    title = os.path.split(fname)[1]
    with open(fname,"w") as f:
        f.write("Title: \"%s\"\n" % title.upper().replace(".CSV",".csv"))
        for h in headers:
            f.write("%s\n" % h.strip())
        for el in ts:
            line ="%s,%s\n" % (el.time.strftime("%Y%m%d,%H%M"),el.value)
            f.write(line.replace("nan","m")) 
    f.close()

def write_csv(fname,ts,meta={}):
    with open(fname,"w") as f:
        for item in meta:
            f.write("#{}: {}".format(item,meta[item]))
        ts.to_csv(f,date_format="%Y-%m-%dT%H:%M",float_format="%.2f",line_terminator="\n")
        

MAX_OP_DAY = 300
S_COL = 18
AX_COL = 49
START_ROW = 1
OLD_S = 0
NEW_S = 1
capacity = [[0]+[375]*2 + [1130]*9,
            [0]+[375]*2 + [1067]*6 + [1130]*3,
            [0]+[375]*2 + [1129]*5 + [1101]*4]
            
ops_status={"T/IS":1,"T/S":0,"I/S":1,"I/P":1,"S/D":0,"SED":0,"SO/S":0,"FO/S":0,"FOG":0,"FOP":0}
SRR=0
DFD = 2


def flow_change(version,unit,op,unit_status,stat=None):
    assert version == SRR or version == DFD
    if op.startswith("T"):
        return 0.0,unit_status
    new_status = ops_status[op]
    #if stat and stat in ["I/S","O/S"]: 
    #    new_status = 1 if stat=="I/S" else 0
    if new_status == unit_status[unit]:
        return 0.0,unit_status
    cap = capacity[version]
    efc = cap[unit] if new_status==1 else -cap[unit]
    unit_status[unit] = new_status
    return efc, unit_status

def exact_index(a, x):
    from bisect import bisect_left
    'Locate the leftmost value exactly equal to x'
    i = bisect_left(a, x)
    if i != len(a) and a[i] == x:
        return i
    return -1
            
            
            
def parse_srr(df,rate_version=SRR):
    print("Processing SAP File ******")
    flow_old = None
    dfcull = df[~df.Exclude.isnull()]
    problem = dfcull.Exclude.str.lower() != 'x'
    pdata = []
    if not problem.sum() == 0:
        raise ValueError(dfcull[problem])
        print("Bad value in Exclude column")
        
    else:
        df = df[df.Exclude.isnull()]
    df.loc[df.AVR.isnull(),"AVR"]=''
    df["CFS"] = df.CFS.str.replace("CFS","").str.replace("cfs","").str.lstrip("0")
    df.CFS.to_csv("test_cfs.csv")
    df.loc[df.CFS.isnull(),'CFS']='nan'
    df.loc[df.CFS.str.replace(" ","") == '','CFS'] = 'nan'
    print(df.CFS)
    df["CFS"]=df.CFS.astype(np.float64)

    
    
    for ndx,row in df.iterrows():
        t,u = ndx
        s = row.AVR
        o = row.Status
        q = row.CFS
        x = row.Exclude
        
        # todo: eliminated time parsing ... are there any 2400 times that won't be handled well?
        unitno=int(u[4:])
        print(t,u,o)
        if "," in o:
            op = o.split(",")[0]
        else: 
            op = o
        try:
            qop = float(q)
        except:
            qop = np.nan
            print("Got q=",q)            
        
        pdata.append((t,unitno,op,s,qop))
    

    
    unit_status = [0]*12      
    pdata.sort()
    pfile = open("pdata.txt","w")
    for d in pdata:
        pfile.write("%s %s %s %s %s\n" % d)
    pfile.close()            
    
    t0 = pdata[0][0]
    d_old = (None,None)
    flow_old = 0.
    tdata = []
    for d in pdata:
        dtx = d[0]
        duplicate_time = dtx==d_old[0]
        unitno = d[1]
        op = d[2]
        stat = d[3]
        if op.endswith(","): op = op[:-1]
        if not op in ops_status.keys():
            raise ValueError(f"Op unknown: {dtx} {unitno} op={op} stat={stat}")
        dq, unit_status = flow_change(rate_version,unitno,op,unit_status,stat)

        #if op == 'S/D' : print unit_status, unitno
        if dtx == t0: dq = max(dq,0.)  #initial out of service marker is status, not a change
        flow = flow_old + dq
        q = d[4]
        if q and q != '' and (not np.isnan(q)) and abs(flow-float(q)) > 200:
            print("%s Calc=%s Note=%s" % (dtx,flow,q))
        if dq != 0.0:
            tdata.append((dtx,flow,unitno))
        flow_old = flow
        d_old = d

    oldval = None
    udata = []
    for val in tdata:
        t = val[0]
        newval = (t,val[1])
        if oldval and t == oldval[0]:
            oldel = udata.pop()
            v = oldel[1]
        udata.append(newval)
        oldval = val

    utimes = pd.DatetimeIndex(data=[x[0] for x in udata])
    uts = pd.Series(index = utimes,data= [x[1] for x in udata])
    return uts


def augment_with_dss(ts,stime,etime,interval,banks_dss,path):
    banks_rts = interpolate_ts(ts,interval,method=PREVIOUS)
    if banks_dss:
        dss_rts = dss_retrieve_ts(banks_dss,path,(stime,etime+days(2)))
        print("DSS Banks file end: %s" % dss_rts.index[-1])
        dss_rts = interpolate_ts(dss_rts,interval,method=PREVIOUS)
        dss_rts = gaussian_filter1d(dss_rts.data,sigma=20,mode='nearest',order=0)
        udata = ts_merge([banks_rts,dss_rts])
    else:
        udata=banks_rts
    udata=udata.window(stime,etime+days(1))
    return udata
        
    
def finalize_and_compare(banks_rts,bbid_cfs,qbanks_cdec_daily,stime,etime,interval):
    """ Sanity check, plotting and output writing
        This routine is needed to 

    Parameters
    ----------
    series  :  tuple(:class:`DataFrame <pandas:pandas.DataFrame>`) or tuple(:class:`DataArray) <xarray:xarray.DataArray>`
        Series ranked from hight to low priority              
    Returns
    -------    
    merged : :class:`DataFrame <pandas:pandas.DataFrame>`
        A new time series with time interval same as the inputs, time extent
        the union of the inputs, and filled first with ts1, then with remaining
        gaps filled with ts2, then ts3....

    """
    fig,(ax0,ax1) = plt.subplots(2,sharex=True)
    unitheader_cms = "PST,'DISCHARGE, PUMPING (cms)'"
    unitheader_cfs = "PST,'DISCHARGE, PUMPING (cfs)'"
    banks_rts[:]=gaussian_filter1d(banks_rts,sigma=2,mode='nearest',order=0) # label = "Banks"
    banks_rts.plot(drawstyle="steps-post",color="b",ax=ax0)
    ax0.legend(["Banks processed"])

    qbanks_cdec_daily.plot(drawstyle="steps-post",color="r",ax=ax0)    # ,label="Banks CDEC daily"
    ts_ave = banks_rts.resample(days(1)).mean()[stime:etime]
    ts_ave.plot(drawstyle="steps-post",color="g",ax=ax0) #,label="Banks Daily"

    print("End of data for CDEC: {} and Banks data from script:  {}".format(qbanks_cdec_daily.index[-1],banks_rts.index[-1]))

    qbanks_cfs = banks_rts[stime:etime]
    qbanks_cfs.name='flow'
    qbanks_cms = qbanks_cfs*CFS2CMS
    qbanks_cfs.name='flow'
    
    write_csv("prepared_banks_cms.csv",qbanks_cms,{"station": "Banks","unit": "f^3s^-1"})
    write_csv("prepared_banks_cfs.csv",qbanks_cfs,{"station": "Banks","unit": "m^3s^-1"})    

    qbbid_cfs = bbid_cfs[stime:etime]
    qbbid_cms = qbbid_cfs*CFS2CMS
    dx0 = pd.infer_freq(qbanks_cfs.index)
    dx1 = pd.infer_freq(qbbid_cfs.index)
    
    print("Length of bbid: %s qbanks: %s" %(len(qbbid_cfs),len(qbanks_cfs)))
    print("Interval of bbid: %s qbanks: %s" % (dx0,dx1))
    print("Banks time series start: %s " % qbanks_cfs.index[0])
    assert qbanks_cfs.index[0] == qbanks_cms.index[0]
    print("Banks time series end: %s" % qbanks_cfs.index[-1])
    assert qbbid_cms.index[0] == qbanks_cms.index[0],\
       "BBID and Banks start dates don't match: %s %s" % (qbbid_cms.index[0],qbanks_cms.index[0])
    assert qbbid_cms.index[-1] == qbanks_cms.index[-1],\
       "BBID and Banks end dates don't match: %s %s" % (qbbid_cms.index[-1],qbanks_cms.index[-1])
       
    sap_vs_cdec = pd.concat([ts_ave,qbanks_cdec_daily],axis=1)
    sap_vs_cdec.columns=["processed","cdec"]
    banks_diff = sap_vs_cdec.diff(axis=1)
    print(sap_vs_cdec)
    print("Calculating difference between banks daily data on CDEC and processed flows that have been daily averaged")


    banks_diff.plot(linewidth=1.8,color='orange',ax=ax1)
    print("Banks difference time series start %s end %s " % (banks_diff.index[0],banks_diff.index[-1]))
    print("CDEC averaged banks end: %s " % ts_ave.index[-1])
    #ax1.legend(["Banks CDEC daily","Banks processed daily","Diff"])
    plt.show()
    
    print("Adding BBID and Banks to get exports.")
    banks_diff = banks_diff[stime:etime]   
    print(qbbid_cms)
    print(qbanks_cms)
    
    exports_cms = qbbid_cms.add(qbanks_cms,axis=0)
    exports_cfs = qbbid_cfs.add(qbanks_cfs,axis=0)        
    print("Writing exports for %s to %s" % (exports_cms.index[0],exports_cms.index[-1]))
    write_csv("prepared_exports_cms.csv",exports_cms,meta={"station": "swp+bbid","units":"m^3s-1"})
    write_csv("prepared_exports_cfs.csv",exports_cfs,meta={"station": "swp+bbid","units":"m^3s-1"})
    print("Calculating difference between Banks time series as calculated and CDEC")
    print("Filling in qbanks_cdec_daily for nan values with calculated so there will be no difference")
    
    assert banks_diff.index[0] == exports_cfs.index[0]
    assert banks_diff.index[-1] == exports_cfs.index[-1]
    print("Writing out difference in banks estimate, same time span, units are cfs")
    write_csv("prepared_banks_diff_cfs.csv",banks_diff)
    print("All data written")
   
def dparse(x,y):
    from dateutil.parser import parse
    print(x,y)
    z = x[0]
    y = x[1]
    dstring = "{} {}:{}".format(z,y[0:2],y[2:4])
    print(dstring)
    return parse(dstring)
    
def main():
    """Read and smooth BBID data, possibly folding together with DSS file"""
    stime = dtm.datetime(2007,10,1)
    etime = dtm.datetime(2022,1,3)
    interval = minutes(15)
    # Are we using DSM2, perhaps for future part of operational forecast?
    #bbid_dss_dicu = None
    bbid_csv = "./data/cdec_download/cdec_bbi_bbi_flow_2007_2022.csv"
    
    bbid_cfs = process_bbid(bbid_csv,interval,stime,etime)
    


    banks_xls = "data/sap_banks_2006_2022_edit.xlsx"
    rversion = DFD  # version of the rating to turn pumping unit status into cfs
    

    
    banks_df = pd.read_excel(banks_xls,header=0,dtype={"Date": str,"Time":str,"Exclude":str,"AVR":str,"CFS":str})
                             #parse_dates=[[5,0]],date_parser=dparse,index_col=("Date_Time","Unit"))
                             
    banks_df["Datetime"] = pd.to_datetime(banks_df.Date.str.slice(0,11) +" " + banks_df.Time.str.slice(0,2) + ":" + banks_df.Time.str.slice(2,4))   
    print(banks_df)    
    banks_df=banks_df.set_index(["Datetime","Unit"])[["Status","AVR","PSS","MWs","CFS","Notes","Exclude"]]
    
    banks_dss = None
    hro_daily_fname = "data/cdec_download/cdec_hro_hro_flow_2007_2022.csv"
    qbanks_cdec_daily = read_ts(hro_daily_fname,start=stime,end=etime)

    
    try:
        uts = parse_srr(banks_df,rversion)        
        if banks_dss:
            if not os.path.exists(banks_dss):
                raise ValueError("DSS file containing banks data does not exist: %s " % banks_dss)
            ts = augment_with_dss(uts,stime,etime,
                             interval,banks_dss,banks_path)
        else:
            ts = uts.resample(interval).fillna("pad")
        finalize_and_compare(ts,bbid_cfs,qbanks_cdec_daily,stime,etime,interval)
    except Exception as e:
        print("Parse failed for file %s (or DSS had problem)" % banks_xls)
        print(e)
        raise

if __name__ == '__main__':
    main()
        
        



