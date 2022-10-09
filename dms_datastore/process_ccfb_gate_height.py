#!/usr/bin/env python
import datetime as dtm
import os
import numpy as np
import schimpy.unit_conversions as units
import matplotlib.pylab as plt
from dms_datastore.read_ts import *
from shutil import copyfile
import numpy as np
import pandas as pd
import pytz

""" Preprocess CCF data from Wonderware to dated SCHISM format
1.	Get the latest gate open and closure height file from Wonderware but only for the gap between the last forecast and this one -- don’t overwrite data that has already been corrected for DST. Wonderware is well behaved, and it will be assumed here that this file is complete and needs no backup plan. You can log in at https://csbis.water.ca.gov.  You will use your water domain username and password. 

2. Select Query from the initial menu and then the SQL tab. You eventually will want to change the start time (the one with DateTime >=) but you don't need to do it every time and some overlap helps prevent errors when integrating old and new data.

This is the query -- you will need to cahnge
SET QUOTED_IDENTIFIER OFF
SELECT * FROM OPENQUERY(INSQL, "SELECT DateTime = convert(nvarchar, DateTime, 20),[DTHST.CCFB_GATE01.POS_FT],[DTHST.CCFB_GATE02.POS_FT], [DTHST.CCFB_GATE03.POS_FT], [DTHST.CCFB_GATE04.POS_FT], [DTHST.CCFB_GATE05.POS_FT]
FROM WideHistory
WHERE wwRetrievalMode = 'Delta'
AND wwVersion = 'Latest'
AND DateTime >= '20220101 00:00:00.000'
AND DateTime <= '20220514 00:00:00.000'")

2.	Save. The save button makes csv in wonderware. I use ccf_gate_height_wonderware_2022_9999.csv 

3. The preprocessor will correct the file for DST, match date/number format to the one used for schism (2009-01-31 00:00) and append to the existing. It will also trim redundant entries and reformat. Run process_ccfb_gate_height. At that point,you would append to prior files in GitHub and in Modeling_Data. We tend to find problems and fix them. 

The dataprep work area no longer has the entire history of CCF, because this involvoes onerous switching between methods in the pre-Wonderware/SAP era. 

todo: 
1. Automate the appending of this data to an original and of a forecast to this file.
2. Automate the estimation of gate heights given irregular timing (e.eg. priority 3) and a file of SWP pumping flows. There is a function doing this approximation in this file but it isn't hooked up anymore.

"""


def read_wonderware(infile):
    """ Read a gate height file from Wonderware and convert to PST
    
    Parameters
    ----------
    infile : str
    path to the Wonderware file
    """
    
    ts = pd.read_csv(infile,sep=",",parse_dates=["DateTime"],
                     index_col="DateTime",dtype=float,na_values=["(null)"])    
    pst = pytz.timezone('ETC/GMT+8')
    pdt = pytz.timezone('US/Pacific')


    ts.index = ts.index.tz_localize(pdt).tz_convert(pst)
    ts.index = ts.index.floor("1T")
    ts = ts[~ts.index.duplicated(keep="first")]
    ts.index = ts.index.tz_localize(None)
    return ts


def ccf_trim(df,outfile):
    """ Trim a dataframe containing gate heights to eliminate near-duplicates """
    gatecols = ["gate01","gate02","gate03","gate04","gate05"]
    df.columns = gatecols
    df = df.astype(float)
    # Get rid of duplicate indexes (less than 1 second apart)
    df = df[~df.index.duplicated(keep='first')]
    df.sort_index(inplace=True)
    # Trim near-duplicate times and then duplicate data
    df2 = df.asfreq('2min',method='pad')

    df2=df2.apply(np.round, args=[2])
    df2=df2.loc[~df2.isnull().any(axis=1)]

    # Workaround to a problem in drop_duplicates()
    if (df2.iloc[0,:] == 0.).all():
        df2.iloc[0,4]=1.e-6
    if (df2.iloc[-1,:] == 0.).all():
        df2.iloc[-1,3]=1.e-6

    df2.drop_duplicates(inplace=True,keep='first')
    

    df2.to_csv(outfile,float_format="%.4f")
    return df2


# todo: changed from 5 to 1
no_gates_used = 2

def write_ccf_th(fname,df):
    #datetime      install  ndup  op_down   op_up     elev	  width	   height
    df.index.name = "datetime"  
    df["elev"] = "-4.0244"
    df["width"] = "6.096"
    df["op_down"] = "1.0"
    df["op_up"] = "0.0"
    df["ndup"] = df.ndup.astype(int)
    df["install"] = int(1) 
    df[["install","ndup","op_down","op_up","elev","width","height"]].to_csv(fname,sep=" ",float_format="%.3f",date_format="%Y-%m-%dT%H:%M")
    



################ Below here until the next set of ### is detritus that may be useful for infering gates from flow 
################ Older versions of process_ccfb_gate_height are potentially useful for seeing the way the 
################ longer history has been synthesized, but probably are not too useful
    

def ccfb_th_line(tm,nduplicate,height):
    """ Create a time history line by filling in values that are always the same"""
    ngvd_m=0.70
    datestr = tm.strftime("%Y-%m-%dT%H:%M")
    install = 1
    op_down = 1.0
    op_up   = 0.0
    width   = units.ft_to_m(20)
    elev    = units.ft_to_m(-15.5) + ngvd_m
    if height < 0.0:
        raise ValueError("Negative gate height not allowed")
    if height == -0.000:
        height = 0.0
    # Gets rid of unsightly "-0.000" entries
    startline = "{0} {1} {2} {3} {4} {5} {6} {7:6.3f}\n"
    finalline = startline.format(datestr,install,int(nduplicate),\
                                 op_down,op_up,elev,width,abs(height))
    return finalline

def write_th(ts,thfile):
    """Convert a csv file with date,heights to an irregular time series"""
    print("out",thfile)
    with open(thfile,"w") as outfile:
        for el in ts:
            nduplicate = el.value[0]
            aveheight = el.value[1]
            outfile.write(ccfb_th_line(el.time,nduplicate,aveheight))    
    
    
def invert_flow(flow,x1,x2):
    """ Invert a flow into a gate height with no information on up/down stage 
        Formula is x2*h*h + x1 *h - flow = 0
    """
    safe_flow = np.minimum(np.maximum(0.,flow.data),(x1*x1)/(-4.*x2))
    m = np.sqrt(x1*x1 + 4.*x2*safe_flow)
    hgate = 0.*flow
    hgate_minus = (-x1-m)/(2.*x2)
    hgate_plus = (-x1+m)/(2.*x2)
    hgate.data = np.minimum(hgate_minus,hgate_plus)
    hgate.data = np.minimum(16.5,hgate.data)
    return hgate    

   

   
def read_gate_height(datafile,thresh_open=0.03):
    """Convert a csv file with date,heights to a bivariate irregular time series of #duplicates and height
       thresh_open is the threshold at which gate is considered in use, which can filter a lot of noise
    """
    lastmin=-1
    df = pd.read_csv(datafile,sep=",",index_col=0,parse_dates=[0],header=0,na_values=["(null)"])
    height_sum = df.sum(axis=1)  # don't move this without care
    df["ndup"] = (df>thresh_open).sum(axis=1)
    df["height"] = height_sum.divide(df.ndup,axis="index",fill_value=0.)*units.FT2M
    df.loc[df.ndup == 0,'height'] = 0.
    return df[["ndup","height"]]
   

def combine_heights(heights, height_q,forced=[]):
    """ Merge two irregular series based on whether they cover the same day.
        If the first series has entries in a day, it bumps the second. 
        Dates in forced are ones in which the second priority is used regardless of coverage by the first
        #todo: Should have generically named args and be moved to VTools
    """
    if len(forced) > 0:
        if type(forced[0] == dtm.datetime):
            forced = [d.date() for d in forced]
    series_vals={}
    days_covered = set()
    for ht in heights:
        if np.isnan(ht.value[0]): continue
        if not ht.time.date() in forced: 
            days_covered.add(ht.time.date())
            series_vals[ht.time] = ht.value

    for htq in height_q:
        htqdate = htq.time.date()
        if not htqdate in days_covered:
            series_vals[htq.time] = (no_gates_used,htq.value)
        
    times = series_vals.keys()
    times.sort()
    vals = []
    for t in times:
        vals.append(series_vals[t])
    
    data = np.array(vals)
    gate_ts = its(times,data)
   
    return gate_ts
    

                    
      
def timing_data(stime,etime):
    source = "data/forecast.dss"
    select = "B=CHWST000,C=POS,F=DSM2-20150901-91A"
    usewindow = (stime,etime)
    ts=dss_retrieve_ts(source,select,usewindow,unique=True)
    return ts 

def heights_inverted_hourly(hills_csv):
    """ Processes a csv file of stages and Hills flows into a gate height.
        The Hills Eq are very inaccurate, but this inversion can help in cases when in/out stage
        and a calculated Hills flow were retained as data but the actual gate heights were not.
        All five gates are assumed to be operated equally.
        
        The lines of the csv file look like this: Datetime, up stage, down stage, Hills-calculated flow
        2008-01-01 00:00,0.004,-0.494,5597.588
    """
    heights=[]
    times=[]
    with open(hills_csv,"r") as f:
        for line in f:
            if line and len(line) > 4:
                d,zup,zdown,flow = line.strip().split(",")
                timestamp = dtm.datetime.strptime(d,"%Y-%m-%d %H:%M")
                height = invert_hills_flow(float(flow),float(zup),float(zdown))
                heights.append(height)
                times.append(timestamp)
    ts = its(times,heights)
    return its2rts(ts,hours(1))


#################
#################





def create_arg_parser():
    # Argument parsing not really ready yet
    import argparse
    parser = argparse.ArgumentParser(
        description='Convert a csv file with date and five CCFB heights to *.th file.')
    parser.add_argument('--infile', type=str,
                        help='name of the input csv file containing CCFB heights from wonderware')
    parser.add_argument('--basefile',type=str,default=None,
                        help='name of prior file to append')
    parser.add_argument('--transition',type=lambda x: pd.to_datetime(x),default=None,
                        help='Date of transition. This is first day to start using the new data. Default is to truncate the last day of the base file to midnight so any improvements in that file are retained')
    parser.add_argument('--appendfile',type=str,default=None,
                        help='name of prediction file to append')
    parser.add_argument('--append_transition',type=lambda x: pd.to_datetime(x),
                        default=None,
                        help='Date of transition to secondary file for predictions. This is the first time to use the prediction data. Default is to use the last moment of the new data')
    parser.add_argument('--outfile', type=str,default="",
                        help='name of output *.th file')
    parser.add_argument('--preprocess', type=bool,default=True,
                        help='preprocess Wonderware file for time zone and sparsity')
    parser.add_argument('--prepro_out',type=str,help="name of intermediate output file for preprocessor",default="ccf_prepro.csv")
    return parser
    
def main():
    parser = create_arg_parser()
    args = parser.parse_args()
    infile=args.infile
    thfile = args.outfile
    preprocess = args.preprocess
    intermediate = args.prepro_out 
    basefile = args.basefile
    transition = args.transition
    appendfile = args.appendfile
    append_transition = args.append_transition
    # Check that prepend and append files exist
    if basefile == thfile:
        raise ValueError("basefile and outfile must be different")
    if basefile is not None:
        if not os.path.exists(basefile): 
            raise ValueError(f"If used, basefile ({appendfile}) must exist as a file")
    if appendfile == thfile:
        raise ValueError("appendfile and outfile must be different")
    if appendfile is not None:
        if not os.path.exists(appendfile): 
            raise ValueError(f"If used, appendfile ({appendfile}) must exist as a file")


    ## start work
    if preprocess:
        ts = read_wonderware(infile)
        ccf_trim(ts,intermediate)
        infile = intermediate
    # Read in heights directly and return a time series of heights
    height_ts = read_gate_height(infile)

    if basefile is not None:
        print(f"prepending {basefile}")
        backup = f"ccf_pre_base_backup_{pd.Timestamp.now().strftime('%Y%m%d%H%M')}.csv"

        print(f"creating backup {backup}")
        copyfile(basefile,backup)
        existing = pd.read_csv(basefile,sep="\s+",index_col=0,comment="#",
                               parse_dates=[0],header=0)[["ndup","height"]]
        
        if transition is None: 
            transition = existing.last_valid_index().floor('D')
        height_ts = pd.concat((existing.loc[slice(None,transition),["ndup","height"]],
                               height_ts.loc[slice(transition,None),["ndup","height"]]),axis=0) 
        duplicatetime = height_ts.index.duplicated(keep='first')
        nduplicatetime = duplicatetime.sum()
        print(f"nduplicatetime for prepending {nduplicatetime}")
        if nduplicatetime > 1: 
            print("base grafting process created more than one duplicate")
        else:
            if nduplicatetime == 1:
                height_ts = height_ts[~duplicatetime]
    if appendfile is not None:
        print(f"appending {appendfile}")
        pred  = pd.read_csv(appendfile,sep="\s+",index_col=0,comment="#",
                            parse_dates=[0],header=0)[["ndup","height"]]
        if append_transition is None: 
            append_transition = height_ts.last_valid_index()
            print(f"Append transition time {append_transition}")
        height_ts = pd.concat(
               (height_ts.loc[slice(None,append_transition),["ndup","height"]],
                pred.loc[slice(append_transition,None),["ndup","height"]]),axis=0) 
        duplicatetime = height_ts.index.duplicated(keep='first')
        nduplicatetime = duplicatetime.sum()
        print(f"nduplicatetime for appending {nduplicatetime}")
        if nduplicatetime > 1: 
            print("appending process created more than one duplicate")
        else:
            if nduplicatetime == 1:
                height_ts = height_ts[~duplicatetime]
    print("writing")
    print(height_ts.last_valid_index())
    write_ccf_th(thfile,height_ts)

        

        
if __name__=="__main__":
    main()    
    
    