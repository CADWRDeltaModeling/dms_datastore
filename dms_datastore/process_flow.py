#!/usr/bin/env python
# -*- coding: utf-8 -*-

import matplotlib.pyplot as plt
from vtools.functions.interpolate import *
from vtools.functions.filter import *
from vtools.data.vtime import *
import os.path as osp
from dms_datastore.read_ts import *
import datetime as dtm  
import schimpy.unit_conversions as units
from schimpy.unit_conversions import ec_psu_25c,CFS2CMS,CMS2CFS
from scipy.ndimage.filters import gaussian_filter1d
from vtools.functions.error_detect import *
import pandas as pd
import pyhecdss

do_plots=True

alt="DWR-DMS-201203"
flux_ndx = {"coyote":0,"ccc_rock":1,
            "ccc_old":2,"swp":3,"cvp":4,"sjr":5,
            "calaveras":6,"east":7,"american":8,"sac":9,"yolo_toedrain":10,"yolo":11,
            "northbay":12,"napa":13,"ccc_victoria":14}
flux_labels=flux_ndx.keys()
print("flux labels")
print(flux_labels)

# 3 0 1 0 2 ! Coyote
# 4 0 1 0 0 !Rock Sl. 
# 2 0 1 0 0 !CCWD: Old R.
# 4 0 1 0 0 !SWP
# 4 0 1 0 0 !CVP
# 4 0 1 0 1 !San Joaquin
# 4 0 1 0 2 !Calavaras R.
# 3 0 1 0 2 !Cosum.+ Moke. R.
# 5 0 1 0 1 !Sacramento
# ?         ! Yolo toedrain 
# 3 0 1 0 1 !Yolo
# 4 0 1 0 0 !North Bay
# 3 0 1 0 2 ! Napa
# 3 0 1 0 0 ! ccc_victoria

def read_csv_date(file):
    f=open(file,"r")
    times=[]
    vals=[]
    for line in f.readlines()[1:]:
        if line and len(line)>2:
            dstr,vstr=line.strip().split(",")
            times.append(dtm.datetime.strptime(dstr,"%d-%b-%y"))
            vals.append(float(vstr))
    f.close()
    ts=its(times,vals)
    return its2rts(ts,days(1))


    

def read_pd(fname):
    ts = pd.read_csv(fname,index_col=1,parse_dates=[1],header=0,comment="#",na_values="M")
    ts.columns = [x.strip() for x in ts.columns]
    val = ts[['Value']].as_matrix()
    times = ts.index.to_pydatetime()
    tsout = its(times,val)
    tsout = its2rts(tsout,minutes(15))
    tsout.data = tsout.data.flatten()
    return tsout    
    
    
def describe_null(dset,name):
    print(f"null for {name}")   

    if dset.isnull().values.any(): 
        try:
            isnan = dset.isnull().any(axis=1)
            intnan = isnan.loc[dset.first_valid_index():dset.last_valid_index()]
            nans = dset.loc[isnan,:]
            print("dataframe")
        except:
            isnan = dset.isnull()
            intnan = isnan.loc[dset.first_valid_index():dset.last_valid_index()]
            nans = dset[isnan]
            print("series")
        interiornan = intnan.sum(axis=0)
        count = isnan.sum(axis=0)
        print("Count: {} Interior: {}".format(count,interiornan))
        print(nans)
    else: print("None")
    
sdate=pd.Timestamp(2008,1,1)
edate=pd.Timestamp(2022,1,1)
max_extrap_exports = 10300  # number of data points at destination freq (e.g. 15min) to extrapolate cdec for ccc_old and ccc_rock
interval = minutes(15)
buffer = days(5)
lag_sac = hours(3)

cdec_dir = "data/cdec_download"
usgs_dir="Data/usgs_download"
wdl_dir = "Data/wdl_download"
dsm2_file="D:/Delta/dsm2_v8/timeseries/hist_19902012.dss"
alt="DWR-DMS-201203"


print("moke and cosumnes")
# Michigan bar
usgs_file="usgs_mhb_11335000_flow_2007_2022.rdb"
#todo: removed buffer. Does it work?
usgs = read_usgs1(osp.join(usgs_dir,usgs_file),start=sdate,end=edate+hours(2),selector="15453_00060").squeeze()
usgs.loc["2008-07-04T23:00":"2008-07-09T00:00"] = 22.3 

#usgs2 = usgs.replace(22.3,dtm.datetime(2008,7,4,23),dtm.datetime(2008,7,9))
usgs.interpolate(limit=300,inplace=True)
# todo: safe replacement of single long gap 
#usgs = cosine_lanczos(usgs,hours(40)).window(sdate,edate)
#usgs = cosine_lanczos(usgs,hours(40)).window(sdate,edate)
describe_null(usgs,"michigan bar gap")


# todo: this seems to be missing in the downloads, but it won't have evolved anyhow since they only 
# update annually
usgs_file="q_woodbridge.rdb"


#usgs2 = read_ts(osp.join(usgs_dir,usgs_file),selector="228917_00060_00003")
usgs2 = pd.read_csv(osp.join(usgs_dir,usgs_file),comment="#",sep="\s+",parse_dates=['datetime'],index_col='datetime').squeeze()
usgs2 = usgs2["228917_00060_00003"]
usgs2 = usgs2.interpolate(limit=80)  # todo: this is a lot but it is OK in existing gaps as of 2021-05-22
usgs2 = usgs2.to_period(freq="D").to_frame()
usgs2 = rhistinterp(usgs2+1.0,interval,lowbound=0.0,p=4.,maxiter=4) - 1.0
usgs2 = usgs2.clip(lower=0.0)
usgs2 = usgs2[sdate:edate]

ebmud_file = "woodbridge_ebmud.csv"
moke2 = pd.read_csv(osp.join("data",ebmud_file),sep=",",
                    parse_dates=[0],index_col=0,
                    dtype={"Discharge":float},usecols=["Date","Discharge"]).squeeze()
moke2 = moke2.to_period(freq="D")
moke2 = rhistinterp(moke2+1.0,interval,lowbound = 0., p=1.,maxiter=4)-1.
moke2 = moke2.clip(lower=0.0)


cdec_file = "cdec_cmn_*flow_*.csv"
cmn = read_ts(osp.join(cdec_dir,cdec_file)).squeeze()
cmn = cmn.resample(interval).interpolate(method="pad",limit=20)
cmn = med_outliers(cmn,range=(-1,100000),scale=10000)
cmn = cmn.interpolate(limit = 60)

usgs2.columns = ["moke"]
moke2.columns = ["moke"]
cmn.columns = ["moke"]
moke = ts_merge([x.squeeze() for x in [usgs2,moke2,cmn]])

moke.columns = ["east"]  # prep for merge
usgs.columns = ["east"]  # prep for merge
east=usgs.add(moke,axis=0)
east = east[sdate:edate].to_frame()
east.columns=["east"]
flux = -units.CFS2CMS*east

describe_null(east,"East")





print("american")
#am_file = os.path.join("data/usgs_american_request","11446500_fair_oaks_discharge.csv")
#am = read_pd(am_file)
#am = its2rts(am,minutes(15))

am_file = osp.join(usgs_dir,"usgs_afo_11446500_flow_2007_2022.rdb")
am = read_ts(am_file)
am = am.interpolate()  # todo: Check this no significant gaps are missed
am = am[(sdate-lag_sac):(edate+lag_sac)]  # Buffer because will be subtracted from Sac and then Sac will be lagged
flux["american"] = -units.CFS2CMS*am[sdate:edate]
describe_null(am,"American")

print("sac")
# Freeport flow does not include Sac Regional SAN flow
# Reason for this is that for biogeochemistry it is a source and requires water+constituent
free_usgs_file ="usgs_fpt_11447650_flow_2007_2022.rdb"
selector="sensor=236032_00060",
free_usgs = read_ts(osp.join(usgs_dir,free_usgs_file))
#print(free_usgs["2008-09-01":"2008-09-02"])
#describe_null(free_usgs,"first")
print("freq",free_usgs.index.freq)
#free_usgs = free_usgs.resample(minutes(15)).interpolate(limit=10)
free_usgs = free_usgs.interpolate(limit=20)


free_usgs = cosine_lanczos(free_usgs,hours(40))[sdate-buffer:edate+buffer]
free_usgs = free_usgs.interpolate() # todo: risky


describe_null(free_usgs,"free_usgs")
free_usgs_backup = "W:/usgs_scalar_to_oct_2013/QAVE.UV.USGS.11447650.2.C.00000000.rdb"
free_usgs_backup = "W:/continuous_station_repo/raw/incoming/usgs/11447650/Discharge.ft^3_s.(15_min_ave_VELQ).velq@11447650.EntireRecord.csv"
free_usgs_bk = read_ts(free_usgs_backup)
free_usgs_bk = free_usgs_bk.resample(minutes(15)).interpolate(limit=10)
free_usgs_bk = free_usgs_bk.interpolate(limit=64)
free_usgs_bk = cosine_lanczos(free_usgs_bk,hours(40))[sdate-buffer:edate+buffer]
#describe_null(free_usgs_bk,"bk")


free_usgs_r = pd.read_csv("fpt_subtide_r_20210601_15min.csv",header=None)*100.*CMS2CFS
ndx = pd.date_range(start=pd.Timestamp(2021,6,1),freq='15T',periods=len(free_usgs_r))
free_usgs_r.index=ndx
print("length of R")
print(free_usgs_r.index[-1])
free_usgs.columns = ["fpt"]  # prep names for update()
free_usgs_r.columns = ["fpt"]
free_usgs.update(free_usgs_r)


# poor labels here. The "backup" actually has the long term data in better form
# the free_usgs has more recent, but has bad values. backup is hourly
#sac = ts_merge([free_usgs.squeeze(),free_usgs_bk.squeeze()])[sdate:edate]
sac = free_usgs

describe_null(sac,"Sacramento")

#ax=free_usgs.plot()
#free_usgs_bk.plot(ax=ax)
#plt.show()

# some are missing in 2020. Sum SUT, SSS, SDI? 

# now subtract off the american river flow
sac = sac.sub(am.squeeze(),axis=0)

if (sac.lt(0.)).values.any(axis=0): 
    # Don't want to generate negative Sac flow
    raise ValueError("American flow can't be greater than Freeport")

# and lag 3 hours
# note that the final window has to be AFTER the lag
sac_final = sac.shift(-3,hours(1),)[sdate:edate]
flux["sac"] = -units.CFS2CMS*sac_final
describe_null(sac_final,"sac")

cache = read_ts(osp.join(usgs_dir,"q_cache.rdb"))   # old and unchanging
cache = cache.interpolate(limit=4)
cache = cosine_lanczos(cache,hours(40))
cache.columns = ["value"]

cache_ryer = read_ts(osp.join(usgs_dir,"usgs_rye_11455385_flow_2018_2022.rdb"))
cache_ryer = cache_ryer.interpolate(limit=4)
cache_ryer = cosine_lanczos(cache_ryer,hours(40))
cache_ryer.columns=["value"]
cache = ts_merge([cache_ryer,cache])


# Miner slough is hwb
miner = read_ts(osp.join(usgs_dir,"usgs_hwb_11455165_flow_2007_2022.rdb"))
miner = miner.interpolate(limit=4)
miner = cosine_lanczos(miner,hours(40))
yolo_south = cache.sub(miner.squeeze(),axis=0)
# note that the step is 15min but the low pass is 15d
yolo_south_15d = cosine_lanczos(yolo_south,days(15))

print("yolo toedrain")
path="//BYOLO040/FLOW//1DAY/DWR-DMS-201203/"
dssdata = pyhecdss.get_ts(dsm2_file,path)

for i,data in enumerate(dssdata):
    assert i==0
    yolo1 = data[0]

yolo1 = rhistinterp(yolo1+100.,interval,lowbound=0.0,p=12.) - 100.
yolo1 = yolo1.clip(lower=0.0)
yolo1 = yolo1.fillna(0.)[sdate:edate]

# This is the "mod11" (best) fit to data in 2016 for Mercury project
yolo2016 = pd.read_csv("data/yolo_project_2016/yolo_project_2016.th",sep="\s+",header=0,parse_dates=[0],index_col=0)
yolo2016 = yolo2016.yolo

path = "ncro_lis_b91560q_flow_2007_2022.csv"
lisbon1 = read_wdl2(osp.join(wdl_dir,path))
tw2 = lisbon1[dtm.datetime(2013,1,9,6):dtm.datetime(2013,1,9,10)]
lisbon1 = med_outliers(lisbon1,range=(-1000.,5000.),scale=50.)
lisbon1 = lisbon1.interpolate(limit = 20)[sdate:edate]
# This is the same data, but NCRO data doesn't extend all the way to real time
path = "cdec_lis_*flow_2007_2022.csv"
lisbon = read_ts(osp.join(cdec_dir,path))
lisbon = med_outliers(lisbon,range=(-1000.,5000.),scale=50.)
lisbon = lisbon.interpolate(limit = 20)[sdate:edate]

patch1 = yolo_south[dtm.datetime(2010,1,19):dtm.datetime(2010,2,18)]
patch2 = yolo_south_15d[dtm.datetime(2010,12,17):dtm.datetime(2011,1,13)]
patch3 = yolo_south[dtm.datetime(2011,3,19):dtm.datetime(2011,3,28)]
patch4 = lisbon[dtm.datetime(2008,1,26,16):dtm.datetime(2008,2,15,8)]
patch5 = lisbon[dtm.datetime(2008,4,28,5):dtm.datetime(2008,5,20,12)]
patch6 = lisbon[dtm.datetime(2008,1,2,16):dtm.datetime(2008,1,7,7)]
patch7 = yolo1[dtm.datetime(2011,3,31,12):dtm.datetime(2011,4,12)]
patch8 = yolo_south_15d[dtm.datetime(2012,12,22):dtm.datetime(2013,1,10)]
patch9 = lisbon[dtm.datetime(2014,12,18):dtm.datetime(2015,1,1)]

ts_patch = ts_merge([x.squeeze() for x in [patch1,patch2,patch3,patch4,patch5,patch6,patch7,patch8,patch9,yolo2016,lisbon1,lisbon]])
ts_patch = ts_patch.interpolate()

toe_base = ts_patch.copy()
toe_base = toe_base.clip(upper=3000.)
toe_extra = (ts_patch - toe_base)*0.05
toe_extra = toe_extra.clip(lower=0.)
toe_final = toe_base.add(toe_extra,axis=0)
toe_final = toe_final[sdate:edate]
yolo = ts_patch.sub(toe_final,axis=0)
#yolo.data[yolo.data < 0.] = 0.
yolo = yolo[sdate:edate]
flux["yolo_toedrain"]=-units.CFS2CMS*toe_final
describe_null(toe_final,"Yolo Toe Drain")

flux["yolo"]=-units.CFS2CMS*yolo
describe_null(yolo,"Yolo")





print("calaveras")
path = "cdec_nhg_*flow_2007_2022.csv"
ts_cdec = read_ts(osp.join(cdec_dir,path))
ts2 = ts_cdec
ts2 = ts2.resample(interval).interpolate(limit=20)
# This is a significant assumption that summer flows below 500cfs do not reach the Delta
# The principle has been confirmed by email, but it is a big reduction relative to Dayflow
ts2[ts2.lt(500.)] = 0.
ts2=ts2.fillna(0.)
#ts2 = rhistinterp(ts2+1.0,interval,lowbound=0.0,p=4,maxiter=20) - 10.0
flux["calaveras"] = -units.CFS2CMS*ts2
describe_null(ts2,"Calaveras")

#sjr_usgs_backup="W:/usgs_scalar_to_oct_2013/Q.UV.USGS.11303500.4.C.00000000.rdb"
#sjr_usgs_bk = read_ts(sjr_usgs_backup)
print("sjr")
vnl_usgs_file="usgs_vns_11303500_*flow_2007_2022.rdb"
usgs = read_ts(osp.join(usgs_dir,vnl_usgs_file)) #,selector="sensor=15169_00060")     
usgs = med_outliers(usgs,level=4,scale = 100.,filt_len=5,range=(10.,50000.))
usgs = usgs.interpolate(limit=500)

usgs = monotonic_spline(usgs,dest=usgs.index)[sdate:edate]
moss_file ="ncro_msd_b95820q_flow_2007_2022.csv"
mossdale = read_ts(osp.join(wdl_dir,moss_file))
mossdale = mossdale.interpolate(limit=20)
mossdale = cosine_lanczos(mossdale,hours(40))[sdate:edate]
ts15 = ts_merge([usgs.squeeze(),mossdale.squeeze()])[sdate:edate]
ax=usgs.plot()
mossdale.plot(ax=ax)
ts15.plot(ax=ax)
ax.legend(["Vernalis","Mossdale","Merged"])
plt.show()

#usgs = usgs.resample('15T').interpolate(limit=8)
#usgs = monotonic_spline(usgs,dest=minutes(15)) 
flux["sjr"] = -units.CFS2CMS*ts15
describe_null(ts15,"sjr")


print("north bay")

path = "cdec_bks_*flow_2007_2022.csv"
ts_cdec = read_ts(osp.join(cdec_dir,path),start=None,end=None).squeeze()
ts = ts_cdec.to_period(freq="D")
ts = ts.interpolate(limit=2) # as of 2022-01 there is only one missing value
ts2 = rhistinterp(ts+10,interval,lowbound=0.0,p=20.) - 10
ts2 = units.CFS2CMS*ts2.clip(lower=0.)
flux["northbay"]=ts2[sdate:edate]
describe_null(ts2,"Barker")



print("swp")
exports_file = "prepared_exports_cms.csv"
exports_sap = pd.read_csv(exports_file,sep=",",header=0,parse_dates=[0],index_col=0)

ts2=exports_sap
flux["swp"]=ts2[sdate:edate]


#todo: removed buffer from cvp, napa coyote
print("cvp")
path = "cdec_trp_*flow_2007_2022.csv"
ts_cdec = read_ts(osp.join(cdec_dir,path))
ts_cdec = ts_cdec.interpolate()
ts_cdec = ts_cdec.asfreq('D').to_period()
cvp = rhistinterp(ts_cdec+10.,interval,lowbound=0.0,p=6.,maxiter=10) -20.
cvp = cvp.clip(lower=0.0)[sdate:edate]
flux["cvp"]=units.CFS2CMS*cvp
describe_null(cvp,"cvp")

print("napa")
usgs_file="usgs_napr_11458000_flow_2007_2022.rdb"
usgs = read_ts(osp.join(usgs_dir,usgs_file))
usgs=usgs.resample(interval).interpolate(limit=10)[sdate:edate]
napa = usgs.fillna(0.)
flux["napa"] = -units.CFS2CMS*napa[sdate:edate]

print("coyote")
usgs_file="usgs_coycr_11172175_flow_2007_2022.rdb"
usgs = read_ts(osp.join(usgs_dir,usgs_file))
ts2=usgs.resample(interval).interpolate(limit=50)
ts2 = ts2.fillna(0.)
flux["coyote"] = -units.CFS2CMS*ts2


##############################
print("ccc rock")
dssdata = pyhecdss.get_ts(dsm2_file,'//CHCCC006/FLOW-DIVERSION////')
for i,data in enumerate(dssdata):
    assert i==0
    ccrock = data[0]
    ccrock = rhistinterp(ccrock+2.,interval,lowbound=0.0,p=12.) - 2
    ccrock = ccrock.clip(lower=0.0)
    ccrock = ccrock.fillna(0.)[sdate:edate]  
    ccrock.columns = ["value"]
    
path = "cdec_inb_*flow_2007_2022.csv"
ts_cdec = read_ts(osp.join(cdec_dir,path),start=None,end=None)
ts_cdec = ts_cdec.to_period().squeeze()
ts_cdec = ts_cdec.interpolate(limit=5)

ts_cdec = ts_cdec.clip(lower = 0.0) 
ts_cdec = rhistinterp(ts_cdec+5.,interval,lowbound=0.0,p=8.) - 5.
ts_cdec = ts_cdec.clip(lower=0.0)

# Confirm there are now no nans from beginning of cdec record through last good cdec
assert not ts_cdec.isnull().any(axis=None)  

# Now fill the preliminary period and extrapolate if requested
first_cdec = ts_cdec.first_valid_index()
last_cdec = ts_cdec.last_valid_index()
ts_cdec = ts_cdec.reindex(flux.index)
ts_cdec.loc[slice(None,first_cdec)] = 0.

if max_extrap_exports > 0:
    ts_cdec.loc[last_cdec:edate].ffill(limit=max_extrap_exports,inplace=True)
    # Re-assert with possibly fuller dates all the way to edate
    assert not ts_cdec.isnull().any(axis=None)

ts2 = ts_merge([ts_cdec.squeeze(),ccrock.squeeze()])

ts2 = ts2.interpolate(limit=6)
flux["ccc_rock"]=units.CFS2CMS*ts2
describe_null(ts2,"CCC Rock")

###############################
print("ccc_old")
dssdata = pyhecdss.get_ts(dsm2_file,"//ROLD034/FLOW-EXPORT//1DAY//")
idatacount = 0
for i,data in enumerate(dssdata):
    assert i==0
    ccold = data[0]
    ccold = rhistinterp(ccold+2.,interval,lowbound=0.0,p=12.) - 2
    ccold = ccold.clip(lower=0.0)
    ccold = ccold.fillna(0.)[sdate:edate]
    idatacount += 1
assert idatacount == 1
# This site is only reported on CDEC starting 2008-04-16. It is ragged off CDEC around that time.
# Actual start is earlier
    
path = "cdec_idb_*flow_2007_2022.csv"
series_start = pd.Timestamp(2008,4,16)
ts_cdec = read_ts(osp.join(cdec_dir,path),start=None,end=None)
ts_cdec.loc[slice(None,series_start)] = 0.
ts_cdec = ts_cdec.interpolate(limit=2)
ts_cdec = ts_cdec.to_period()
ts_cdec = ts_cdec.clip(lower = 0.0)
ts_cdec = rhistinterp(ts_cdec+5.,interval,lowbound=0.0,p=8.) - 5.
ts_cdec = ts_cdec.clip(lower=0.0)

# Confirm there are now no nans from beginning of cdec record through last good cdec
assert not ts_cdec.isnull().any(axis=None)  

# Now fill the preliminary period and extrapolate if requested
first_cdec = ts_cdec.first_valid_index()
last_cdec = ts_cdec.last_valid_index()
ts_cdec = ts_cdec.reindex(flux.index)
ts_cdec.loc[slice(None,series_start)] = 0.

if max_extrap_exports > 0:
    ts_cdec.loc[last_cdec:edate].ffill(limit=max_extrap_exports,inplace=True)
    # Re-assert with possibly fuller dates all the way to edate
    assert not ts_cdec.isnull().any(axis=None)

# Now add DSM2. This is legacy
ts2 = ts_merge([ts_cdec.squeeze(),ccold.squeeze()])
ts2 = ts2.clip(lower=0.0)
#ts2 = ts_cdec
flux["ccc_old"]=units.CFS2CMS*ts2[sdate:edate]
describe_null(ts2,"CCC Old")

##########################################

print("ccc victoria")
#dssdata = pyhecdss.get_ts(dsm2_file,"//CHVCT001/FLOW-EXPORT//1DAY//")
dssdata = []
for i,data in enumerate(dssdata):
    assert i==0
    ccvic = data[0]
    ccvic = rhistinterp(ccvic+2,interval,lowbound=0.0,p=12.) - 2
    ccvic = ccvic.clip(lower=0.0)
    ccvic = ccvic.fillna(0.)[sdate:edate]
path = "cdec_ccw_*flow_2007_2022.csv"

series_start = pd.Timestamp(2010,7,1) # start of victoria
ts_cdec = read_ts(osp.join(cdec_dir,path)).squeeze()
# This site begins in 2010-07-01. It is ragged off CDEC around that time.
# Other gaps to 2021 at least are 1 day long at the most, 2 is conservative
ts_cdec.loc[slice(None,series_start)] = 0.

ts_cdec = ts_cdec.interpolate(limit=2)
ts_cdec = ts_cdec.to_period()
ts_cdec = ts_cdec.clip(lower=0.0)
ts_cdec = rhistinterp(ts_cdec+5.,interval,lowbound=0.0,p=8.) - 5.
ts_cdec = ts_cdec.clip(lower=0.0)
# No nans from beginning of record through last good cdec
assert not ts_cdec.isnull().any(axis=None)  

# Now fill the preliminary period and extrapolate if requested
first_cdec = ts_cdec.first_valid_index()
last_cdec = ts_cdec.last_valid_index()
ts_cdec = ts_cdec.reindex(flux.index)
ts_cdec.loc[slice(None,series_start)] = 0.

if max_extrap_exports > 0:
    ts_cdec.loc[last_cdec:edate].ffill(limit=max_extrap_exports,inplace=True)
    # Re-assert with possibly fuller dates all the way to edate
    assert not ts_cdec.isnull().any(axis=None)
ts2 = ts_cdec.clip(lower=0.0)

flux["ccc_victoria"]=units.CFS2CMS*ts2[sdate:edate]
describe_null(ts2,"CCC Victoria")



if do_plots:
    print("Plotting fluxes")
    flux.plot()
    plt.legend()
    plt.show()

print("flux")
flux = flux[flux_labels]
flux.index.name = 'datetime'
print(flux)

flux.loc[sdate:edate,:].to_csv("fluxnew.th",date_format="%Y-%m-%dT%H:%M",float_format="%.3f",header=True,sep=" ")
#np.savetxt("fluxnew.th",flux,fmt="%0.3f",delimiter=" ")
for ndx,r in flux.iterrows():
    if r.isnull().any():
        print(ndx)
        print(r)

if flux.isnull().values.any():
    print("There were null flows")
    #currently in 2010-08-07 and possibly others
else:
    print("No nan flows")

# calaveras      21 ec       last   constant      125                                              
# cosumnes      446 ec       last   constant      125                                              
# moke          447 ec       last   constant      125                                              
# mtz           361 ec       last   ${QUALBNDINP} /FILL+CHAN/RSAC054/EC//1HOUR/${HISTQUALVERSION}/ 
# sac           330 ec       last   ${QUALBNDINP} /FILL+CHAN/RSAC139/EC//1DAY/${HISTQUALVERSION}/  
# vernalis       17 ec       last   ${QUALBNDINP} /FILL+CHAN/RSAN112/EC//1DAY/${HISTQUALVERSION}/  
# yolo          317 ec       last   ${QUALBNDINP} /FILL+CHAN/RSAC139/EC//1DAY/${HISTQUALVERSION}/  


#calaveras   21    1 last   ${BNDRYINPUT} /FILL+CHAN/RCAL009/FLOW//1DAY/${HISTFLOWVERSION}/         
#cosumnes   446    1 last   ${BNDRYINPUT} /FILL+CHAN/RCSM075/FLOW//1DAY/${HISTFLOWVERSION}/         
#moke       447    1 last   ${BNDRYINPUT} /FILL+CHAN/RMKL070/FLOW//1DAY/${HISTFLOWVERSION}/         
#north_bay  273   -1 last   ${BNDRYINPUT} /FILL+CHAN/SLBAR002/FLOW-EXPORT//1DAY/${HISTFLOWVERSION}/ 
#sac        330    1 last   ${BNDRYINPUT} /FILL+CHAN/RSAC155/FLOW//1DAY/${HISTFLOWVERSION}/         
#vernalis    17    1 last   ${BNDRYINPUT} /FILL+CHAN/RSAN112/FLOW//1DAY/${HISTFLOWVERSION}/         
#yolo       316    1 last   ${BNDRYINPUT} /FILL+CHAN/BYOLO040/FLOW//1DAY/${HISTFLOWVERSION}/        


# ccc       206   -1 last   ${BNDRYINPUT}         /FILL+CHAN/CHCCC006/FLOW-DIVERSION//1DAY/${HISTFLOWVERSION}/ 
# cccoldr    80   -1 last   ${BNDRYINPUT}         /FILL+CHAN/ROLD034/FLOW-EXPORT//1DAY/${HISTFLOWVERSION}/     
# cvp       181   -1 last   ${BNDRYINPUT}         /FILL+CHAN/CHDMC004/FLOW-EXPORT//1DAY/${HISTFLOWVERSION}/
# ccw       191   -1 last   ${BNDRYINPUT}         /FILL+CHAN/CHVCT001/FLOW-EXPORT//1DAY/${HISTFLOWVERSION}/   ##CCWP intake on Victoria Canal starting from Aug.1st,2010. - Lan 7/8/2011    
# stocwwtp   15    1 last   ${WWTP_FLOW_FILE}     /FILL+CHAN/STOC-WWTP/FLOW//1DAY/${HISTFLOWVERSION}/                                         
# sacrwwtp  335    1 last   ${WWTP_FLOW_FILE}     /FILL+CHAN/SACR-WWTP/FLOW//1DAY/${HISTFLOWVERSION}/
# manteca     6    1 last   ${WWTP_FLOW_FILE}     /FILL+CHAN/MANTECA-WWTP/FLOW//1DAY/${HISTFLOWVERSION}/ 




