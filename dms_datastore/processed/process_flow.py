#!/usr/bin/env python
# -*- coding: utf-8 -*-

import matplotlib.pyplot as plt
from vtools.functions.interpolate import *
from vtools.functions.filter import *
from vtools.data.vtime import *
from vtools.data.vis_gap import *
import os.path as osp
from dms_datastore.read_ts import *
import datetime as dtm
import vtools.functions.unit_conversions as units
from vtools.functions.unit_conversions import ec_psu_25c,CFS2CMS,CMS2CFS
from scipy.ndimage import gaussian_filter1d
from vtools import *
from vtools.functions.error_detect import *
import pandas as pd
import pyhecdss

# Notes USGS Woodbridge not getting updated in downloads.


do_plots=False

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
     
    
def describe_null(dset,name):
    print(f"describe_null for {name}")   
    
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
    else: 
        print("None")
    
sdate=pd.Timestamp(2006,10,1)
edate=pd.Timestamp(2025,6,28)
yolo_cms_file="prepared_yolo_cms.csv"

max_extrap_exports = 10300  # number of data points at destination freq (e.g. 15min) to extrapolate cdec for ccc_old and ccc_rock
interval = minutes(15)
buffer = days(5)
lag_sac = hours(3)


#continuous_repo = '//cnrastore-bdo/Modeling_Data/repo/continuous/formatted'
continuous_repo = '//cnrastore-bdo/Modeling_Data/repo/continuous/screened'
#continuous_repo = '//cnrastore-bdo/Modeling_Data/jenkins_repo/continuous/formatted'
ebmud_dir = '//cnrastore-bdo/Modeling_Data/ebmud'

usgs_dir = continuous_repo
wdl_dir = continuous_repo

cdec_dir = "data/cdec_download"
#usgs_dir="Data/usgs_download"
#wdl_dir = "Data/wdl_download"
alt="DWR-DMS-201203"

###

print("moke and cosumnes")
# Michigan bar
usgs_file="usgs_mhb_11335000_flow_*.csv"
usgs_dir2 = "data/usgs_download" # todo
#todo: removed buffer. Does it work?
usgs = read_ts(osp.join(usgs_dir,usgs_file),start=sdate,end=edate+hours(2)).squeeze() 
usgs.loc["2023-08-19":"2023-08-29"] = usgs.loc["2023-08-19":"2023-08-29"].mean()
#,selector="15453_00060").squeeze()
usgs.loc["2008-07-04T23:00":"2008-07-09T00:00"] = 22.3 

if do_plots:
    usgs.plot()
    plt.title("Cosumnes")
    plt.show()

#usgs2 = usgs.replace(22.3,dtm.datetime(2008,7,4,23),dtm.datetime(2008,7,9))
# This indexing merely signifies that we have verified safety up to 2025 on the gaps that exist
usgs.loc[usgs.index[0]:pd.Timestamp(2025,1,1)] = usgs.interpolate()

describe_null(usgs,"michigan bar gap")


# todo: this seems to be missing in the downloads, but it won't evolve often anyhow since they only 
# update annually
usgs_file="q_woodbridge.rdb"

#moke_usgs = read_ts(osp.join(usgs_dir,usgs_file),selector="228917_00060_00003")
moke_usgs = read_ts(osp.join("data/usgs_download",usgs_file)).squeeze() #,comment="#",sep="\s+",parse_dates=['datetime'],index_col='datetime').squeeze()
#moke_usgs = moke_usgs["228917_00060_00003"]
moke_usgs = moke_usgs.interpolate(limit=80)  # todo: this is a lot but it is OK in existing gaps as of 2021-05-22
moke_usgs = moke_usgs.to_period(freq="d").to_frame()
moke_usgs = rhistinterp(moke_usgs+1.0,interval,lowbound=0.0,p=4,maxiter=4) - 1.0
moke_usgs = moke_usgs.clip(lower=0.0)
moke_usgs = moke_usgs[sdate:edate]

ebmud_file = "ebmud_wbr_flow_*.csv"
moke2 = read_ts(osp.join(ebmud_dir,ebmud_file))
moke2 = moke2.interpolate(limit=4)
moke2 = moke2.to_period(freq="d").squeeze()
print("Moke2 tail")
print(moke2.tail())


moke2 = rhistinterp(moke2+4.0,interval,lowbound = 0., p=1.,maxiter=8)-4.
moke2 = moke2.clip(lower=0.0)
print("Moke2 tail")
print(moke2.tail())


if do_plots:
    moke2.plot()
    plt.title("moke2")
    plt.show()

cdec_file = "cdec_cmn_*flow_*.csv"
cmn = read_ts(osp.join(cdec_dir,cdec_file)).squeeze()
cmn = cmn.resample(interval).ffill(limit=20)
cmn = med_outliers(cmn,range=(-1,100000),scale=10000)
cmn = cmn.interpolate(limit = 60)

moke_usgs.columns = ["moke"]
moke2.columns = ["moke"]
cmn.columns = ["moke"]
moke = ts_merge([moke_usgs,moke2,cmn],names="moke")

describe_null(moke,"Moke")
describe_null(moke_usgs,"moke_usgs")
describe_null(moke2,"moke2")
describe_null(cmn,"cmn")

east_side_by_side = pd.concat([usgs,moke],axis=1)
east_side_by_side.columns = ['cosumnes','moke']
# assures both are univariate as well as making sure addion isn't name-sensitive
east_side_by_side['east'] = usgs.squeeze() + moke.squeeze() 


if do_plots:
    plt.plot(east_side_by_side.index,east_side_by_side.values)
    plt.legend()
    plt.show()

east=east_side_by_side.east
east = east[sdate:edate].to_frame()
east.columns=["east"]

flux = -units.CFS2CMS*east

describe_null(east,"East")

##
ACRE_FT_DAY_CMS = 0.504166    # First is to cfs, then on to cms later
ts_ccwd = pd.read_csv("//cnrastore-bdo/Modeling_Data/ccwd/Total Diversions 1974-2021.csv",sep=',',parse_dates=[0],index_col=0,header=0,comment="#")
ts_ccwd.columns=['ccc_rock','ccc_old','ccc_victoria']
print(ts_ccwd)
ts_ccwd*=ACRE_FT_DAY_CMS
# Confirm there are now no nans from beginning of cdec record through last good cdec

assert not ts_ccwd.isnull().any(axis=None)  
ts_ccwd = ts_ccwd.to_period()
ts_ccwd = rhistinterp(ts_ccwd+5,interval,lowbound = 0.0,p=8.)-5.
ts_ccwd = ts_ccwd.clip(lower=0.)

def ccwd_intake(station_id,name,interp_limit,ts_ccwd):
    path = f"cdec_{station_id}_*flow_2006_*.csv"
    ts_cdec = read_ts(osp.join(cdec_dir,path),start=pd.Timestamp(2022,1,1),end=None)
    ts_cdec = ts_cdec.interpolate(limit=interp_limit)
    ts_cdec = ts_cdec.to_period()
    ts_cdec = ts_cdec.clip(lower = 0.0) 
    ts_cdec = rhistinterp(ts_cdec+5.,interval,lowbound=0.0,p=8.) - 5.
    ts_cdec = ts_cdec.clip(lower=0.0)
    ts_cdec.columns = [name]
    # Confirm there are now no nans from beginning of cdec record through last good cdec
    assert not ts_cdec.isnull().any(axis=None)  

    # Now fill the preliminary period and extrapolate if requested
    first_cdec = ts_cdec.first_valid_index()  # should be (2022,1,1)
    last_cdec = ts_cdec.last_valid_index()
    ts2 = ts_merge([ts_ccwd[name],ts_cdec.squeeze()])
    ts2 = ts2.interpolate(limit=6)
         
    
    return ts2


freeport_arch_dir = "//cnrastore-bdo/Modeling_Data/projects/usgs/usgs_aquarius_request_2020/11447650"
freeport_archived_file = "Discharge.ft^3_s.(15_min_ave_VELQ).velq@11447650.EntireRecord.csv"
fpt_archived = read_ts(osp.join(freeport_arch_dir,freeport_archived_file))
fpt_archived = fpt_archived.interpolate(limit=20)
fpt_archived = cosine_lanczos(fpt_archived,hours(40))[sdate-buffer:]
describe_null(fpt_archived,"free_archive pass 2")


ts2 = ccwd_intake("inb","ccc_rock",5,ts_ccwd)    
flux["ccc_rock"]=units.CFS2CMS*ts2
# Fill NA values up to fixed # of days. This is often needed at end of fpt 
flux.loc[:,"ccc_rock"]=flux.ccc_rock.ffill(limit=7)
describe_null(ts2,"CCC Rock")

###

ts2 = ccwd_intake("idb","ccc_old",2,ts_ccwd)
flux["ccc_old"]=units.CFS2CMS*ts2[sdate:edate]
# Fill NA values up to fixed # of days. This is often needed at end of series 
flux.loc[:,"ccc_old"]=flux.ccc_old.ffill(limit=7)

describe_null(ts2,"CCC Old")
###
ts2 = ccwd_intake("ccw","ccc_victoria",2,ts_ccwd)
# Fill NA values up to fixed # of days. This is often needed at end of series 
flux["ccc_victoria"]=units.CFS2CMS*ts2[sdate:edate]
flux.loc[:,"ccc_victoria"]= flux.ccc_victoria.ffill(limit=7)
describe_null(ts2,"CCC Victoria")

##



print("american")

am_file = osp.join(usgs_dir,"usgs_afo_11446500_flow_*.csv")
am = read_ts(am_file)
am = am.interpolate()  # todo: Check this no significant gaps are missed
am = am[(sdate-lag_sac):(edate+lag_sac)]  # Buffer because will be subtracted from Sac and then Sac will be lagged
flux["american"] = -units.CFS2CMS*am[sdate:edate]
describe_null(am,"American")

print("sac")
# Freeport flow does not include Sac Regional SAN flow
# Reason for this is that for biogeochemistry it is a source and requires water+constituent
free_usgs_file ="usgs_fpt_11447650_flow_*.csv"
selector="sensor=236032_00060",
free_usgs = read_ts(osp.join(usgs_dir,free_usgs_file))

#print(free_usgs["2008-09-01":"2008-09-02"])
#describe_null(free_usgs,"first")
print("freq",free_usgs.index.freq)
#free_usgs = free_usgs.resample(minutes(15)).interpolate(limit=10)
free_usgs = free_usgs.interpolate(limit=20)


free_usgs = cosine_lanczos(free_usgs,hours(40))[sdate-buffer:edate+buffer]
describe_null(free_usgs,"free_usgs pass 1")




free_usgs.columns = ["value"]
fpt_archived.columns = ["value"]
free_usgs = ts_merge([free_usgs,fpt_archived])
free_usgs = free_usgs.interpolate() # todo: risky

if do_plots:
    ax=free_usgs.plot()
    ax.set_title("Freeport")
    plt.show()

describe_null(free_usgs,"free_usgs")


# This step is not automated yet. Data are dumped into a text file, then analyzed using stochastic cycles
# in R and finally reloaded. The start of this can be rolled forward. 
# This is kind of a pain, but one benefit of this method is that the filtration is not gappy.
# If we don't do this, extrapolation will result from the call to interpolate a few lines above marked 'risky'

# free_usgs_r = pd.read_csv("fpt_subtide_r_20210601_15min.csv",header=None)*100.*CMS2CFS
# ndx = pd.date_range(start=pd.Timestamp(2021,6,1),freq='15T',periods=len(free_usgs_r))
# free_usgs_r.index=ndx
# print("length of R")
# print(free_usgs_r.index[-1])
# free_usgs.columns = ["fpt"]  # prep names for update()
# free_usgs_r.columns = ["fpt"]
# free_usgs.update(free_usgs_r)


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


# Yolo is done in another script that must be run first
yolo = pd.read_csv(yolo_cms_file,sep=",",index_col=0,parse_dates=[0],header=0,comment="#")[sdate:edate]
flux["yolo_toedrain"]= -yolo.yolo_toedrain
describe_null(yolo.yolo_toedrain,"Yolo Toe Drain")

flux["yolo"] = (-yolo.yolo)
# These tiny minimum fluxes balance out evaporation that causes entire boundary to run dry
is_summer = (flux.index.month>4) & (flux.index.month < 10)
flux.loc[is_summer,"yolo"] = flux.loc[is_summer,"yolo"].clip(upper=-0.01) 
describe_null(yolo.yolo,"Yolo")





print("calaveras")
path = "cdec_nhg_*flow_2006_*.csv"
ts_cdec = read_ts(osp.join(cdec_dir,path))
ts2 = ts_cdec
ts2 = ts2.resample(interval).interpolate(limit=20)
# This is a significant assumption that summer flows below 500cfs do not reach the Delta
# The principle has been confirmed by email, but it is a big reduction relative to Dayflow
ts2[ts2.lt(500.)] = 0.
ts2=ts2.fillna(0.)
#ts2 = rhistinterp(ts2+1.0,interval,lowbound=0.0,p=4,maxiter=20) - 10.0
flux["calaveras"] = -units.CFS2CMS*ts2
# Fill NA values up to fixed # of days. This is often needed at end of series 
flux["calaveras"]=flux['calaveras'].ffill(limit=7)
describe_null(ts2,"Calaveras")

#sjr_usgs_backup="W:/usgs_scalar_to_oct_2013/Q.UV.USGS.11303500.4.C.00000000.rdb"
#sjr_usgs_bk = read_ts(sjr_usgs_backup)
print("sjr")
vnl_usgs_file="usgs_vns_11303500_*flow_*.csv"
usgs = read_ts(osp.join(usgs_dir,vnl_usgs_file)) #,selector="sensor=15169_00060")     
usgs = med_outliers(usgs,level=4,scale = 100.,filt_len=5,range=(10.,50000.))
usgs = usgs.interpolate(limit=500)

usgs = monotonic_spline(usgs,dest=usgs.index)[sdate:edate]
moss_file ="ncro_msd_b95820q_flow_*.csv"
mossdale = read_ts(osp.join(wdl_dir,moss_file))
mossdale = mossdale.interpolate(limit=20)
mossdale = cosine_lanczos(mossdale,hours(40))[sdate:edate]
ts15 = ts_merge([usgs.squeeze(),mossdale.squeeze()])[sdate:edate]
# This indexing is here to express the fact that only this section is safe to do without interpolation limit
ts15.loc[pd.Timestamp(2017,1,1):pd.Timestamp(2017,7,1)] = ts15.interpolate()


do_plots=True
if do_plots:
    plt.plot(usgs.index,usgs.values)
    plt.plot(mossdale.index,mossdale.values)
    plt.plot(ts15.index,ts15.values)
    plt.legend(["Vernalis","Mossdale","Merged"])
    plt.show()

#usgs = usgs.resample('15T').interpolate(limit=8)
#usgs = monotonic_spline(usgs,dest=minutes(15)) 
flux["sjr"] = -units.CFS2CMS*ts15
describe_null(ts15,"sjr")


print("north bay")

path = "cdec_bks_*flow_*.csv"
ts_cdec = read_ts(osp.join("data/cdec_download",path),start=None,end=None).squeeze()
ts = ts_cdec.to_period(freq="D")
ts = ts.interpolate(limit=2) # as of 2022-01 there is only one missing value
ts.columns = ['value']
ts.index.name = 'datetime'

print("barker cdec")
print(ts)
print(type(ts))

ts_dsm2 = pd.read_csv("data/northbay_flow_1989_2012.csv",index_col=0,parse_dates=[0],comment="#",sep=",").squeeze()
ts_dsm2 = ts_dsm2.to_period(freq="D")
ts_dsm2 = ts_dsm2.interpolate(limit=2)
ts_dsm2.columns=['value']

print("barker ts_dsm2")
print(ts_dsm2)
print(ts)
ts = ts_merge((ts,ts_dsm2),names="value")

ts2 = rhistinterp(ts+10,interval,lowbound=0.0,p=20.) - 10
ts2 = units.CFS2CMS*ts2.clip(lower=0.)

flux["northbay"]=ts2[sdate:edate]
describe_null(ts2,"Barker")



print("swp")
exports_file = "prepared_exports_cms.csv"
exports_sap = pd.read_csv(exports_file,sep=",",header=0,parse_dates=[0],index_col=0,comment="#")

ts2=exports_sap
flux["swp"]=ts2[sdate:edate]


#todo: removed buffer from cvp, napa coyote
print("cvp")
path = "cdec_trp_*flow_*.csv"
ts_cdec = read_ts(osp.join(cdec_dir,path))
ts_cdec = ts_cdec.interpolate()
ts_cdec = ts_cdec.asfreq('D').to_period()
cvp = rhistinterp(ts_cdec+10.,interval,lowbound=0.0,p=6.,maxiter=10) -20.
cvp = cvp.clip(lower=0.0)[sdate:edate]
flux["cvp"]=units.CFS2CMS*cvp
describe_null(cvp,"cvp")

print("napa")
usgs_file="usgs_napr_11458000_flow_*.csv"
usgs = read_ts(osp.join(usgs_dir,usgs_file))
usgs=usgs.resample(interval).interpolate(limit=10)[sdate:edate]
napa = usgs.fillna(0.)
flux["napa"] = -units.CFS2CMS*napa[sdate:edate]

print("coyote")
usgs_file="usgs_coycr_11172175_flow_*.csv"
usgs = read_ts(osp.join(usgs_dir,usgs_file))
ts2=usgs.resample(interval).interpolate(limit=50)
ts2 = ts2.fillna(0.)
flux["coyote"] = -units.CFS2CMS*ts2


###### CCWD
# This is a period of record file given to us by CCWD covering 1974-2021




#####################

if do_plots:
    print("Plotting fluxes")
    fig,(ax0,ax1) = plt.subplots(2,sharex=True)
    flux[["east","ccc_rock","ccc_old","ccc_victoria","yolo_toedrain","calaveras","northbay","napa","coyote"]].plot(ax=ax0)
    flux[["american","sac","yolo","sjr","swp","cvp"]].plot(ax=ax1)
    ax0.legend()
    ax0.grid()
    ax0.set_ylabel("cms")
    ax1.legend()    
    ax1.grid()
    ax1.set_ylabel("cms")
    plt.show()

print("flux")
flux = flux[flux_labels]
flux.index.name = 'datetime'
print(flux)

flux.loc[sdate:edate,:].to_csv("fluxnew.th",date_format="%Y-%m-%dT%H:%M",float_format="%.2f",header=True,sep=" ",na_rep="nan")
#np.savetxt("fluxnew.th",flux,fmt="%0.3f",delimiter=" ")

nullrow = flux.isnull().any(axis=1)
null_df = flux.loc[nullrow,:]


nnull = nullrow.sum()
if nnull>0:
    print(f"There were {nnull} null flows")
    null_df.to_csv("null_entries.csv",float_format="%.3f",header=True,
                   na_rep="nan",index=True,date_format="%Y-%m-%dT%H:%M",sep=",")
    #currently in 2010-08-07 and possibly others
else:
    print("No nan flows")





