#!/usr/bin/env python
# -*- coding: utf-8 -*-

#from statsmodels.tsa.seasonal import STL,seasonal_decompose
import numpy as np
import matplotlib.pyplot as plt
from vtools.functions.interpolate import *
from vtools.functions.filter import *
from vtools.data.vtime import *
import os
import os.path as osp
from dms_datastore.read_ts import *
import datetime as dtm  
import schimpy.unit_conversions as units
from schimpy.unit_conversions import ec_psu_25c
from scipy.ndimage.filters import gaussian_filter1d
from vtools.functions.error_detect import *
import pandas as pd
import pyhecdss


do_plots=False
alt="DWR-DMS-201203"

salt_ndx = {"time":0,"sjr":1,"sac":2,"yolo_toedrain":3,"yolo":4}

# These are the bcflux.in entries for the Bay Delta, in their simplest form
# Note: this is not updated for the American and for the Yolo-Yolo Toedrain split, although the code 
# below is set up for the latter. 
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
            nans = dset.loc[isnan]
        except:
            isnan = dset.isnull()
            nans = dset[dset.isnull()]
        count = isnan.sum()
        print("Count: {}".format(nans.sum()))
        print(nans)
    else: print("None")
    
sdate=dtm.datetime(2007,10,1)
edate=dtm.datetime(2022,1,1)

interval = minutes(15)
buffer = days(5)

cdec_dir = "data/cdec_download"
usgs_dir="Data/usgs_download"
wdl_dir = "Data/wdl_download"
des_dir = "Data/des_download"
dsm2_file="D:/Delta/dsm2_v8/timeseries/hist_19902012.dss"
alt="DWR-DMS-201203"


print("sac")
verona_gs = read_ts(os.path.join(usgs_dir,"usgs_von_11425500_ec_2007_2022.rdb")).squeeze()
verona_gs = verona_gs.interpolate(limit=320)
#verona_cdec = read_ts("data/cdec_download/von_ec.csv").squeeze()
#verona_cdec = verona_cdec.mask((verona_cdec < 10.) | (verona_cdec > 500.))
#verona_cdec = verona_cdec.interpolate(limit=10)
#verona_cdec.loc["2008-02-01":"2008-04-01"] = np.nan
#verona_cdec.loc["2021-01-04":"2008-01-05"] = np.nan
srh = read_ts(os.path.join(des_dir,"des_srh_70_ec_2007_*.csv")).squeeze()
srh = srh.mask((srh < 10.) | (srh > 400.))
approx_bias=25.
srh = srh.interpolate(limit=10) - approx_bias
#srh.loc["2008-02-01":"2008-04-01"] = np.nan
sac = ts_merge([verona_gs,srh])
#srdiff = verona_gs - srh
sac=sac.interpolate()   # todo: major 

print("Sac EC has gap: %s" % sac.isnull().any())




print("yolo toedrain")
""" The code below does a seasonal decomposition here. This is a little tough because
the flow is sometimes upstream, sometimes downstream, and at the transitions the concentration flips.
At some point we may want to encode all this, but as a first cut it iseasiest to ignore this
with the insights being that 
1) when Lisbon is an outflow the boundary condition isn't even used and
2) for the rest of the time 600-650 is the typical seasonal result (spikes are much higher)
"""
yolo_toedrain = sac*0. + 650.
yolo_toedrain.name = "yolo_toedrain"
print(yolo_toedrain.tail())
lisbon = read_ts(os.path.join(wdl_dir,"ncro_lis_b9156000_ec_2013_2021.csv"))
lisbon = lisbon.resample('15T').interpolate(limit=200)
#lisbon.name = "yolo_toedrain"
lisbon.columns = ["yolo_toedrain"]
print(lisbon.tail())
lisbon_cdec = read_ts(os.path.join(cdec_dir,"cdec_lis_*ec_2007_*.csv"))
lisbon_cdec = med_outliers(lisbon_cdec,level=4,quantiles=(0.01,0.99),range = (50,1500))
#lisbon_cdec.name = "yolo_toedrain"
lisbon_cdec.columns = ["yolo_toedrain"]
print(lisbon_cdec.tail())

yolo_toedrain = yolo_toedrain.to_frame()
yolo_toedrain.columns=["yolo_toedrain"]
yolo_toedrain.update(lisbon_cdec)
yolo_toedrain.update(lisbon)
yolo_toedrain = yolo_toedrain.squeeze()

ax=lisbon.plot()
lisbon_cdec.plot(ax=ax)
yolo_toedrain.plot(ax=ax)
plt.legend(["WDL","CDEC","final"])

plt.show()

# lisbon = lisbon.resample('D').mean()
# lisbon = lisbon.interpolate(limit=200)
# stld = seasonal_decompose(lisbon,period=365,extrapolate_trend='freq')
# tmean = stld.trend.mean()
# res=stld.seasonal
# res.loc[:] = gaussian_filter1d(res,sigma=12,mode='nearest')+tmean
# res=res["2016-01-01":"2016-12-31"]
# res=res.to_frame()
# res["julian_day"] = res.index.dayofyear
# res=res.set_index("julian_day",drop=True)
# #res = STL(lisbon,period=365,robust=True,low_pass=701).fit()
# #res = res.seasonal

# res.plot()
# plt.show()

# ax=lisbon.plot()
# plt.show()


print("sjr")
sjr_ec = read_ts(os.path.join(des_dir,"des_sjr_90_ec_2007_9999.csv")).squeeze()
sjr_ec = med_outliers(sjr_ec,level=4,quantiles=(0.25,0.75))

# This is awfully close to a possible value, so a bit dangerous
sjr_ec = sjr_ec.mask( (sjr_ec < 25.) | (sjr_ec > 1425.) )
sjr_ec = sjr_ec.interpolate(limit=200)

path="cdec_ver_*ec_2007_*.csv"
ts_cdec = read_ts(osp.join(cdec_dir,path),start=None, end=None).squeeze()
ts_cdec = med_outliers(ts_cdec,level=4,quantiles=(0.25,0.75))
ts_cdec = ts_cdec.mask( (ts_cdec < 25.) | (ts_cdec > 1425.) )
ts_cdec = ts_cdec.interpolate(limit=150)

sjr = ts_merge([sjr_ec,ts_cdec])["2007-01-01":]
# ax=sjr.plot()
# ts_cdec.plot(ax=ax)
# sjr_ec.plot(ax=ax)
# plt.show()
print(sjr[sjr.isnull()])

print("SJR EC has gap: %s" % sjr[sdate:edate].isnull().any())



print("yolo")
""" The assumption here is that when Yolo bypass is used, flow is mostly fresh coming from runoff
    We should revisit but probably good for now"""
yolo = sac*1.

columns = ["sjr","sac","yolo_toedrain","yolo"]
salt = pd.concat([sjr,sac,yolo_toedrain,yolo],axis=1)[sdate:edate]

salt.columns = columns
ec = salt.copy()
salt[:] = ec_psu_25c(salt[:])

salt[columns].to_csv("salt_new.th",date_format="%Y-%m-%dT%H:%M",float_format="%4.2f",sep=" ")

# This subset is enough to see the missign values and nature, since the others are pure copies
ec.plot(subplots=True)
plt.legend()
plt.show()

print("Null values in salt: {}".format(salt.isnull().any()))

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




