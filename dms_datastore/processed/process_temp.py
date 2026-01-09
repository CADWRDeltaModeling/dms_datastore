#!/usr/bin/env python
# -*- coding: utf-8 -*-

import matplotlib.pyplot as plt
from vtools.functions.filter import *
from vtools.data.vtime import *
import os.path as osp
from dms_datastore.read_ts import *
from vtools.functions.merge import *
from vtools.functions.error_detect import med_outliers
import datetime as dtm
import vtools.functions.unit_conversions as units
from vtools.functions.unit_conversions import ec_psu_25c
from scipy.ndimage import gaussian_filter1d

# from vtools.functions.error_detect import *
import pandas as pd
import csv
import numpy as np


def describe_null(dset, name):
    print(f"null for {name}")

    if dset.isnull().values.any():
        try:
            isnan = dset.isnull().any(axis=1)
            intnan = isnan.loc[dset.first_valid_index() : dset.last_valid_index()]
            nans = dset.loc[isnan, :]
            print("dataframe")
        except:
            isnan = dset.isnull()
            intnan = isnan.loc[dset.first_valid_index() : dset.last_valid_index()]
            nans = dset[isnan]
            print("series")
        interiornan = intnan.sum(axis=0)
        count = isnan.sum(axis=0)
        print("Count: {} Interior: {}".format(count, interiornan))
        print(nans)
    else:
        print("None")


def read_pd(fname):
    ts = pd.read_csv(
        fname, index_col=1, parse_dates=[1], header=0, comment="#", na_values="M"
    )
    ts.columns = [x.strip() for x in ts.columns]
    val = ts[["Value"]].as_matrix()
    times = ts.index.to_pydatetime()
    tsout = its(times, val)
    tsout = its2rts(tsout, minutes(15))
    tsout.data = tsout.data.flatten()
    return tsout


def f_to_c(ts):
    return (ts - 32.0) * 5.0 / 9.0


temp_ndx = {
    "time": 0,
    "coyote": 1,
    "sjr": 2,
    "calaveras": 3,
    "east": 4,
    "american": 5,
    "sac": 6,
    "yolo_toedrain": 7,
    "yolo": 8,
    "napa": 9,
}
nstation = 4
sdate = pd.Timestamp(2006, 10, 1)
edate = pd.Timestamp(2025, 6, 30)
buffer = days(5)

repo_dir = "//cnrastore-bdo/Modeling_Data/repo/continuous/screened"
cdec_dir = "data/cdec_download"
usgs_dir = "Data/usgs_download"
ncro_dir = "//cnrastore-bdo/Modeling_Data/repo/continuous/screened"
des_dir = "Data/des_download"
usgs_legacy = "//cnrastore-bdo/Modeling_Data/projects/usgs/usgs_aquarius_request_2020/"

print("Knights Landing")
"""
As of 2021-12-01 update, the closest data is in the Sac above Colusa Drain station, which is quite near the boundary
Although, it is pretty far upstream, Sac @ Wilkins Slough (USGS) matches well enough to be a surrogate
Slightly downstream is the USGS verona station as reported on NWIS. This station stops in 2017, and then went "rogue", reporting on CDEC
but with no maintenance and deteriorating quality to the point where we talked to them about it and they took it off. 

Freeeport was originally thought to be a good substitute after that, but it turns out to be awful in 2020 and 2021 late summer. Hopefully 
There are enough alternate sources to not need this.
"""


freeport = read_ts(osp.join(repo_dir, "usgs_fpt_11447650_temp*.csv")).squeeze()
freeport = freeport.interpolate(limit=8).squeeze()

freeport_early = read_ts(
    osp.join(
        usgs_legacy,
        "11447650/Temperature,_water.degC.(DATA_PROGRAM)@11447650.EntireRecord.csv",
    ),
    start=sdate - buffer,
)
freeport_early = freeport_early.interpolate(limit=8).squeeze()

# This station stopped reporting temperature in 2017 on NWIS but seemingly has data on CDEC,
# Didn't update file because it isn't being updated
verona_t = read_ts(
    osp.join(usgs_dir, "t_verona_usgs.rdb"), start=sdate, end=edate
).squeeze()
verona_t = verona_t.interpolate(limit=20)
verona_early = read_ts(
    osp.join(usgs_legacy, "11425500/Temperature,_water.degC@11425500.EntireRecord.csv"),
    start=sdate - buffer,
)
verona_early = verona_early.interpolate(limit=8).squeeze()


# This is the period of record DWR Knights Landing off WDL
# knights_t = read_ts(osp.join(wdl_dir,"ncro_klup_a0223002_temp_2006_*.csv"))
knights_t = read_ts(osp.join(ncro_dir, "ncro_klup_*_temp_*.csv"))
knights_t.columns = ["klup"]


# This is a real time update of klup we got on a one off basis
# knights_t2 = pd.read_csv(osp.join(wdl_dir,"SacRabCBD_2021-11-17_110212_-0800.csv"),header=1,parse_dates=["datetime"],index_col="datetime").loc[:,"Temp"].to_frame()
# knights_t2 = knights_t2.resample('15T').interpolate(limit=2)
# knights_t2.columns = ["klup"]
# knights_t = ts_merge([knights_t,knights_t2])
knights_t = knights_t.interpolate(limit=12).squeeze()

# This is the Wilkins slough station
wilkins = read_ts(osp.join(usgs_dir, "usgs_wlk_11390500_temp_2006_*.csv"))
wilkins = wilkins.interpolate(limit=12).squeeze()

verona_merged = ts_merge(
    [
        x.squeeze()
        for x in [knights_t, wilkins, verona_t, verona_early, freeport, freeport_early]
    ]
)
print(verona_merged)

# ax=verona_merged.plot(color="0.5",linewidth=3)
# verona_t.plot(ax=ax)
# knights_t.plot(ax=ax)
# wilkins.plot(ax=ax)
# freeport.plot(ax=ax)
# plt.legend(["merged","verona","knights","wilkins","freeport"])
# plt.show()

# ax=verona_merged.plot(color="0.5",linewidth=3)
# freeport.plot(ax=ax)
# plt.show()
describe_null(verona_merged, "Verona")

# verona_merged = ts_merge([verona_t,verona1_t,
#                     verona2_t["2019-02-28":"2020-10-17"],
#                     verona2_t["2020-12-23":"2021-01-03"],
#                     verona2_t["2021-01-05":"2021-03-04"],
#                     freeport])

print("Sac has nan: %s" % verona_merged.isnull().any())


print("SJR")

sjr_t = read_ts(
    osp.join(usgs_dir, "usgs_vns_11303500_temp_*.csv"), start=sdate, end=edate
)
print(sjr_t)
sjr_t = sjr_t.interpolate(limit=8)
sjr_t.columns = ["sjr"]


sjr_des_t = read_ts(osp.join(des_dir, "des_sjr_90_temp_*.csv"), start=sdate, end=edate)
sjr_des_t.columns = ["sjr"]
sjr_des_t = sjr_des_t.interpolate(limit=8)

sjr_cdec_t = read_ts(osp.join(cdec_dir, "cdec_ver_*temp_*.csv"), start=sdate, end=edate)
sjr_cdec_t[sjr_cdec_t < 32.0] = np.nan
sjr_cdec_t = f_to_c(sjr_cdec_t)
sjr_usbr = sjr_cdec_t.resample(minutes(15)).interpolate(method="index", limit=8)
sjr_usbr.columns = ["sjr"]
sjr = ts_merge([sjr_t, sjr_des_t, sjr_usbr])


print("SJR has nan: %s" % sjr.isnull().any())

print("Mokelumne")
nmr_dwr = read_ts(
    osp.join(ncro_dir, "ncro_mkn_b94133_temp_*.csv"), start=sdate, end=edate
)
nmr_dwr = med_outliers(nmr_dwr, scale=0.2, range=(2, 35))
nmr_dwr = nmr_dwr.interpolate(limit=20)
nmr_t = read_ts(
    osp.join(usgs_dir, "t_moke_nf_usgs.rdb")
)  # North Fork Moke = cdec nmr, no longer updated so older file name
nmr_t = med_outliers(nmr_t, scale=2.0, range=(2, 35))
nmr_t = nmr_t.interpolate(limit=20)
# nmr_t.data[nmr_t.data < 40.] = np.nan
nmr_t = nmr_t.interpolate(limit=8)
smr_t = read_ts(
    osp.join(usgs_dir, "t_moke_sf_usgs.rdb")
)  # No longer updated, so older file name
smr_t = med_outliers(nmr_t, scale=2.0, range=(2, 35))
nmr_dwr.columns = ["nmr_t"]
nmr_t.columns = ["nmr_t"]
smr_t.columns = ["nmr_t"]

mkn_t = read_ts(
    osp.join(cdec_dir, "cdec_mkn_*temp_*.csv")
)  # this, unusually, is in degrees C
mkn_t = med_outliers(mkn_t, range=(2, 35))
mkn_t = mkn_t.interpolate(limit=20)
mkn_t.columns = ["nmr_t"]

nmr_t = ts_merge([nmr_dwr, nmr_t, smr_t, mkn_t])

ax = nmr_t.plot()
mkn_t.plot(ax=ax)
plt.show()

# nmr_t = f_to_c(nmr_t)
print("Moke has nan: %s" % nmr_t.isnull().any())


print("American")
watt_t = read_ts(
    osp.join(usgs_dir, "usgs_awb_11446980_temp_*.csv"), start=sdate, end=edate
).squeeze()
watt_t = watt_t.interpolate(limit=20)
watt_t.columns = ["american"]
# fairoak_t = read_ts(osp.join(usgs_dir,"t_fair_oaks_usgs.rdb"),start=sdate,end=edate).squeeze()
fairoak_t = read_ts(
    osp.join(usgs_dir, "usgs_afo_11446500_temp_*.csv"), start=sdate, end=edate
).squeeze()
fairoak_t = fairoak_t.interpolate(limit=20)


fairoak_t.columns = ["american"]
watt_final_t = ts_merge([watt_t, fairoak_t])
watt_final_t = watt_final_t.interpolate()
print("American has nan: %s" % watt_final_t.isnull().any())
print(watt_final_t)


print("Yolo (Lisbon Weir)")
lisbon_t = read_ts(osp.join(ncro_dir, "ncro_lis_b9156000_temp_*.csv")).squeeze()
lisbon_t = med_outliers(lisbon_t, scale=2.0, range=(5, 35))

lis_cdec = (
    read_ts(osp.join(cdec_dir, "cdec_lis_*temp_*.csv"))
    .clip(lower=38.0, upper=85.0)
    .squeeze()
)
lis_cdec2 = med_outliers(lis_cdec, scale=2.0, range=(39, 85.0))
lis_cdec2 = f_to_c(lis_cdec2["2020-08-30":"2021-08-24"])

# ax = lisbon_t.plot()
# lis_cdec.plot(ax=ax)
# lis_cdec2.plot(ax=ax)
# plt.legend(["lisbon_t","cdec","cdec out"])
# plt.show()
lisbon_t = ts_merge([lisbon_t, lis_cdec2])
print("Lisbon")
print(lisbon_t)

temp = pd.concat([sjr, nmr_t, watt_final_t, verona_merged, lisbon_t], axis=1)
print(temp)

temp.columns = ["sjr", "east", "american", "sac", "yolo_toedrain"]

# eliminate extremes
temp[temp > 40] = np.nan
temp[temp < 1] = np.nan
temp = temp.resample("15min").asfreq()


# fillin approximations
# this is a function to allow some flexibility in how it is done
def fillin_columns(arr, dest, src):
    arr[dest] = arr[dest].fillna(arr[src])


temp.index.name = "datetime"
temp.loc[sdate:edate, :].to_csv(
    "temp_no_impute.csv", date_format="%Y-%m-%dT%H:%M", float_format="%.1f"
)
temp.loc[sdate:edate, :].plot()
plt.show()

# East is used here for filling but eventually overwritten by American
fillin_columns(temp, "sjr", "yolo_toedrain")
fillin_columns(temp, "sjr", "east")
fillin_columns(temp, "yolo_toedrain", "east")
fillin_columns(temp, "yolo_toedrain", "sjr")
fillin_columns(temp, "east", "sac")
fillin_columns(temp, "sac", "east")
fillin_columns(temp, "east", "sjr")
fillin_columns(temp, "sac", "sjr")
fillin_columns(temp, "american", "east")
fillin_columns(temp, "american", "sac")

temp["east"] = temp[
    "american"
]  # Here east is replaced, after noting that temperatures in this area are lower
temp["coyote"] = temp["east"]
temp["napa"] = temp["east"]
temp["calaveras"] = temp["east"]
temp["yolo"] = temp["yolo_toedrain"]

temp.loc[sdate:edate, :].plot()
plt.show()


temp = temp.loc[sdate:edate, :]
nanbycolumn = temp.isnull().any(axis=0)
anynan = nanbycolumn.any()
if anynan:
    print("There were nan temperatures in clipped range: %s" % anynan)
    print(nanbycolumn)
    # for key in temp.columns:
    #    if temp[key].isnull().any():
    #        print("Nan in column: %s" % key)
else:
    print("No NaN data found within clipped date range after filling")

columns = [
    "coyote",
    "sjr",
    "calaveras",
    "east",
    "american",
    "sac",
    "yolo_toedrain",
    "yolo",
    "napa",
]
temp[columns].to_csv(
    "temp_new.th",
    date_format="%Y-%m-%dT%H:%M",
    float_format="%.1f",
    sep=" ",
    lineterminator="\n",
)

# This subset is enough to see the missign values and nature, since the others are pure copies
temp[["sac", "east", "american", "sjr", "yolo_toedrain"]].plot(subplots=True)
plt.legend()
plt.show()
