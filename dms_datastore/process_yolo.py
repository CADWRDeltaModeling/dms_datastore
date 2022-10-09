#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os.path as osp
import pandas as pd

import matplotlib.pyplot as plt
from vtools.functions.interpolate import *
from vtools.functions.filter import *
from vtools.data.vtime import *
from dms_datastore.read_ts import * 
from schimpy.unit_conversions import ec_psu_25c,CFS2CMS,CMS2CFS
from vtools.functions.error_detect import *
import pyhecdss

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
        
def process_yolo(outfname,sdate,edate,do_plot=True,do_lis_plot=False,do_bypass_plot=False):

    interval = minutes(15)
    continuous_repo = '//cnrastore-bdo/Modeling_Data/continuous_station_repo/'
    usgs_dir = continuous_repo
    wdl_dir = continuous_repo

    #cdec_dir = "data/cdec_download"
    #dsm2_file="D:/Delta/dsm2_v8/timeseries/hist201912.dss"
    #alt="DWR-DMS-201912"

    # Periods of record
    # RYI: 2003-02-19 to 2019-04-29
    # RYE: 2018-07-03 to Now
    # HWB (Miner): 2003-05-20 to Now
    # Sacweir: Very long term


    # This is the RYI station, replaced in roughly 2018-2019 by RYE
    cache = read_ts(osp.join(usgs_dir,'usgs_ryi_11455350_flow_*.rdb'))
    cache = cache.interpolate(limit=60)  # 60 is way more than we would usually allow 
                                         # when we apply a filter -- we do it here given
                                         # that completeness is more important than 
                                         # fidelity during tidal periods, when this station probably
                                         # won't be used anyhow
    cache = cosine_lanczos(cache,hours(40))
    cache.columns = ["value"]

    # This is the newer station. 
    cache_ryer = read_ts(osp.join(usgs_dir,'usgs_rye_11455385_flow_*.rdb'))
    cache_ryer = cache_ryer.interpolate(limit=60)
    cache_ryer = cosine_lanczos(cache_ryer,hours(40))
    cache_ryer.columns=["value"]
    cache = ts_merge([cache_ryer,cache])  #RYI-RYE priority in the merge is arbitrary

    # There are some hydrologic peaks missing in RYI, particularly in 2017
    # Linear inteprolation will not catch it. Cubic seems fine, but
    # may require some supervision
    cache_interp = cache.interpolate(method="cubic")


    # Miner slough is HWB. 
    #The flow at RYE/RYE is measured below Miner, 
    # so we have to subtract Miner to get the flow out of the bypass
    # out of Yolo to the south
    #miner = read_ts(osp.join(usgs_dir,"usgs_hwb_11455165_flow_*.rdb"))
    miner = read_ts(osp.join(usgs_dir,"usgs_hwb_11455165_flow_*.rdb"))
    miner = miner.interpolate(limit=60)
    miner = cosine_lanczos(miner,hours(40))
    yolo_south = cache_interp.sub(miner.squeeze(),axis=0)
    yolo_south_4d = cosine_lanczos(yolo_south,days(4))

    # This is the (best) fit to data in 2016 for Mercury project
    # It accounts for the rate of storage on the bypass
    yolo2016 = pd.read_csv("data/yolo_project_2016/yolo_project_2016.th",sep="\s+",header=0,parse_dates=[0],index_col=0)
    yolo2016 = (-yolo2016.yolo*CMS2CFS).to_frame()
    yolo2016.columns= ["value"]


    # Sacweir. The USGS readers probably don't work for daily data so this is a brittle alternative.
    sacweir = read_ts(osp.join('.',"usgs_ssw2_11426000_flow_2000_2022.rdb")).interpolate().to_period()
    sacweir.columns = ['value']
    sacweir = rhistinterp(sacweir+100.,interval,lowbound=0.0,p=12.)-100.
    sacweir.columns=["value"]

    woodland = read_ts("usgs_yby_11453000_flow_*.rdb")
    woodland = woodland.interpolate(method='linear',limit=1200)
    woodland.columns = ['value']
    sacweir.reindex(woodland.index)
    sacweir.fillna(0.0)
    woodland = woodland + sacweir
    woodinterp = woodland.interpolate()
    woodland.loc[pd.Timestamp(2019,1,16):pd.Timestamp(2019,1,26)] = woodinterp

    lis_flow_fname = "ncro_lis_b91560q_flow_*.csv"
    lis_elev_fname = "ncro_lis_b91560_elev_*.csv"
    lisbon1 = read_ts(osp.join(wdl_dir,lis_flow_fname))

    lisbon1 = med_outliers(lisbon1,range=(-1000.,5000.))  # was scale=50
    lisbon1 = lisbon1.interpolate(limit = 20)[sdate:edate]
    lisbon1 = lisbon1.resample('15T').interpolate(limit=3)
    lisbon_elev1 = read_ts(osp.join(wdl_dir,lis_elev_fname))


    # This is the same data, but NCRO data doesn't extend all the way to real time
    # Some CDEC data is also invalid because it is out of bank flow. So minimizing its use
    # Discover how far it does go
    end_of_ncro = lisbon1.last_valid_index()
    end_of_ncro_elev = lisbon_elev1.last_valid_index()


    lis_cdec_fname = "cdec_lis_*flow_*9999.csv"
    lisbon2 = read_ts(osp.join(wdl_dir,lis_cdec_fname))
    lisbon2 = med_outliers(lisbon2,range=(-1000.,5000.))
    lisbon2= lisbon2.interpolate(limit = 20)[end_of_ncro:edate]
    lisbon2.columns=["value"]
    lisbon1 = ts_merge([lisbon1,lisbon2])
    lisbon1.plot()
    plt.title("Debug")
    plt.show()


    lis_cdec_elev_fname = "cdec_lis_*elev_*9999.csv"
    lisbon_elev2 = read_ts(osp.join(wdl_dir,lis_cdec_elev_fname))  
    lisbon_elev2 = med_outliers(lisbon_elev2,range=(-5.,25.))
    lisbon_elev2= lisbon_elev2.interpolate(limit = 20)[end_of_ncro_elev:edate]
    lisbon_elev2.columns=["value"]
    lisbon_elev1 = ts_merge([lisbon_elev1,lisbon_elev2])
    print(lisbon_elev1)


    #Enumerate fixes since 2008
    toe = lisbon1.copy()
    NO_FILL = 0
    TIDAL = -2
    INTERP = -1
    WOODLND = 1
    lis_do_fill = (toe*0.).squeeze() 
    lis_do_fill[:] = 0.

     # When to fill toe with woodland. -1 = interpolate self, +1 = from Woodland Initially force zero
    lis_do_fill.loc[pd.Timestamp(2008,1,2):pd.Timestamp(2008,1,8)] = INTERP
    lis_do_fill.loc[pd.Timestamp(2008,1,26):pd.Timestamp(2008,2,17)] = WOODLND
    lis_do_fill.loc[pd.Timestamp(2008,5,28):pd.Timestamp(2008,5,30)] = TIDAL
    lis_do_fill.loc[pd.Timestamp(2009,1,23): pd.Timestamp(2009,1,27)] = INTERP
    lis_do_fill.loc[pd.Timestamp(2009,9,8):pd.Timestamp(2009,9,15)] = TIDAL
    lis_do_fill.loc[pd.Timestamp(2010,1,21):pd.Timestamp(2010,2,22)] = WOODLND
    lis_do_fill.loc[pd.Timestamp(2010,9,24):pd.Timestamp(2010,10,2)] = TIDAL
    lis_do_fill.loc[pd.Timestamp(2010,11,24):pd.Timestamp(2010,12,15)] = INTERP
    lis_do_fill.loc[pd.Timestamp(2010,12,17):pd.Timestamp(2011,1,26)] = WOODLND
    #lis_do_fill.loc[pd.Timestamp(2011,3,16):pd.Timestamp(2011,3,22)] = LIMIT
    lis_do_fill.loc[pd.Timestamp(2012,7,3):pd.Timestamp(2012,8,1)] = TIDAL
    lis_do_fill.loc[pd.Timestamp(2012,7,3):pd.Timestamp(2012,8,1)] = TIDAL
    lis_do_fill.loc[pd.Timestamp(2012,12,1):pd.Timestamp(2013,1,9)] = WOODLND
    lis_do_fill.loc[pd.Timestamp(2014,12,17):pd.Timestamp(2015,1,1)] = WOODLND
    lis_do_fill.loc[pd.Timestamp(2015,3,23):pd.Timestamp(2015,4,5)] = INTERP
    lis_do_fill.loc[pd.Timestamp(2018,7,26):pd.Timestamp(2018,7,25)] = TIDAL
    lis_do_fill.loc[pd.Timestamp(2019,1,16):pd.Timestamp(2019,1,26)] = INTERP
    lis_do_fill.loc[pd.Timestamp(2019,6,8):pd.Timestamp(2022,2,1)] = INTERP
    lis_do_fill.loc[pd.Timestamp(2020,2,3):pd.Timestamp(2020,3,23)] = WOODLND

    # Toe drain gaps and defects may be filled by interpolation, by a tidal method
    # (currently not implemented -- we identified the places we want to do it, but still use interpolation)
    # or for middling values fill with Woodland data

    toe.loc[lis_do_fill == INTERP] = toe.interpolate().loc[lis_do_fill==INTERP]
    toe.loc[lis_do_fill == TIDAL] = toe.interpolate().loc[lis_do_fill==TIDAL]
    woodupdate=toe.copy()
    woodupdate.update(woodland,overwrite=True)
    toe.loc[lis_do_fill == WOODLND] = woodupdate.loc[lis_do_fill == WOODLND]

    
    # Now create the Yolo time series and mask for when Yolo Bypass flow is assumed
    lisbon_elev1.columns = ["value"]; toe.columns = ["value"] ; lisbon1.columns = ["value"] # needed for concat
    yolo = woodland.copy()
    # Adjust for the 2016 study data, which are more careful about water stored on Bypass for that year
    yolo.loc[pd.Timestamp(2016,3,11):pd.Timestamp(2016,4,1)]=yolo2016.loc[pd.Timestamp(2016,3,11):pd.Timestamp(2016,4,1)]
    yolo.columns = ["value"]
    
    # Full is the "final" data frame that will hold the effective toe drain and yolo flows 
    # as well as some intermediate quantities
    full = pd.concat([toe,yolo],axis=1)
    full.columns=["toe","yolo"]
    
    use_yolo = (lisbon_elev1 > 11.5) | (lisbon1 > 4000.) | (toe > 4000.) | (yolo>4000.)
    use_yolo = use_yolo.reindex(full.index)
    use_yolo.fillna(method="ffill",inplace=True)
    use_yolo.loc[pd.Timestamp(2019,1,16):pd.Timestamp(2019,1,26)] = False
    full['use_yolo'] = use_yolo

    # Since we are getting closer to final product and cannot tolerate missing values,
    # interpolate Toe drain without a limit in gap size and apply only in areas where
    # use_yolo is False.
    toeinterp = toe.interpolate()
    toe = toe.where(use_yolo,toeinterp)

    # Yolo bypass flow will be zero when use_yolo is False
    full.loc[~use_yolo.value,'yolo'] = 0.

    
    # Now add a measure of total Bypass flow, which will be the maximum of the Yolo and Toe Drain
    full["yolo_bypass"]=full.max(axis=1)   # Total Bypass flow which includes Toe and Yolo flow pathways
    full.loc[~full.use_yolo,"yolo_bypass"] = full.toe[~full.use_yolo]

    # Todo: how would this differ from use_yolo?
    full_low = full.yolo_bypass <= 4000.
    full_high = ~full_low    # recipricol for convenience
    
    toe_eff=full.toe.clip(upper=4000.)
    toe_eff[use_yolo.value & toe_eff.isnull()] = 4000.    # Often values above 4000 will be marked as nan
                                                          # This should mostly pick the correct instances and mark them as 4000.
    toe_eff[full_high] = toe_eff + 0.05*(full.yolo_bypass - toe_eff)    # Allocate 5% of excess Bypass flow to Toe Drain channel
    full['toe_eff'] = toe_eff
    yolo_eff = full.yolo_bypass - full.toe_eff                   # Yolo carries the part of Total Bypass flow that is not routed down Toe Drain
    yolo_eff[full_low] = 0.                               # Yolo  is zero when the Total Bypass flow including Yolo/Toe is small
    full['yolo_eff'] = yolo_eff

    ax=full.plot()
    #ax.legend(["toe","yolo","full"])

    plt.title("Output and some inputs")
    plt.show()




    fig,(ax0,ax1) = plt.subplots(2,sharex=True)
    toe.plot(ax=ax0,linewidth=3,color="0.45")
    lisbon1.plot(ax=ax0)
    #lisbon.plot(ax=ax0)
    #cache_interp.plot(ax=ax0)
    #cache.plot(ax=ax0)
    yolo.plot(ax=ax0)
    #yolo1.plot(ax=ax0)
    woodland.plot(ax=ax0)
    #(woodupdate-15).plot(ax=ax0)

    #yolo2016.plot(ax=ax0)
    lisbon_elev1.plot(ax=ax1)
    #ax.grid()
    ax0.grid()
    ax0.set_ylabel("cfs")
    #ax0.legend(["Toe","Lisbon","woodland"])
    ax0.legend(["Toe","Lisbon WDL","Yolo","Woodland"])
    #ax0.legend(["Lisbon WDL","cache_interp","cache","yolo south/miner","woodland","yolo2016"])
    ax1.grid()
    ax1.set_ylabel("ft")
    plt.show()
    unitstr = ["cfs","cms"]
    
    
    multipliers = [1.,CFS2CMS]
    for unitstring,multiplier in zip(unitstr,multipliers): 
        outfile = outfname.replace(".csv",f"_{unitstring}.csv")
        with open(outfile,'w',newline='\n') as outf:
            outf.write("# Calculated Model Inputs for Yolo Toe Drain and Yolo Bypass")
            outf.write("# Units: cfs\n")
            outf.write("# yolo_toedrain: discharge in Toe Drain\n")
            outf.write("# yolo: remaining discharge in Yolo Bypass during flood events\n")
            output=full[["toe_eff","yolo_eff"]]*multiplier
            output.columns=["yolo_toedrain","yolo"]
            describe_null(output.yolo,"yolo")
            describe_null(output.yolo_toedrain,"yolo_toedrain")
            output.loc[sdate:edate,:].to_csv(outf,header=True,index=True,date_format="%Y-%m-%dT%H:%M",float_format="%.2f")
      
    



def main():
    outfile = "yolo_v20220202.csv"
    sdate = pd.Timestamp(2005,1,1)  # Start date of processed data. Earlier than 2005 will require new data and approach
    edate = pd.Timestamp(2022,2,1)
    edate = pd.Timestamp.now()
    # The following two flags will produce diagnostic info. If a problem is revealed, the script output probably is bogus but you will know
    # how to fix it. Please refer to comments above "FIXES" for LIsbon and Yolo
    do_lis_plot = False    #  Do plots of Lisbon that may help decide how to fill this station
    do_bypass_plot = False # Do plots that compare the north (Woodland+Sac Weir) to Southern (RYE/RYI - Miner (HBW) approaches to help decids 
                            # if they need filling/manipulation    
    do_plot = True    # Plot the results
    process_yolo(outfile,sdate,edate,do_plot,do_lis_plot,do_bypass_plot)


if __name__ == '__main__':
    main()
