import os
import os.path as osp
import pandas as pd

import matplotlib.pyplot as plt
from vtools import *
from dms_datastore.read_ts import *
from vtools.functions.unit_conversions import ec_psu_25c,CFS2CMS,CMS2CFS
from vtools.functions.error_detect import *
import yaml
from dms_datastore.read_multi import read_ts_repo
from dms_datastore.write_ts import write_ts_csv

lisbon_elev_top = 11.5
lisbon_flow_top = 4000.

def process_yolo_cache_slough():
    """
    Process the southern estimate of flow coming out of the Cache Complex using Cache Slough and (subtracting) Miner.
    Does not account at this point for change in storage or the Toe Drain  vs Bypass difference.
    Southern estimate of total bypass flow.
    """

    # RYE station is the newer station
    cache_ryer = read_ts_repo(station_id='rye', variable='flow',start=sdate,end=edate)
    cache_ryer = cache_ryer.interpolate(limit=60)
    cache_ryer = cosine_lanczos(cache_ryer,hours(40))
    cache_ryer.columns=["value"]

    cache_interp = cache_ryer.interpolate(method="cubic")
    # The flow at RYE/RYE is measured below Miner,
    # so we have to subtract Miner to get the flow
    # out of Yolo to the south
    miner = read_ts_repo(station_id='mir', variable='flow',start=sdate,end=edate)
    miner = miner.interpolate(limit=60)
    miner = cosine_lanczos(miner,hours(40))
    yolo_south = cache_interp.sub(miner.squeeze(),axis=0)
    yolo_south_4d = cosine_lanczos(yolo_south,days(4))

    return yolo_south_4d


def est_yolo_woodland_sacweir(sdate, edate):
    """
    Create the northern estimate (first priority estimate) of total bypass flow. 
    This does not account for the Bypass vs Toe Drain difference. 

    If this sum is low (ie Sac Weir flow is zero and Woodland flow is low),
    it may indicate that more flow is carried by the Toe Drain in which case this
    estimate MAY be usable as an estimate of Toe Drain flow.
    
    If this sum is high (Woodland high, possibly with Sac Weir flow), then 
    this becomes an estimate of the sum of the two Yolo (bypass and toe) and
    the ultimate estimate will probably be obtained by subtracting a Toe Drain 
    estimate.
    """
    interval = minutes(15)

    woodland_flow = read_ts_repo(station_id='yby', variable='flow',start=sdate,end=edate)
    woodland_flow = woodland_flow.interpolate(method='linear',limit=1200)
    woodland_flow.columns = ['value']

    sac_weir_flow = pd.DataFrame(0,index=woodland_flow.index, columns=['value']).to_period() #todo: temporarily use 0 for sac weir
    # sac_weir_flow = read_ts(osp.join(input_dir,config['yolo']['data_sources']['sac_weir_flow']), start=sdate, end=edate).interpolate().to_period()
    sac_weir_flow = rhistinterp(sac_weir_flow+100.,interval,lowbound=0.0,p=12.)-100.
    sac_weir_flow.columns=["value"]

    sac_weir_flow = sac_weir_flow.reindex(woodland_flow.index)
    sac_weir_flow = sac_weir_flow.fillna(0.0)
    return (woodland_flow + sac_weir_flow)


def get_lisbon(sdate, edate):
    """
    create flow and elevation at Lisbon station

    """
    # use read_ts_repo
    lisbon_flow = read_ts_repo(station_id='lis', variable='flow',start=sdate,end=edate)

    lisbon_flow = lisbon_flow.interpolate(limit = 20)[sdate:edate] # todo: hardwired
    lisbon_flow = lisbon_flow.resample('15min').interpolate(limit=3)
    
    # The filling limit on lisbon_elev is far beyond what would be reasonble for
    # a tidal quantity for accuracy, but the times when it is too much are times when
    # lisbon weir is tidal and that will not affect the test of whether it is > 11.6
   
    lisbon_elev = read_ts_repo(station_id='lis', variable='elev',start=sdate,end=edate)
    lisbon_elev = lisbon_elev.interpolate(limit=50)

    return(lisbon_flow, lisbon_elev)



def fill_lisbon_flow(lisbon_flow_unfilled, sdate, edate):
    """
    Fill in gaps in Lisbon flow data with relationship between Lisbon flow and lbtoe flow
    """
    lbtoe_flow = read_ts_repo(station_id='lbtoe', variable='flow',start=sdate,end=edate)

    lisbon_flow_unfilled[lisbon_flow_unfilled.isnull()] = lbtoe_flow[lisbon_flow_unfilled.isnull()] - 200 # todo: simple relationship
    lisbon_flow_filled = lisbon_flow_unfilled.interpolate(limit=20)
    if np.sum(lisbon_flow_filled.isna().values) > 0:
        print("Warning: there are still {} missing values in the Lisbon flow data after calling function fill_lisbon_flow."
              .format(np.sum(lisbon_flow_filled.isna().values)))

    return(lisbon_flow_filled)


def fill_yolototal(yolo_total_raw):
    """
    Fill in gaps in Yolo Total flow data using southern estimate first
    Fill remaining gaps by interpolation
    """
    yolo_total_filled = yolo_total_raw.copy()
    yolo_total_optional = process_yolo_cache_slough().reindex(yolo_total_raw.index)
    yolo_total_filled[yolo_total_raw.isnull()] = yolo_total_optional[yolo_total_raw.isnull()]
    yolo_total_filled = yolo_total_filled.interpolate()
    return(yolo_total_filled)


def adjust_yoloflow(yolo_flow_raw, low_flow):
    """
    Adjust Yolo flow to make sure no negative values
    """
    print("Number of negative Yolo flow value before adjustment: {}".format((yolo_flow_raw < -1.).sum()))
    yolo_flow = yolo_flow_raw.copy()
    yolo_flow[low_flow] = 0.
    yolo_flow[yolo_flow < 0.] = 0.
    return(yolo_flow)


def process_yolo_effective_flow(toe_raw, lisbon_elev, sdate, edate):
    """
    Implement the processing logic of getting the effective Yolo and Toe drain flows
    """
    lisbon_elev.columns = ["value"]; toe_raw.columns = ["value"]

    # full_yolo is the "final" data frame that will hold the effective toe drain and yolo flows
    # as well as some intermediate quantities
    yolo_data_all = toe_raw.copy()
    yolo_data_all.columns = ["toe"]

    is_yolo_active = (lisbon_elev > lisbon_elev_top) | (toe_raw > lisbon_flow_top)
    is_yolo_active = is_yolo_active.reindex(yolo_data_all.index)
    is_yolo_active.ffill(inplace=True)
    yolo_data_all['is_yolo_active'] = is_yolo_active

    # interpolate Toe drain without a limit in gap size and apply only in areas where
    # is_yolo_active is False.
    print("Interpolate Toe Drain without limit where is_yolo_active is not active")
    toeinterp = yolo_data_all.toe.interpolate()
    yolo_data_all.loc[~yolo_data_all.is_yolo_active,"toe"] = toeinterp.where(~yolo_data_all.is_yolo_active)

    # Now add an estimate of yolo total flow, preferred estimate is Woodland + Sac Weir
    # Fill estimated gaps with the southern estimate based on Cache Slough - Miner
    yolo_total_raw = est_yolo_woodland_sacweir(sdate, edate).reindex(yolo_data_all.index)
    yolo_total_filled = fill_yolototal(yolo_total_raw)
    yolo_data_all["yolo_total"] = yolo_total_filled

    # This adjustment keeps the full_yolo interpretation correct, but values
    # that meet these criteria are not used in later computations
    yolo_data_all.loc[~yolo_data_all.is_yolo_active,"yolo_total"] = yolo_data_all.toe[~yolo_data_all.is_yolo_active]

    # adjust effective toe drain flow
    toe_eff = yolo_data_all.toe.clip(upper=4000.)
    toe_eff[is_yolo_active.value & toe_eff.isnull()] = 4000.
    toe_eff[~is_yolo_active.value & toe_eff.isnull()] = yolo_data_all.yolo_total[~is_yolo_active.value & toe_eff.isnull()]
    # Use yolo_total flow to determine whether high flow occurs or not
    full_low = yolo_data_all.yolo_total <= 4000.
    full_high = ~full_low    # recipricol for convenience
    # adjust toe_eff when yolo_total is high
    toe_eff.mask(full_high, toe_eff + 0.05*(yolo_data_all.yolo_total - toe_eff),inplace=True)
    yolo_data_all['toe_eff'] = toe_eff
    # compute effective yolo flow
    yolo_eff_raw = yolo_data_all.yolo_total - yolo_data_all.toe_eff
    yolo_eff = adjust_yoloflow(yolo_eff_raw, full_low)
    yolo_data_all['yolo_eff'] = yolo_eff
    print("Number of negative Yolo flow value after adjustment: {}".format((yolo_eff < -1.).sum()))
    print("Number of gaps in Yolo flow data: {}".format(yolo_data_all['yolo_eff'].isnull().sum()))
    print("Number of gaps in Toe flow data: {}".format(yolo_data_all['toe_eff'].isnull().sum()))
    return(yolo_data_all['toe_eff'], yolo_data_all['yolo_eff'])



if __name__ == '__main__':
    with open("processed_config.yml", 'r') as f:
        config = yaml.safe_load(f)
    sdate = pd.to_datetime(config['yolo']['start_date'])
    edate = pd.to_datetime(config['yolo']['end_date'])
    yolo_outfile = config['yolo']['output'][0]
    ytoe_outfile = config['yolo']['output'][1]
    if sdate < pd.Timestamp(2020,1,1):
        raise ValueError("Start date is before the minimum allowed date 2020-01-01.")

    lisbon_flow_raw, lisbon_elev = get_lisbon(sdate, edate)
    lisbon_flow = fill_lisbon_flow(lisbon_flow_raw, sdate, edate)
    yolo_toe_raw = lisbon_flow.copy()

    toe_final, yolo_final = process_yolo_effective_flow(yolo_toe_raw, lisbon_elev, sdate, edate)
    if toe_final.isnull().any() or yolo_final.isnull().any():
        raise ValueError("There are missing values in the final Toe Drain or Yolo flow data.")

    print("Processing for yolo flow complete.")
    print("Writing output files...\n\
          processed_output/yolo_flow.csv\n\
          processed_output/ytoe_flow.csv")
    script_dir = osp.dirname(osp.abspath(__file__))
    output_dir = osp.join(script_dir, "processed_output")
    os.makedirs(output_dir, exist_ok=True)
    write_ts_csv(toe_final, osp.join(output_dir, ytoe_outfile))
    write_ts_csv(yolo_final, osp.join(output_dir, yolo_outfile))

