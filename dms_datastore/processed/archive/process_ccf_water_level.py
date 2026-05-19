# -*- coding: utf-8 -*-
"""
Created on Tue Jun 17 09:03:21 2025

@author: smunger

# Interior water levels at Clifton Court reported by SCADA in units of feet NAVD88.,
# These were adjusted to NAVD88 by adding the NGVD29 to NAVD88 conversion of 2.56 ft
# Following the calibration adjustment from survey dated of 4/15/2024 
# 0.27 ft is subtracted from the upstream gage (2.57ft-0.27ft)
# 0.22 ft is added to the downstream gage (2.57ft+0.22ft)
# convert to PST

"""
import pandas as pd
import matplotlib.pyplot as plt

path_fn =r'\\nasbdo\Modeling_Data\clifton_court\ccf_water_levels_wonderware_2020_2024.csv'


def to_pst(ts):
    """ Convert to PST
    
    Parameters
    ----------
    infile : str
    path to the Wonderware file
    """    
    pst = 'ETC/GMT+8'
    pdt = 'US/Pacific'


    ts.index = ts.index.tz_localize(pdt, nonexistent='shift_forward',ambiguous="infer").tz_convert(pst)
    ts.index = ts.index.floor("1min")
    ts = ts[~ts.index.duplicated(keep="first")]
    ts.index = ts.index.tz_localize(None)
    return ts



ts_raw = pd.read_csv(path_fn,index_col=0,parse_dates=[0],na_values=["(null)"])
ts = to_pst(ts_raw)
ts.columns=["ccf_up","ccf_down"]

# make datum and instrument correction
ts['ccf_up'] = ts['ccf_up']+2.57-0.27 
ts['ccf_down'] = ts['ccf_down']+2.57+0.22

# Remove outliers -- pretty agressively. 
ts['ccf_up'] = ts['ccf_up'].mask((ts.ccf_up>8.5) | (ts.ccf_up<-0.5))
ts['ccf_down'] = ts['ccf_down'].mask((ts.ccf_down>4.5) | (ts.ccf_down<0))

ts.to_csv("dwr_ccf_waterlevels_NAVD88_2020_2024.csv")
ts.plot()
plt.show()