#!/usr/bin/env python
import requests
import xarray as xr
import pandas as pd
import os
import numpy as np
import time
import datetime as dtm
import copy
import argparse
from vtools.data.vtime import hours,days,minutes

def create_arg_parser():
    """ Create an argument parser
        return: argparse.ArgumentParser
    """

    # Read in the input file
    parser = argparse.ArgumentParser(
        description="""
        Download hycom ocean model raw opendap data within lat(37,39),lon(236,239),
        and interpolated to hourly data.
        Usage:
        download_hycom --sdate 2020-02-19  --raw_dest /path/to/modeling_data/raw
                     --processed_dest /path/to/modeling_data/raw
                     """)
    parser.add_argument('--sdate', default=None, required=True,
                        help='starting date of HRRR data, must be \
                        format like 2020-02-19')
    parser.add_argument('--raw_dest', default=None, required=True,
                        help='path to store downloaded raw hycom data')
    parser.add_argument('--processed_dest', default=None, required=True,
                        help='path to store interpolated hycom data')
    parser.add_argument('--edate', default=None, required=False,
                        help="end date for the record to be downloaded,\
                            if not given download up to today")

    return parser


    
def hycom_schism_opendap(start=None,end=None,dest=None):
    """ Download hycom  opendap data for all time based on a bounding set of lat/lon 
    
    This particular variant is available from 2019
    """
    url="https://tds.hycom.org/thredds/dodsC/GLBy0.08/expt_93.0/ts3z?lat,lon,time,water_temp,salinity"
    data = xr.open_dataset(url)

    if start is None:
        start = pd.Timestamp(2019,1,1)
    if end is None:
        end = pd.Timestamp.now()
    if dest is None:
        dest = './raw'
        
    if os.path.exists(dest) is False:
        os.mkdir(dest)
        print("Destination path created: %s"%dest)
        
    s = copy.copy(start)
    nnday = (end - start).days+1
    print(nnday)
    print("Start=",start," End=",end," dest=",dest," nday=",nnday)        


    for nday in range(nnday):
        print("Downloading: ",s)
        e = s + days(1)
        #subset = data.sel(time=slice(s.to_datetime64(),e.to_datetime64()),lat=slice(37.2,38.801),lon=slice(236.48,238.021))
        subset = data.sel(time=slice(s.to_datetime64(),e.to_datetime64()),lat=slice(37,39),lon=slice(236,239))
        ##sub['salinity' = sub['salinity']*sub['salinity']get
        #newtime = pd.date_range(s,freq='1H',periods=24)
        #resampled= subset.interp(time=newtime)
        datestr = s.strftime("%Y%m%d")        
        #filename = os.path.join(dest,"hycom_processed_"+datestr+".nc")
        filename = os.path.join(dest,"hycom_raw_"+datestr+".nc")
        subset.to_netcdf(filename,mode='w',\
                           format='NETCDF4_CLASSIC',unlimited_dims=['time'],\
                           encoding = {'salinity': {'_FillValue': -9999.0}, 'water_temp': {'_FillValue': -9999.0}} )
        s = s + days(1)
        e = e + hours(24)
        
def hycom_schism_opendap_alt2(start=None,end=None,dest=None):
    """ Download hycom  opendap data for all time based on a bounding set of lat/lon   
        from a seperate repos. 
    
    This particular variant is available from 8/10/2024
    """
    url="https://tds.hycom.org/thredds/dodsC/ESPC-D-V02/s3z?lat,lon,time,salinity"
    url2="https://tds.hycom.org/thredds/dodsC/ESPC-D-V02/t3z?lat,lon,time,water_temp"

    if start is None:
        start = pd.Timestamp(2024,9,1)
    if end is None:
        end = pd.Timestamp.now()
    if dest is None:
        dest = './raw'

    start_year = start.year 
    end_year = end.year 

    if start_year == end_year: ## if the start and end year are the same, we add year to the urls
        url = f"https://tds.hycom.org/thredds/dodsC/ESPC-D-V02/s3z/{start_year}?lat,lon,time,salinity"
        url2= f"https://tds.hycom.org/thredds/dodsC/ESPC-D-V02/t3z/{start_year}?lat,lon,time,water_temp"

    data = xr.open_dataset(url)
    data2 = xr.open_dataset(url2)


        
    if os.path.exists(dest) is False:
        os.mkdir(dest)
        print("Destination path created: %s"%dest)
        
    s = copy.copy(start)
    nnday = (end - start).days+1
    print(nnday)
    print("Start=",start," End=",end," dest=",dest," nday=",nnday)        

    
    for nday in range(nnday):
        print("Downloading: ",s)
        e = s + days(1)
        #subset = data.sel(time=slice(s.to_datetime64(),e.to_datetime64()),lat=slice(37.2,38.801),lon=slice(236.48,238.021))
        subset = data.sel(time=slice(s.to_datetime64(),e.to_datetime64()),lat=slice(37,39),lon=slice(236,239))
        subset2 = data2.sel(time=slice(s.to_datetime64(),e.to_datetime64()),lat=slice(37,39),lon=slice(236,239))
        ##sub['salinity' = sub['salinity']*sub['salinity']get
        #newtime = pd.date_range(s,freq='1H',periods=24)
        #resampled= subset.interp(time=newtime)
        datestr = s.strftime("%Y%m%d")        
        #filename = os.path.join(dest,"hycom_processed_"+datestr+".nc")
        filename = os.path.join(dest,"hycom_raw_"+datestr+".nc")
        subset.to_netcdf(filename,mode='w',\
                           format='NETCDF4_CLASSIC',unlimited_dims=['time'],\
                           encoding = {'salinity': {'_FillValue': -9999.0}} )
        subset2.to_netcdf(filename,mode='a',\
                           format='NETCDF4_CLASSIC',unlimited_dims=['time'],\
                           encoding = {'water_temp': {'_FillValue': -9999.0}} )
        s = s + days(1)
        e = e + hours(24)


def hycom_schism_opendap_alt():
    """ Alternate interface that seems slower, so this is currently not used"""
    url0="https://tds.hycom.org/thredds/dodsC/GLBy0.08/expt_93.0/ts3z?lat,lon,time,water_temp,salinity"
    
    data = xr.open_dataset('test.nc')
    print("OK")
    url ="http://ncss.hycom.org/thredds/ncss/GLBy0.08/expt_93.0/ts3z?var=salinity&var=water_temp&north=38.801&west=236.4899&east=238.021&south=37.299&disableProjSubset=on&horizStride=1&time=2023-01-10T09%3A00%3A00Z&vertCoord=&addLatLon=true&accept=netcdf4"
    data = None
    with requests.get(url) as response:
        
        with open('test.nc','wb') as fout:
            fout.write(response.read())
            data = xr.open_dataset('test.nc')    
    print(data)    

def process_hycom(start=None, end=None,dest="./processed", raw="./raw"):
    """
    Interpolate hycome data to hourly;
    converte from UTC to PST
    Rename temperature and salinity variable names to be consistent with SCHISM
    
    Parameters
    ----------
    start : TYPE, optional
        e.g, pd.Timestamp(2022,10,1). The default is None.
    end : TYPE, optional
        e.g, pd.Timestamp(2022,10,1). The default is None.
    dest : TYPE, optional
        Destination path. The default is None. =='./processed' when dest=None

    Returns
    -------
    None.

    """
    
    if start is None:
        start = pd.Timestamp(2022,10,1)
    if end is None: # due to conversion from utc to pst, data for the last day is not available
        end = pd.Timestamp.now()
    
    if os.path.exists(dest) is False:
        os.mkdir(dest)
        print("Destination path created: %s"%dest)

    nnday = (end - start).days 
    s = copy.copy(start)

    for nday in range(nnday):
        print("Processing: ",s)
        e = s + days(1)    
        datestr1 = s.strftime("%Y%m%d") 
        datestr2 = e.strftime("%Y%m%d") 
        filename1 = os.path.join(raw,"hycom_raw_"+datestr1+".nc")
        filename2 = os.path.join(raw,"hycom_raw_"+datestr2+".nc")
        dest_fn = os.path.join(dest,"hycom_interpolated_hourly_pst"+datestr1+".nc")
        
        # to convert to PST, we need data from two consecutive days
        raw_nc1 = xr.open_dataset(filename1)
        raw_nc2 = xr.open_dataset(filename2)
        
        if len(np.where(raw_nc1.time.diff(dim='time')!=np.timedelta64(3,'h'))[0] ) !=0: # time inverval different from 3hours
            print("%s has inconsistent time interval different from 3 hours "%filename1)
            
        if len(np.where(raw_nc2.time.diff(dim='time')!=np.timedelta64(3,'h'))[0] ) !=0: # time inverval different from 3hours
            print("%s has inconsistent time interval different from 3 hours "%filename2)
        
        newtime1 = pd.date_range(s,freq='1h',periods=24)
        resampled1 = raw_nc1.interp(time=newtime1)
        newtime2 = pd.date_range(e,freq='1h',periods=24)
        resampled2 = raw_nc2.interp(time=newtime2)
            
        resampled1['time'] = resampled1.time - pd.Timedelta(8,'h')
        resampled2['time'] = resampled2.time - pd.Timedelta(8,'h')     
        
        subset1 = resampled1.sel(time=slice(s.to_datetime64(),e.to_datetime64()-
                                            hours(1)))
        subset2 = resampled2.sel(time=slice(s.to_datetime64(),e.to_datetime64()-
                                            hours(1)))
        
        merged_set = xr.concat([subset1,subset2],dim='time')
        merged_set = merged_set.rename({'salinity':'salt','water_temp':'temp'})
        merged_set.time.attrs = {'timezone':'modified from utc to pst'}
        merged_set.to_netcdf(dest_fn,mode='w',\
                             format='NETCDF4_CLASSIC',unlimited_dims=['time'],\
                             encoding = {'salt': {'_FillValue': -9999.0}, 
                                         'temp': {'_FillValue': -9999.0}} )
        
        assert(merged_set.time.shape[0]==24) # This can occur for the last day downloaded due to conversion from utc to pst.            
        s = s + days(1)

def main():   
    parser = create_arg_parser()
    args = parser.parse_args()
    raw_dest = args.raw_dest
    processed_dest = args.processed_dest
    end_date = args.edate
    
    start_date = pd.to_datetime(args.sdate, format='%Y-%m-%d')
    if end_date is None:
        end_date = pd.Timestamp.today()
    else:
        end_date = pd.to_datetime(args.edate, format='%Y-%m-%d')
    hycom_schism_opendap_alt2(start_date,end_date,raw_dest)
    process_hycom(start_date,end_date,processed_dest,raw_dest)

if __name__ == '__main__':
    main()


