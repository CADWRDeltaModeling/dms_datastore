#!/usr/bin/env python
import argparse
import ssl
import requests
import pandas as pd
import re
import zipfile
import os
import io
import string
import datetime as dt
import numpy as np
import concurrent
import concurrent.futures
import time
import json
from dms_datastore import read_ts
from dms_datastore.write_ts import write_ts_csv
from dms_datastore.process_station_variable import (
    process_station_list,
    stationfile_or_stations
)

from dms_datastore import dstore_config
from dms_datastore.logging_config import logger 


# station_number,station_type,start_time,end_time,parameter,output_interval,download_link

mappings = {
    "Water Temperature": "temp",
    "Stage": "elev",
    "Conductivity": "ec",
    "Electrical Conductivity at 25C": "ec",
    "Fluorescent Dissolved Organic Matter": "fdom",
    "Water Temperature ADCP": "temp",
    "Dissolved Oxygen": "do",
    "Chlorophyll": "cla",
    "Dissolved Oxygen (%)": None,
    "Dissolved Oxygen Percentage": None,
    "Velocity": "velocity",
    "pH": "ph",
    "Turbidity": "turbidity",
    "Flow": "flow",
    "Salinity": "salinity",
    "ECat25C": "ec",
    "StreamFlow": "flow",
    "WaterTemp": "temp",
    "WaterTempADCP": "temp",
    "DissolvedOxygenPercentage": None,
    "StreamLevel": "elev",
    "WaterSurfaceElevationNAVD88": "elev",
    "fDOM": "fdom",
}


mapping_df = pd.DataFrame(list(mappings.items()), columns=["src_var_id", "var_name"])
mapping_df['src_name'] = 'ncro'

def similar_ncro_station_names(site_id):
    """This routine is here to convert a single site_id to a short list of related names.
       The reason for the routine is that NCRO surface water stations identifiers
       don't correspond well to our abstraction of a station.
       There are station ids that are stripped down B1234, or that have added 00 digits B9123400
       or that have added Q B91234Q
    """
    if site_id.lower().endswith("q"):
        base_id = site_id[:-1]
    elif site_id.lower().endswith("00") and len(site_id)>6:
        base_id = site_id[:-2]
    else:
        base_id = site_id
    return [base_id.upper(), base_id.upper()+"Q", base_id.upper()+"00"]




ncro_inventory_file = "ncro_por_inventory.txt"
ncro_inventory = None
inventory_dir = os.path.split(__file__)[0]
inventoryfile = os.path.join(inventory_dir,"ncro_inventory_full.csv")

def load_inventory():
    global ncro_inventory,inventoryfile
    
    if ncro_inventory is not None:
        return ncro_inventory
    if os.path.exists(inventoryfile) and (time.time() - os.stat(inventoryfile).st_mtime) < 6000.:
        ncro_inventory = pd.read_csv(inventoryfile,header=0,sep=",",\
              parse_dates=["start_time","end_time"])
        return(ncro_inventory)
        
    url = "https://wdlhyd.water.ca.gov/hydstra/sites"
    
    dbase = dstore_config.station_dbase()
    dbase = dbase.loc[dbase['agency'].str.contains('ncro'),:]


    session = requests.Session()
    response = session.get(url) #,verify=False, stream=False,headers={'User-Agent': 'Mozilla/6.0'})
    response.encoding = 'UTF-8'
    inventory_html = response.content.decode('utf-8')
    fio = io.StringIO(inventory_html)
    data = json.load(fio)
    sites = data['return']['sites']
    sites_df = pd.DataFrame(sites)

    dfs = []
    for id,row in dbase.iterrows():
        agency_id = row.agency_id
        origname = agency_id
        names = similar_ncro_station_names(origname)

        url2 = f"https://wdlhyd.water.ca.gov/hydstra/sites/{','.join(names)}/traces"
        response = session.get(url2) #,verify=False, stream=False,headers={'User-Agent': 'Mozilla/6.0'})
        response.encoding = 'UTF-8'
        inventory_html = response.content.decode('utf-8')
        fio2 = io.StringIO(inventory_html)
        data2 = json.load(fio2)

        
        # Flatten the JSON
        flattened_data = []
        for site in data2['return']['sites']:
            if site is None or not 'site' in site:
                json.dump(f'examplebad_{origname}.json',data2)
                logger.info("Bad file for {origname}")
                continue
            else:
                site_name = site['site']
            for trace in site['traces']:
                trace_data = trace.copy()  # Avoid modifying the original JSON
                trace_data['site'] = site_name
                flattened_data.append(trace_data)

        df2 = pd.DataFrame(flattened_data)
        if df2.empty: 
            continue

        df2 = df2.loc[df2['trace'].str.endswith("RAW"),:]
        df2['start_time'] = pd.to_datetime(df2.start_time)
        df2['end_time'] = pd.to_datetime(df2.end_time)
        dfs.append(df2)

    df_full = pd.concat(dfs,axis=0)
    df_full = df_full.reset_index(drop=True)
    df_full.index.name = "index"
    df_full.to_csv(inventoryfile)
    ncro_inventory = df_full
    return df_full

def download_trace(site,trace,stime,etime):
    """Download time series trace associated with one request"""
    url_trace = f"https://wdlhyd.water.ca.gov/hydstra/sites/{site}/traces/{trace}/points?start-time={stime.strftime('%Y%m%d%H%M%S')}&end-time={etime.strftime('%Y%m%d%H%M%S')}"
    max_attempt = 4
    session = requests.Session()
    
    attempt = 0
    while attempt < max_attempt:
        attempt = attempt + 1
        try:
            if attempt > 16:
                logger.info(f"{station_id} attempt {attempt}")
                if attempt > 16:
                    logger.info(fname)
            logger.info(f"Submitting request to URL {url_trace} attempt {attempt}")
            #time1=time.time()
            streaming = False
            response = session.get(url_trace, stream=streaming,timeout=200)
            response.raise_for_status()
            if streaming:
                pass
                #station_html = ""
                #for chunk in response.iter_lines(chunk_size=4096):  # Iterate over lines
                #    if chunk:  # Filter out keep-alive new chunks
                #        station_html += chunk.decode()+"\n" 
            station_html = response.text.replace("\r", "")            
            break
        except Exception as e:
            logger.info("Exception: " + str(e) )
            if attempt == max_attempt:
                return None
            else:
                time.sleep(1)  # Wait one second more second each time to clear any short term bad stuff
    return station_html

def parse_json_to_series(json_txt):
    jsdata = json.loads(json_txt)
    
    traces = jsdata['return']['traces']
    if len(traces) > 1: raise ValueError("Multiple trace json responses not supported")

    # Preallocate lists for columns
    sites, times, values, qualities = [], [], [], []

    # Populate the lists efficiently
    for trace_entry in traces:
        site = trace_entry['site']
        site_details = trace_entry['site_details']
        trace = trace_entry['trace']
        trace_details = trace_entry['trace_details']
        
        for record in trace:
            times.append(record['t'])
            values.append(record['v'])
            qualities.append(record['q'])

        # Create DataFrame directly from lists
    df = pd.DataFrame({
        'datetime': pd.to_datetime(times, format='%Y%m%d%H%M%S'),  # Vectorized timestamp parsing
        'value': pd.to_numeric(values,errors='coerce'),  # Vectorized conversion to float
        'qaqc_flag': qualities
    })

    # Set 't' as the index
    df.set_index('datetime', inplace=True)

    return site,site_details,trace_details,df

def ncro_metadata(station_id,agency_id,site_details,trace_details,paramname):
    meta = {}
    meta["provider"] = "DWR-NCRO"
    meta["station_id"] = station_id
    meta["agency_station_id"] = agency_id
    meta["agency_station_name"] = site_details['name']
    meta["agency_unit"] = trace_details['unit']
    meta["agency_param_desc"] = trace_details['desc']    
    meta["param"] = paramname
    return meta 


def ncro_download(stations,dest_dir,start,end=None,param=None,overwrite=False):
    """ Download robot for NCRO
    Requires a list of stations, destination directory and start/end date
    """
    
    if end == None: end = dt.datetime.now()
    if not os.path.exists(dest_dir):
        os.mkdir(dest_dir) 
    
    failures = []
    skips = []
    inventory = load_inventory()
    dbase = dstore_config.station_dbase()

    for ndx,row in stations.iterrows():
        agency_id = row.agency_id
        station = row.station_id
        param = row.src_var_id
        paramname = row.param
        subloc = row.subloc

        stime = pd.to_datetime(start) 
        try: 
            etime=pd.to_datetime(end)
        except:
            etime=pd.Timestamp.now()
        found = False

        subinventory = inventory.loc[(inventory.site.isin(similar_ncro_station_names(row.agency_id))) & 
                                     (inventory.param == param) & 
                                     (inventory.start_time <= etime) & 
                                     (inventory.end_time >= stime), : ]
        for tsndx,tsrow in subinventory.iterrows():
            site = tsrow.site
            trace = tsrow.trace
            txt = download_trace(site,trace,stime,etime)

            if txt is not None:
                logger.debug("Query produced trace")
                site,site_details,trace_details,df = parse_json_to_series(txt)
                fname = f"ncro_{row.station_id}_{site}_{paramname}_{stime.year}_{etime.year}.csv".lower()
                fpath = os.path.join(dest_dir, fname)
                meta = ncro_metadata(row.station_id,agency_id,site_details,trace_details,paramname)                    
                write_ts_csv(df,fpath,metadata=meta,
                             chunk_years=False,format_version="dwr-ncro-json")

            else:
                logger.debug(f"Empty return")
          
 

def create_arg_parser():
    parser = argparse.ArgumentParser("Download NCRO data")
   
    parser.add_argument('--dest', dest = "dest_dir", default="ncro_download", help = 'Destination directory for downloaded files.')
    parser.add_argument('--start',default=None,help = 'Start time, format 2009-03-31 14:00')    
    parser.add_argument('--end',default = None,help = 'End time, format 2009-03-31 14:00')
    parser.add_argument('--param',default = None, help = 'Parameter(s) to be downloaded.')
    parser.add_argument('--stations', default=None, nargs="*", required=False,
                        help='Id or name of one or more stations.')
    parser.add_argument('stationfile',nargs="*", help = 'CSV-format station file.')
    parser.add_argument('--overwrite', action="store_true", help =  
    'Overwrite existing files (if False they will be skipped, presumably for speed)')
    return parser




def main():
    parser = create_arg_parser()
    args = parser.parse_args()
    destdir = args.dest_dir
    stationfile = args.stationfile
    overwrite = args.overwrite
    start = args.start
    end = args.end
    if start is None: 
        stime = pd.Timestamp(2024,1,1)
    else:
        stime = dt.datetime(*list(map(int, re.split(r'[^\d]', start))))
    if end is None:
        etime = dt.datetime.now()
    else:
        etime = dt.datetime(*list(map(int, re.split(r'[^\d]', end))))
    param = args.param

    stationfile=stationfile_or_stations(args.stationfile,args.stations)
    slookup = dstore_config.config_file("station_dbase")
    vlookup = mapping_df
    #vlookup = dstore_config.config_file("variable_mappings")            
    df = process_station_list(stationfile,param=param,station_lookup=slookup,
                                  agency_id_col="agency_id",param_lookup=vlookup,source='ncro')

    ncro_download(df,destdir,stime,etime,overwrite=overwrite)  

def test():
    destdir = "."
    overwrite = True
    stime = pd.Timestamp(2015,1,1)
    etime = dt.datetime.now()
    params = ["do","elev","flow","velocity","ph","cla","turbidity","temp"]
    params = ["fdom"]
    params = ["ssc"]
    for param in params:
        stations = ["orm","old","oh1","bet"]
        stationfile=stationfile_or_stations(stationfile=None,stations=stations)
        slookup = dstore_config.config_file("station_dbase")
        vlookup = mapping_df
        #vlookup = dstore_config.config_file("variable_mappings")            
        df = process_station_list(stationfile,param=param,station_lookup=slookup,
                                  agency_id_col="agency_id",param_lookup=vlookup,source='ncro')
        ncro_download(df,destdir,stime,etime,overwrite=overwrite)  

def test_read():
    fname = "ncro_old_b95380_temp_2015_2024.csv"
    fname = "ncro_orm_b95370_cla_*.csv"
    ts = read_ts.read_ts(fname)
    print(ts)

if __name__ == "__main__":
    main()
    #test()
    #test_read()
    #df = load_inventory()
    #print(df)
    #download_ncro_period_record(df,dbase,dest="",variables=None)    