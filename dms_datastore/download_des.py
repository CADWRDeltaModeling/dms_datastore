#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Download robot for Nationla Water Informaton System (NWIS)
    The main function in this file is nwis_download. 
    
    For help/usage:
    python nwis_download.py --help
"""
import argparse
import os
import datetime as dt
import time
import re
import ssl
import requests
import io
from dms_datastore.process_station_variable import process_station_list,stationfile_or_stations
from dms_datastore import dstore_config
import pandas as pd
from dms_datastore.logging_config import logger 

__all__=["des_download"]

des_local_dir = os.path.split(__file__)[0]



# These are the current flag definitions for both EMP and MARSH as of 9/14/2020.
# There is no web service to retrieve this information.
_flag_dict = {
    'A': 'Added Filler Data',
    'X': 'Bad Data',
    'B': 'Bad Qualified',
    'G': 'Good Data',
    'M': 'Missing Data',
    'Q': 'Qualified Good Data',
    'U': 'Unchecked'}

def is_unique(s):
    a = s.to_numpy() # s.values (pandas<0.24)
    return (a[0] == a).all()

def open_url_no_ssl_cert_check(url):
    """
    returns a url open handle without SSL certification checks
    """
    return io.BytesIO(requests.get(url, verify=False).content) #  needed to bypass SSL certification checks

def create_arg_parser():
    parser = argparse.ArgumentParser()
   
    parser.add_argument('--dest', dest = "dest_dir", default="des_download", help = 'Destination directory for downloaded files.')
    parser.add_argument('--start',default=None,help = 'Start time, format 2009-03-31 14:00')    
    parser.add_argument('--end',default = None,help = 'End time, format 2009-03-31 14:00')
    parser.add_argument('--param',default = None, help = 'Parameter(s) to be downloaded.')
    parser.add_argument('--stations', default=None, nargs="*", required=False,
                        help='Id or name of one or more stations.')
    parser.add_argument('stationfile',nargs="*", help = 'CSV-format station file.')
    parser.add_argument('--overwrite', action="store_true", help =  
    'Overwrite existing files (if False they will be skipped, presumably for speed)')
    return parser

def query_station_data(program_id,result_id,start,end):
    # download, parse, and write the data
    if pd.isnull(end): end = pd.Timestamp.now()
    url = f'https://dwrmsweb0263.ad.water.ca.gov/TelemetryDirect/api/Results/ResultData?program={program_id}' \
          f'&resultid={result_id}&start={start:%Y-%m-%d:%H:%M:%S}&end={end:%Y-%m-%d:%H:%M:%S}&version=1'
    logger.info('url=' + url)
    data_df = pd.read_csv(open_url_no_ssl_cert_check(url), parse_dates=['time'], index_col='time', sep='|', encoding="utf-8", dtype={"value":float})
    data_df.sort_index(inplace=True)
    data_df['qaqc_flag_desc'] = data_df['qaqc_flag_id'].map(_flag_dict)
    data_df = data_df.filter(['value', 'qaqc_flag_id', 'qaqc_flag_desc'], axis=1)
    return data_df

def write_ts(fpath,df,meta):
    with open(fpath,'w',encoding="utf-8") as fout:
        for item in meta.keys():
            fout.write(f"# {item} : {meta[item]}\n")
        df.to_csv(fout,sep=",",header=True,lineterminator="\n",date_format="%Y-%m-%dT%H:%M")


des_unit_map = {"ÂµS/cm":"microS/cm",
                "µS/cm":"microS/cm",
                "μS/cm at 25°C":"microS/cm",
                "°C":"deg_c",
                "°F":"deg_f","CFS":"ft^3/s",
                "ft (MSL)":"feet","inches":"inches",
                "ft/s":"ft/s","W/m2":"Wm^-2",
                "Âµg/L":"ug/l","µg/L":"ug/l","μg/L":"ug/l",
                "mg/L":"mg/l",
                "1":"psu","% saturation":"% saturation",
                "NTU":"NTU","FNU":"FNU",
                "mph":"mph","Degrees":"deg",
                "ft":"feet","km/h":"km/h",
                "pH Units":"pH","Cal/cm2/min":"Cal/cm2/min"}
                


def des_metadata(station_id,agency_id,rid):
    meta = {}
    meta["provider"] = "DWR-DES"
    meta["station_id"] = station_id
    meta["agency_station_id"] = agency_id  
    meta["agency_result_id"] = rid.result_id
    meta["agency_analyte_name"] = rid.analyte_name
    meta["agency_probe_depth"] = rid.probe_depth
    meta["agency_unit_name"] = rid.unit_name
    meta["agency_equipment_name"] = rid.equipment_name
    meta["agency aggregate_name"] = rid.aggregate_name
    meta["agency_interval_name"] = rid.interval_name
    meta["agency_station_name"] = rid.station_name
    meta["source"] = 'https://dwrmsweb0263.ad.water.ca.gov/TelemetryDirect/api/Results/ReadingDates'
    return meta 


def inventory(program_name, program_id):
    '''
    Obtain an inventory of stored datastreams for all stations in a program. Datastreams are different for each instrument

    Arguments:
    ----------
        program_name is the name of the program (EMP or Marsh)
    
        program_id :  is an integer identifying the DES program ("100 = EMP, 200 = Marsh")

    '''
    
    url = 'https://dwrmsweb0263.ad.water.ca.gov/TelemetryDirect/api/Results?program='+str(program_id)

    results_df = pd.read_csv(open_url_no_ssl_cert_check(url), sep='|',dtype={"interval_id":int,"aggregate_id":int,"station_active":str},
                             parse_dates=["start_date","end_date"])

    results_df = results_df.loc[results_df.interval_id != 4,:]  # Not "Visit"
    results_df = results_df.loc[results_df.aggregate_id <= 2,:]  # Not labeled "inst" or "Avg"
    #results_df = results_df.loc[results_df.station_active=="Y",:]  # Active
    inventory_cols = ["result_id","station_id","station_name","station_active","constituent_id",
                      "analyte_name","unit_name","equipment_name","aggregate_id","aggregate_name","interval_id","interval_name",
                      "reading_type_id","reading_type_name","rank_id","rank_name","probe_depth","start_date","end_date","cdec_code",
                      "automated","automated_name","public_station","version"]
    results_df = results_df[inventory_cols]
    return results_df



def _depth_trans(x):
    """Translates depths that are d < 0 to "air", 0<=d<=1 to "upper" and d>1 to "lower"
    """
    if not x.startswith("depth"): 
        # Looks like most do, but this provides some safety
        raise ValueError("Unexpected probe depth")
    dpart = float(x.split("=")[1].strip())
    if dpart < 0.: return "air"
    else: return "upper" if dpart <= 1. else "lower"


def des_download(stations,dest_dir,start,end=None,param=None,overwrite=False):
    """ Download robot for DES
    Requires a list of stations, destination directory and start/end date
    These dates are passed on to CDEC ... actual return dates can be
    slightly different
    """
    if end == None: end = dt.datetime.now()
    if not os.path.exists(dest_dir):
        os.mkdir(dest_dir) 
    
    
    failures = []
    skips = []
    inventoryfile = os.path.join(des_local_dir,"inventory_full.csv")

    if os.path.exists(inventoryfile) and (time.time() - os.stat(inventoryfile).st_mtime) < 0.:   #6000.
        logger.info(f"Loading existing inventory file {inventoryfile}")
        inventory_full = pd.read_csv(inventoryfile,header=0,sep=",",\
              dtype={"interval_id":int,"aggregate_id":int,"station_active":str},
              parse_dates=["start_date","end_date"])
    else:
        logger.info(f"Reloading and saving inventory file {inventoryfile}")
        inventory100 = inventory("emp",100)
        inventory200 = inventory("marsh",200)
        inventory100["program_id"] = 100
        inventory200["program_id"] = 200
        inventory_full = pd.concat([inventory100,inventory200],axis=0)
        inventory_full.to_csv(inventoryfile,header=True,sep=",",index=False)    

    for ndx,row in stations.iterrows():
        agency_id = row.agency_id
        station = row.station_id
        param = row.src_var_id
        paramname = row.param
        subloc = row.subloc

       
        stime=start.strftime("%Y-%m-%d")
        try: 
            etime=end.strftime("%Y-%m-%d")
        except:
            etime=pd.Timestamp.now().strftime("%Y-%m-%d")
        found = False

        
        try:
            tst_id = int(agency_id)
        except:
            logger.info(f"agency_id '{agency_id}' for station {station} was not convertable to an integer which is unexpected for DES. Check file lists")
            failures.append((station,param))
            continue
            

        rids = inventory_full.loc[(inventory_full['station_id'] == tst_id) & 
                                  (inventory_full['interval_name']!='Visit') & 
                                  (inventory_full['analyte_name'] == param), ['result_id','station_id','station_name',
                                                                              'analyte_name','program_id',
                                                                              'probe_depth','unit_name','equipment_name',
                                                                              'aggregate_name','interval_name','cdec_code',
                                                                              'start_date','end_date'] ] #
        # This is a workaround at a time when DISE data is due to shift back ends and I (Eli) didn't want to do a ton
        # of work that would be superceded. Apparently the DISE backend station_id is not unique across EMP and Suisun
        # Marsh programs. This makes it kind of useless, but we will deal with that later.
        cross_program_redundant_ids = [10,21,22,40,110,120]
        if tst_id in cross_program_redundant_ids:
            # Think the CDEC code works for these cases
            row_station_id = row.station_id.upper()
            rids = rids.loc[rids['cdec_code'].str[0:3] == row_station_id[0:3]]

        # Add subloc column that is translated to "air", "upper" or "lower". 
        # If there is only a single subloc (depth=1) it is marked "default"
        rids["subloc"] = rids["probe_depth"].apply(_depth_trans) #"default" # dummy for transform
        rids["nrep"] = rids.groupby(['station_id','analyte_name'])["subloc"].transform('count')
        rids["nrepdepth"] = rids.groupby(['station_id','analyte_name','subloc'])["subloc"].transform('count')

        
        if subloc in ('all', 'default'):
            # Assume default requests makes sense if all the rid entries are the same
            # otherwise we need a direct hit
            # todo: a better way to handle this would be to 
            # link the subloc table and look and see if the station
            # has sublocations defined and only use 'default' if it isn't defined. 
            # Here we hope the client application has done this.
            rids.loc[rids.nrep == rids.nrepdepth,"subloc"] = "default"
            #if not is_unique(rids.subloc):
            #    raise ValueError(f"Default location requested for a station with multiple sublocations for variable {param}")
        elif subloc != 'all':
            rids = rids.loc[rids.subloc == subloc,:]

            
        if len(rids) == 0: 
            logger.debug(f"No Data for station {station} and param {paramname}, agency station id {agency_id}")
            failures.append((station,paramname))
            continue # next request

        for ndx,rid in rids.iterrows(): 
            rid_code = rid.result_id
            prog_id = rid.program_id
            url = f'https://dwrmsweb0263.ad.water.ca.gov/TelemetryDirect/api/Results/ReadingDates?program={prog_id}&resultid={rid_code}'
            max_retry = 12
            itry = 0
            while itry < max_retry:
                try:
                    dates = pd.read_csv(open_url_no_ssl_cert_check(url), parse_dates=['first_date', 'last_date'], sep='|')
                    itry = max_retry
                except:
                    itry = itry + 1
                    sleeptime = 4. if itry > 5 else 2.
                    if itry >= max_retry: raise
                    time.sleep(sleeptime)
                    
            fstart = rid.start_date
            fend = rid.end_date
            if pd.isnull(fend): fend=end
            if "saturation" in rid.unit_name: continue
            if "NFU" in rid.unit_name: continue
            if fend < start: 
                logger.info(f"skipping one file because fend < {start}")
                continue
            if fstart > end: 
                logger.info(f"skipping one file because fstart > {end}")
                continue
            fstart = max(start,fstart)
            fend = min(fend,end)
            if pd.isnull(fend):
                yearname = f"{fstart.year}_9999"
            else:
                yearname = f"{fstart.year}_{fend.year}" 

            sub = rid.subloc

            if sub == "default":  # omit from name
                outfname = f"des_{station}_{agency_id}_{paramname}_{yearname}.csv"
            else:
                outfname = f"des_{station}@{sub}_{agency_id}_{paramname}_{yearname}.csv"            
            outfname = outfname.lower()
            outfname = outfname.lower()
            path = os.path.join(dest_dir,outfname)
            if os.path.exists(path) and not overwrite:
                #logger.info("Skipping existing station because file exists: %s" % outfname)
                skips.append(path)
                continue
            else:
                logger.info(f"Attempting to download station: {station} variable {paramname} from {fstart} to {fend}")                                
                try:
                    df = query_station_data(prog_id,rid_code,fstart,fend)
                    if df.shape[0]<=1:
                        download_success = False
                        logger.info("Empty")               
                    else:
                        download_success = True
                except:
                    fmessage = f"Download failed for station {station}, sublocation {subloc}, paramname {paramname}"
                    logger.info(fmessage)
                    failures.append((station,paramname))
                    download_success = False
                if download_success:
                    meta = des_metadata(station,agency_id,rid)
                    meta["subloc"]=subloc
                    meta["param"]=paramname
                    agency_unit = meta["agency_unit_name"].strip()
                    meta["unit"] = des_unit_map[agency_unit]                    
                    write_ts(path,df,meta)   
    
    if len(failures) == 0:
        logger.info("No failed stations")
    else:
        logger.info("Failed query stations: ")
        for failure in failures:
            logger.info(failure)

def process_station_list2(file):
    stations = []
    f = open(file,'r')
    all_lines = f.readlines()
    f.close()
    stations = [x.strip().split(',')[0] for x in all_lines if not x.startswith("#")]
    return stations
                

def main():
    parser = create_arg_parser()
    args = parser.parse_args()
    destdir = args.dest_dir
    stationfile = args.stationfile
    overwrite = args.overwrite
    start = args.start
    end = args.end
    if start is None: 
        stime = pd.Timestamp(1900,1,1)
    else:
        stime = dt.datetime(*list(map(int, re.split(r'[^\d]', start))))
    if end is None:
        etime = dt.datetime.now()
    else:
        etime = dt.datetime(*list(map(int, re.split(r'[^\d]', end))))
    param = args.param

    stationfile=stationfile_or_stations(args.stationfile,args.stations)
    slookup = dstore_config.config_file("station_dbase")
    vlookup = dstore_config.config_file("variable_mappings")            
    df = process_station_list(stationfile,param=param,station_lookup=slookup,
                                  agency_id_col="agency_id",
                                  param_lookup=vlookup,
                                  source='dwr_des',
                                  subloc='all')
    des_download(df,destdir,stime,etime,overwrite=overwrite)  
        

if __name__ == '__main__':
    main()
