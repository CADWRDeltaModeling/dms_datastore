#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
   Scripts to populate raw/incoming with populate() obtaining des, usgs, noaa, usgs, usbr
   usgs: files may have two series
   des: naive download will produce files from different instruments with time overlaps
        the script    run rationalize_time_partitions for des

   ncro: typically done with download_ncro which is a period of record downloader
         ncro is not realtime run populate2 to get the update for ncro 
   run revise_time to correct start and end times.
   
   What are steps to update just realtime
   
   Need to add something for the daily stations and for O&M (Clifton Court, Banks)
"""  

import glob
import os
import re
import traceback
import argparse
import concurrent.futures
import pandas as pd
from dms_datastore.process_station_variable import process_station_list,stationfile_or_stations,read_station_subloc
from schimpy.station import *
from dms_datastore import dstore_config
from dms_datastore.filename import interpret_fname,meta_to_filename
from dms_datastore.read_ts import read_ts
from dms_datastore.download_nwis import nwis_download,parse_start_year
from dms_datastore.download_noaa import noaa_download
from dms_datastore.download_cdec import cdec_download
from dms_datastore.download_ncro import download_ncro_por,download_ncro_inventory,station_dbase
from dms_datastore.download_des import des_download



# number of data to read in search of start date or multivariate
NSAMPLE_DATA=200

downloaders = {"dwr_des":des_download,"noaa":noaa_download,
               "usgs":nwis_download,"usbr":cdec_download,
               "cdec":cdec_download}




def revise_filename_syears(pat,force=True,outfile="rename.txt"):
    """ Revise start year of files matching pat to the first year of valid data
    
    Parameters
    ----------
    pat : str
        Pattern to match, may include wildcards (uses glob)
    force : True
        Force renaming
    outfile : str
        Name of file to log failures

    """

    filelist = glob.glob(pat)

    renames = []
    for fname in filelist:
        direct,pat = os.path.split(fname)
        head,ext = os.path.splitext(pat)
        parts = head.split("_")
        oldstart,oldend = parts[-2:]
        ts = read_ts(fname,nrows=200,force_regular=False)
        if ts.first_valid_index() is None: 
            raise ValueError(f"Issue obtaining start time from file: {fname}")
            print("Bad: ",fname)
        else: 
            newstart = str(ts.first_valid_index().year)
            newname = fname.replace(oldstart,newstart)
            
            if fname != newname:
                print(f"Renaming {fname} to {newname}")
                renames.append((fname,newname))
                try:                
                    if force:
                        os.replace(fname,newname)
                    else:
                        os.rename(fname,newname)
                except:
                    print("Rename failed because of permission or overwriting issue.") 
                    print("This can be harmless if the downloader handles clipping of the years in file names")
                    print("Dumping list of renames so far to rename.txt")
                    _write_renames(fname,"rename.txt")
                    raise
    _write_renames(renames,outfile)



def revise_filename_syear_eyear(pat,force=True,outfile="rename.txt"):
    """ Revise both the start year and end year of files matching pat to years of valid data
    
    Parameters
    ----------
    pat : str
        Pattern to match, may include wildcards (uses glob)
    force : True
        Force renaming
    outfile : str
        Name of file to log failures

    """


    filelist = glob.glob(pat)
    bad = []
    renames = []    
    for fname in filelist:
        direct,pat = os.path.split(fname)
        head,ext = os.path.splitext(pat)
        parts = head.split("_")
        oldstart,oldend = parts[-2:]
        try:
            ts = read_ts(fname,force_regular=False)
        except:
            file_size = os.path.getsize(fname)
            if (file_size < 25000):
                os.remove(fname)
                bad.append(fname+" (small,deleted)")
            else:
                bad.append(fname+" (not small)")
        if ts.first_valid_index() is None: 
            if ts.isnull().all(axis=None):
                print("All values are bad. Deleting file")
                bad.append(fname + " (all bad, deleting)")
                os.remove(fname)
            else:
                raise ValueError(f"Issue obtaining start time from file: {fname}")
        else: 
            newstart = str(ts.first_valid_index().year)
            newend = oldend if oldend == "9999" else str(ts.last_valid_index().year)
            new_time_block = newstart + "_" + newend  
            old_time_block = oldstart + "_" + oldend            
            newname = fname.replace(old_time_block,new_time_block)
            print(f"Renaming {fname} to {newname}")
            if fname != newname:
                renames.append((fname,newname))
                try:                
                    if force:
                        os.replace(fname,newname)
                    else:
                        os.rename(fname,newname)
                except:
                    print("Rename failed because of permission or overwriting issue. The force argment may be set to False. Dumping list of renames so far to rename.txt")
                    _write_renames(rename,"rename.txt")
                    raise
    _write_renames(renames,outfile)
    if len(bad) > 0:
        print("Bad files:")
        for b in bad: print(b)



def populate_repo(agency,param,dest,start,end,overwrite=False,ignore_existing=None):
    """ Populate repository for the given agency/source and parameter
    
    Parameters
    ----------
    agency : str
        Agency to populate
    param : str
        Parameter to populate. Should be a variable on the variables.csv table
    dest : str
        Location to put files
    start : int
        year to start
    end : int
        year to end or 9999 to go to now
    overwrite : bool
        passed to downloading script
    ignore_existing : list of existing files to ignore
    
    Returns
    -------

   """

    # todo: This may limit usefulness for things like atmospheric    
    slookup = dstore_config.config_file("station_dbase")
    vlookup = dstore_config.config_file("variable_mappings") 
    subloclookup = dstore_config.config_file("sublocations")
    df = pd.read_csv(slookup,sep=",",comment="#",header=0,dtype={"agency_id":str})
    df=df.loc[df.agency.str.lower()==agency,:]
    df["agency_id"] = df["agency_id"].str.replace("\'","",regex=True)  
    
    dfsub = read_station_subloc(subloclookup)
    df = merge_station_subloc(df,dfsub,default_z=-0.5)

    #This will be used to try upper and lower regardless of whether they are listed
    maximize_subloc = False

    df=df.reset_index()    
    if ignore_existing is not None:
        df = df[~df["id"].isin(ignore_existing)]
        

    dest_dir = dest
    source = agency if agency != 'usbr' else 'cdec'
    agency_id_col = "cdec_id" if source == 'cdec' else "agency_id"
    

    df=df[["id","subloc"]]
    stationlist = process_station_list(df,param=param,param_lookup=vlookup,
                                       station_lookup=slookup,agency_id_col=agency_id_col,source=source)
    
    if maximize_subloc:
        stationlist["subloc"] = 'default'
        if param not in  ['flow','elev']:
            sl1 = stationlist.copy()
            sl1['subloc'] = 'upper'
            sl2 = stationlist.copy()
            sl2['subloc'] = 'lower'
            stationlist = pd.concat([stationlist,sl1,sl2],axis=0)
    #if agency == "noaa":
    #    if param == 'elev' or param == 'prediction':    
    #        stationlist = stationlist[stationlist.agency_id.str.startswith("9")]
    downloaders[agency](stationlist,dest_dir,start,end,param,overwrite)

def _write_renames(renames,outfile):
    """ Logger to write rename failures """
    writedf = pd.DataFrame.from_records(renames,columns = ["from","to"])
    writedf.to_csv(outfile,sep=",",header=True) 
    

def existing_stations(pat):
    allfiles = glob.glob(pat)
    existing = set()
    for f in allfiles:
        direct,fname = os.path.split(f)
        parts = fname.split("_")
        station_id = parts[1]
        existing.add(station_id)
    return existing

def list_ncro_stations(dest):
    allfiles = glob.glob(os.path.join(dest,'ncro_*.csv'))
    def station_param(x):
        parts = os.path.split(x)[1].split("_")
        try:
            return (parts[1],parts[3],'cdec',parts[2]) 
        except:
            print(x)
            raise ValueError(f"Unable to parse station and parameter from name {x}")
    stationlist=[station_param(x) for x in allfiles]
    df = pd.DataFrame(data=stationlist,columns=["id","param","agency","agency_id_from_file"])
    return df 


def populate_repo2(df,dest,start,overwrite=False,ignore_existing=None):
    """ Currently used by ncro realtime """
    slookup = dstore_config.config_file("station_dbase")
    vlookup = dstore_config.config_file("variable_mappings") 
    df["station_id"] = df["id"].str.replace("'","")
    df["subloc"] = "default"
    
    if ignore_existing is not None:
        df = df[~df["id"].isin(ignore_existing)]

    source = 'cdec'
    agency_id_col = "agency_id_from_file" 
    stationlist = process_station_list(df,param_lookup=vlookup,
                                       station_lookup=slookup,agency_id_col=agency_id_col,source=source)
    #print("station list ************")                                   
    #print(stationlist.columns)
    end = None
    downloaders['cdec'](stationlist,dest,start,end,overwrite)




def populate(dest,all_agencies=None):
    """ Driver script that populates agencies in all_agencies with destination dest """
    print("dest: ",dest,"agencies: ",all_agencies)
    purge = False
    ignore_existing=None #[]
    current = pd.Timestamp.now()
    if all_agencies is None: 
        all_agencies = ["usgs","dwr_des","usbr","noaa"]

    if not isinstance(all_agencies,list):
        all_agencies = [all_agencies]
    
    
    for agency in all_agencies:
        if agency == "noaa": 
            varlist = ["elev","predictions"]  # handled in next section
        else:
            varlist = ["flow","elev","ec","temp","do","turbidity"]   
        
        for var in varlist:
            populate_repo(agency,var,dest,pd.Timestamp(1980,1,1),pd.Timestamp(1999,12,31,23,59),ignore_existing=ignore_existing)
            populate_repo(agency,var,dest,pd.Timestamp(2000,1,1),pd.Timestamp(2019,12,31,23,59),ignore_existing=ignore_existing)
            populate_repo(agency,var,dest,pd.Timestamp(2020,1,1),None,overwrite=True)
            ext = 'rdb' if agency == 'usgs' else '.csv'
            revise_filename_syear_eyear(os.path.join(dest,f"{agency}*_{var}_*.{ext}"))
            print(f"Done with agency {agency} variable: {var}")

 
def purge(dest):
    if purge:
        for pat in ['*.csv','*.rdb']:
            allfiles = glob.glob(os.path.join(dest,pat))
            for fname  in allfiles: 
                os.remove(fname) 
    
    
def populate_ncro_realtime(dest,realtime_start=pd.Timestamp(2021,1,1)):
    
    #NCRO QAQC
    #dest = "//cnrastore-bdo/Modeling_Data/continuous_station_repo/raw/incoming/dwr_ncro"
    #ncro_download_por(dest)    

    #NCRO recent from CDEC
    end = None
    ncrodf = list_ncro_stations(dest)    
    populate_repo2(ncrodf,dest,realtime_start,overwrite=True)



    

def rationalize_time_partitions(pat):
    allpaths = glob.glob(pat)
    repodir = os.path.split(allpaths[0])[0]
    allfiles = [os.path.split(x)[1] for x in allpaths]
    allmeta = []
    already_checked = set()
    for fname in allfiles:
        print(fname)
        fname_meta = interpret_fname(fname)
        print(fname_meta)        
        allmeta.append(fname_meta)
    for meta in allmeta:
        if meta['filename'] in already_checked: continue
        near_misses = []
        for meta2 in allmeta:
            if meta == meta2: continue
            same_series = (meta["agency"] == meta2["agency"]) and\
                          (meta["param"] == meta2["param"]) and\
                          (meta["station_id"] == meta2["station_id"])
            if same_series:
                already_checked.add(meta2['filename'])           
                near_misses.append(meta2)

        already_checked.add(meta['filename'])
        if len(near_misses) > 0: 
            near_misses.append(meta)        
            #print(f"Main series: {meta['filename']}")
            superseded = []
            for i,meta in enumerate(near_misses): 
                #print(meta)
                issuperseded = False
                
               
                for meta2 in near_misses:
                    if meta == meta2: continue
                    issuperseded |= meta2['syear'] <= meta['syear'] and meta2['eyear'] >= meta['eyear'] 
                if issuperseded: 
                    fnamesuper = meta['filename']
                    print(f"superseded: {fnamesuper}")
                    os.remove(os.path.join(repodir,fnamesuper))  
                    superseded.append(fnamesuper)
            
        else: 
            print(f"Main series: {meta['filename']} had no similar file names")
    for sup in superseded: print(sup)
    
def populate_ncro_repo(dest):
    download_ncro_por(dest)     # period of record for NCRO QA QC'd
    populate_ncro_realtime(dest)  # Recent NCRO     
    
def ncro_only(dest):
    populate_ncro_repo(dest)
    revise_filename_syear_eyear(os.path.join(dest,f"ncro_*.csv"))  
    revise_filename_syear_eyear(os.path.join(dest,f"cdec_*.csv"))  



def populate_main(dest,agencies):
    do_purge = False
    if not os.path.exists(dest):
        os.mkdir(dest)
        print("Directory ",dest," created")
        print("Directory ",dest," created")
    else:
        if do_purge: purge(dest)
    
    failures = []
    if agencies is None or len(agencies)==0:
        all_agencies = ["usgs","dwr_des","usbr","noaa","dwr_ncro"]
    else:
        all_agencies = agencies
    do_ncro = ("ncro" in all_agencies) or ("dwr_ncro" in all_agencies)
    do_des = ("des" in all_agencies) or ("dwr_des" in all_agencies)    
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        future_to_agency = {executor.submit(populate,dest,agency): agency 
                            for agency in all_agencies if (agency not in ["dwr_ncro","ncro"])}
        if do_ncro:
            future_to_agency[executor.submit(populate_ncro_repo,dest)] = "ncro"
        else: 
            raise ValueError("ncro issue")
    
    for future in concurrent.futures.as_completed(future_to_agency):
        agency = future_to_agency[future]
        try:
            data = future.result()
        except Exception as exc:
            failures.append(agency)
            trace=traceback.format_exc()
            print(f'{agency} generated an exception: {exc} with trace:\n{trace}')

    # A fixup mostly for DES, addresses overlapping years of  same variable
    if do_des:
        rationalize_time_partitions(os.path.join(dest,"des*"))  
    
    if do_ncro:
        revise_filename_syear_eyear(os.path.join(dest,f"ncro_*.csv"))  
    revise_filename_syear_eyear(os.path.join(dest,f"cdec_*.csv"))  
    print("These agency queries failed")




def create_arg_parser():
    parser = argparse.ArgumentParser('Delete files contained in a list')

    parser.add_argument('--dest', dest="dest", default=None,
                        help='Directory where files will be stored. ')
    parser.add_argument('--agencies', nargs='+', default=[],
                        help='Text file listing files to delete.')
    return parser


def main():
    parser = create_arg_parser()
    args = parser.parse_args()
    dest = args.dest
    if dest is None: 
        raise ValueError("Destination directory must be specified")
    agencies = args.agencies
    print(dest,agencies)
    populate_main(dest,agencies)
    



if __name__ == '__main__':
    main()

    
# Additional: make sure we have woodbridge, yby, 

   
   
    
    