#!/usr/bin/env python
# -*- coding: utf-8 -*-
import concurrent.futures
from vtools.datastore.process_station_variable import process_station_list,stationfile_or_stations
from vtools.datastore import station_config
from vtools.datastore.read_ts import read_ts
from vtools.datastore.download_nwis import nwis_download,parse_start_year
from vtools.datastore.download_noaa import noaa_download
from vtools.datastore.download_cdec import cdec_download
from vtools.datastore.download_ncro import download_ncro_por,download_ncro_inventory,station_dbase
from dms_data_tools.download_des import des_download
from schimpy.station import *
import pandas as pd
import glob
import os
import re

"""
   populate raw/incoming with populate() obtaining des, usgs, noaa, usgs, usbr
   usgs: files may have two series
   des: naive download will produce files from different instruments with time overlaps
        the script    run rationalize_time_partitions for des

   ncro: typically done with download_ncro which is a period of record downloader
         ncro is not realtime run populate2 to get the update for ncro 
   run revise_time to correct start and end times.
   
   What are steps to update just realtime
   
   Need to add something for the daily stations and for O&M (Clifton Court, Banks)
"""   
   


# number of data to read in search of start date or multivariate
NSAMPLE_DATA=200

downloaders = {"dwr_des":des_download,"noaa":noaa_download,"usgs":nwis_download,"usbr":cdec_download,'cdec':cdec_download}

def populate_repo(agency,param,dest,start,end,overwrite=False,ignore_existing=None):
    slookup = station_config.config_file("station_dbase")
    vlookup = station_config.config_file("variable_mappings") 
    subloclookup = station_config.config_file("sublocations")
    df = pd.read_csv(slookup,sep=",",comment="#",header=0,dtype={"agency_id":str})
    df=df.loc[df.agency.str.lower()==agency,:]
    df["agency_id"] = df["agency_id"].str.replace("\'","",regex=True)  
    
    dfsub = read_station_subloc(subloclookup)
    df = merge_station_subloc(df,dfsub,default_z=-0.5)

    #This will be used to try upper and lower regardless of whether they are listed
    maximize_subloc = False


    
    #df['subloc'] = 'default'
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
    writedf = pd.DataFrame.from_records(renames,columns = ["from","to"])
    writedf.to_csv(outfile,sep=",",header=True) 
    
def revise_filename_syears(pat,force=True,outfile="rename.txt"):
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
                    print("Rename failed because of permission or overwriting issue. The force argment may be set to False. Dumping list of renames so far to rename.txt")
                    _write_renames(rename,"rename.txt")
                    raise
    _write_renames(renames,outfile)



def revise_filename_syear_eyear(pat,force=True,outfile="rename.txt"):
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
    slookup = station_config.config_file("station_dbase")
    vlookup = station_config.config_file("variable_mappings") 
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

#    revise_filename_syear_eyear(f"{agency}*_{var}_*.rdb")


def interpret_fname(fname,to_meta=True):
    fname = os.path.split(fname)[1]
    meta = {}
    datere = re.compile(r"([a-z0-9]+)_([a-z0-9@]+)_([a-z0-9]+)_([a-z0-9]+).*_(\d{4})_(\d{4})(?:\..{3})")
    datere1 = re.compile(r"([a-z0-9]+)_([a-z0-9@]+)_([a-z0-9]+)_([a-z0-9]+).*_(\d{4})(?:\..{3})")
    m = datere.match(fname)
    if m is None:
        m = datere1.match(fname)
        single_date = True
    else:
        single_date = False
    if m is not None:
        meta['filename'] = m.group(0)
        meta['agency'] = m.group(1)
        station_id = m.group(2)
        if "@" in station_id: 
            station_id,subloc = station_id.split("@")
        else: 
            subloc = None
           
        meta['station_id'] = station_id
        meta['subloc'] = subloc
        meta['agency_id'] = m.group(3)
        meta['param'] = m.group(4)
        if single_date:
            meta['year'] = m.group(5)
        else:
            meta['syear'] = m.group(5)
            meta['eyear'] = m.group(6)
        return meta
    else:
        raise ValueError(f"Naming convention not matched for {fname}")

def meta_to_filename(meta):
    station_id = meta['station_id'] if meta['subloc'] is None else f"{meta.station_id}@{meta.subloc}"
    if 'syear' in meta and 'eyear' in meta:
        year_part = f"{meta['syear']}_{meta['eyear']}"
    else:
        year_part = f"{meta['year']}"
    return  f"{meta['agency']}_{station_id}_{meta['agency_id']}"+\
            f"_{meta['param']}_{year_part}.csv"
    

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
    
def main():
    dest = "raw"
    do_purge = False
    if not os.path.exists(dest):
        os.mkdir(dest)
        print("Directory ",dest," created")
        print("Directory ",dest," created")
    else:
        if do_purge: purge(dest)
    
    failures = []    
    all_agencies = ["usgs","dwr_des","usbr","noaa"]
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_agency = {executor.submit(populate,dest,agency): agency for agency in all_agencies}
        future_to_agency[executor.submit(populate_ncro_repo,dest)] = "ncro"
    
    for future in concurrent.futures.as_completed(future_to_agency):
        agency = future_to_agency[future]
        try:
            data = future.result()
        except Exception as exc:
            failures.append(agency)
            print('%r generated an exception: %s' % (agency, exc))
    #populate(dest,agency)
    rationalize_time_partitions(os.path.join(dest,"des*"))  # A fixup mostly for DES, addresses overlapping years of  same variable
    
    
    #download_ncro_por(dest)     # period of record for NCRO QA QC'd
    #populate_ncro_realtime(dest)  # Recent NCRO 
    revise_filename_syear_eyear(os.path.join(dest,f"ncro_*.csv"))  
    revise_filename_syear_eyear(os.path.join(dest,f"cdec_*.csv"))  
    print("These agency queries failed")

if __name__ == '__main__':
    main()

    
# Additional: make sure we have woodbridge, yby, 

   
   
    
    