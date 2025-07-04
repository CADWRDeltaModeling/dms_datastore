#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import os


def stationfile_or_stations(stationfile,stations):
    """ Process stationfile arguments and station arguments 
        Delivers the one that will be used in process_station_list 
        including sanity checks both aren't given"""
    
    # this works for None or empty lists of strings
    if not (stations or stationfile):
        raise ValueError("Either station or stationfile required")
    if stations and stationfile:
        raise ValueError("Station and stationfile inputs are mutually exclusive")    
    if stationfile:  # stationfile provided, stations not
        if len(stationfile) > 1: 
            raise ValueError("Only one stationfile may be input")
        stationfile = stationfile[0]
        if not os.path.exists(stationfile):
            raise ValueError(f"File does not exist: {stationfile}")
        return stationfile
    else: 
        return stations



def process_station_list(stationlist,id_col="id",agency_id_col="agency_id",
                         param_col=None,param=None,
                         subloc_col=None,subloc_lookup=None,subloc="default",
                         station_lookup=None,param_lookup=None,source = 'cdec'):
    """Process a csv list of station
    
    Parameters
    stationlist : str
    List of strings of station ids or name of csv file containing the list of request info (id,subloc,param)
    
    id_col : str
    Name of column containing station id. This is expected to be the agency id unless station_lookup is provided
    
    subloc_col : str
    Addional location or program name needed to identify the data stream. An example of location might be upper and lower sensor. An example of program having its own instrument would be the bgc (biogeochemistry) program for USGS in the Bay-Delta. 
    
    param_col : str
    Name of column containing the parameter for the query. This is expected to be an agency code/name unless station_lookup is provided. If param is provided, this should be None.
    
    param : str
    Requested parameter. Should be an agency code unless param_lookup is given. This will be used for every station.
    
    param_lookup: str
    Lookup file containing columns "variable_name" and "agency_variable_name"
    
    station_lookup
    Lookup file containing "id" and "agency_id" columns.
        
    """

    if param_col is not None and param is not None:
        raise ValueError("Cannot use both param_col and param arguments")

    if isinstance(stationlist,str):
        station_df = pd.read_csv(stationlist,sep=",",comment="#",header=0)
    elif isinstance(stationlist,list):
        station_df = pd.DataFrame(data={"id":stationlist})
    else:
        station_df = stationlist
    

    # Rename the parameter column as param to standardize
    # If there is no param column, create one and populate all its rows with param.
    if param is not None:          # only one parameter, append as column
        station_df["param"] = param
    else:
        if param_col is not None: 
            station_df.rename(columns={param_col:"param"})
    
    if not "subloc" in station_df.columns: 
        station_df["subloc"]="default"
    else:
        station_df["subloc"] = station_df.subloc.astype(str)


    if station_lookup:
        slookup = pd.read_csv(station_lookup,sep=",",comment="#",header=0,index_col=id_col)
        station_df["station_id"] = station_df[id_col].str.lower()       
        station_df.set_index(id_col,drop=True,inplace=True)       
        if "agency" in slookup.columns and "agency" in station_df.columns:
            slookup.rename(columns={"agency":"agency_station_db"},inplace=True) 
        station_df = station_df.merge(slookup,on="id",how="left")       
        station_df.loc[station_df.subloc.isin(['nan','']),'subloc'] = "default"
        station_df["agency_id"] = station_df[agency_id_col]
        no_agency_id = station_df.agency_id.isin(['nan','']) | station_df.agency_id.isnull()
        station_df.loc[no_agency_id,'agency_id'] = station_df.loc[no_agency_id,'station_id']
    else: 
        station_df["agency_id"] = station_df["id"]
    # Some ids are prepended with ' in order to deal with Excel silent coersion.
    station_df["agency_id"] = station_df["agency_id"].astype(str).str.replace("\'","",regex=True)   
    
    # Replace parameters with lookup values from station_lookup. Failure will leave as-is, in case of mix.
    if param_lookup is not None:
        if isinstance(param_lookup,str):
            vlookup = pd.read_csv(param_lookup,sep=",",comment="#",header=0,
                                  usecols=['var_name','src_var_id','src_name'],dtype=str)
        else:
            vlookup = param_lookup
        vlookup = vlookup.loc[vlookup.src_name == source,:]
        vlookup.rename(columns={"var_name":"param"},inplace=True)       
        station_df = station_df.merge(vlookup,on="param",how="left")
        station_df = station_df.fillna(value={"src_var_id":station_df.param})

    station_df = station_df.rename(columns={"id" : "station_id"})
    # Any nans in the agency_id indicate a lookup failure. As a backup assume the agency_id was already provided in the id column
    station_df["agency_id"]=station_df["agency_id"].fillna(station_df.station_id)
    
    return station_df   #[["station_id","agency_id","subloc","param","src_var_id"]]
            

def read_station_subloc(fpath):
    """Read a BayDeltaSCHISM station_sublocs.csv  file into a pandas DataFrame

       The BayDelta SCHISM format has a header and uses "," as the delimiter and has these columns:
       id,subloc,z

       The id is the station id, which is the key that joins this file to the station database. 'subloc' is a label that describes
       the sublocation or subloc and z is the actual elevation of the instrument

       Example might be:
       id,subloc,z
       12345,upper,-0.5

       Other columns are allowed, but this will commonly merged with the station database file so we avoid column names like 'name' that might collide

     Parameters
     ----------
     fpath : fname
        Path to input station.in style file

     Returns
     -------
     Result : DataFrame
         DataFrame with hierarchical index (id,subloc) and data column z

    """

    df = pd.read_csv(fpath,sep=",",header=0,index_col=["id","subloc"],comment='#')
    df["z"] = df.z
    return df[["z"]]

def merge_station_subloc(station_dbase,station_subloc,default_z):
    """Merge BayDeltaSCHISM station database with subloc file, producing the union of all stations and sublocs including a default entry for stations with no subloc entry

     Parameters
     ----------
     station_dbase : DataFrame
        This should be the input that has only the station id as an index and includes other metadata like x,y,

     station_subloc : DataFrame
        This should have (id,subloc) as an index

     Returns
     -------
     Result : DataFrame
         DataFrame that links the information.

    """

    merged =  station_dbase.reset_index().merge(station_subloc.reset_index(),
                left_on="id",right_on="id",
                how='left')
    merged.fillna({"subloc":"default","z": default_z},inplace=True)
    merged.set_index(["id","subloc"],inplace=True)

    return merged


        
if __name__ == '__main__':
    slookup = "d:/delta/BayDeltaSCHISM/data/stations_utm_new.csv"
    vlookup = "D:/Delta/data_tools/dms_data_tools/dms_data_tools/variable_mappings.csv"
    df = process_station_list("slist.csv",param="ec",station_lookup=slookup,agency_id_col="cdec_id",
                              param_lookup=vlookup)
    print("\n")
    print(df)



      


