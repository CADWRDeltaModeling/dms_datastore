import yaml
import os
import pandas as pd

__all__ = ["station_dbase","configuration","get_config","config_file"]

config = None    
localdir = os.path.join(os.path.split(__file__)[0],"config_data")
    
with open(os.path.join(localdir,"dstore_config.yaml"), 'r') as stream:
    config = yaml.load(stream,Loader=yaml.FullLoader)

station_dbase_cache = None
def station_dbase(dbase_name=None):
    global station_dbase_cache
    if station_dbase_cache is None:
        if dbase_name is None:
            dbase_name = config_file("station_dbase")
        db = pd.read_csv(dbase_name,sep=",",comment="#",header=0,index_col="id",dtype={"agency_id":str})
        db["agency_id"] = db["agency_id"].str.replace("\'","",regex=True)
        
        dup = db.index.duplicated()
        db.index = db.index.str.replace("'","")
        if dup.sum(axis=0)> 0:
            print("Duplicates")
            print(db[dup])
            raise ValueError("Station database has duplicate id keys. See above")
        station_dbase_cache = db
    return station_dbase_cache

subloc_cache = None
def sublocation_df(dbase_name=None):
    global subloc_cache
    if subloc_cache is None:
        subloc_name = config_file("sublocations")
        db = pd.read_csv(subloc_name,sep=",",comment="#",header=0,dtype={"id": str, "subloc": str, "z": float, "comment": str})        
        dup = db.duplicated(subset=["id","subloc"],keep="first")
        if dup.sum(axis=0)> 0:
            print("Duplicates in subloc table")
            print(db[dup])
            raise ValueError("Station database has duplicate id keys. See above")
        subloc_cache = db
    return subloc_cache



def configuration():
    config_ret = config.copy()
    config_ret["config_file_location"] = __file__
    return config_ret


def get_config(label):
    return config_file(label)


def config_file(label):
    fname = config[label]
        
    # in director local to this file?
    localpath = os.path.join(localdir,fname)
    if os.path.exists(localpath):  
        return localpath
    else:
        # assume it is in the config_data directory
        assume_fname = os.path.join("config_data",fname)
        if os.path.exists(assume_fname): 
            return assume_fname
        else:
            raise ValueError(f"File not found {fname} for label {label} either on its own or in local directory {localdir}")
