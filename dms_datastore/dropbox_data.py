import os
import re
import yaml
import glob
from dms_datastore.read_ts import *
from dms_datastore.write_ts import write_ts_csv
from dms_datastore.dstore_config import station_dbase

# Global variable to store cached data
_cached_spec = None
_station_dbase = station_dbase()


def get_spec(filename):
    global _cached_spec
    if _cached_spec is None:
        with open(filename, "r") as file:
            _cached_spec = yaml.safe_load(file)
    return _cached_spec


def reader_for(fstr):
    if fstr == "read_ts":
        return read_ts
    else:
        return None

class DataCollector(object):
    def __init__(self,name,location,file_pattern,recursive=False):
        self.name = name
        self.location = location
        self.file_pattern = file_pattern
        self.recursive = recursive
 
    def data_file_list(self):
        fpath = os.path.join(self.location,self.file_pattern)
        return glob.glob(fpath,recursive=self.recursive)

def populate_meta(fpath,listing,meta_out=None):
    meta = listing["metadata"]
    name = listing["name"]

    slookup = station_dbase()
    try:
        station_id = str(meta["station_id"])
    except:
        raise ValueError(f"No station_id for {name}. The station_id must be a key in the metadata section. If inferred, set 'infer_from_agency_id' as value")

    if station_id == "infer_from_agency_id":
        # expect that agency_id has been inferred
        if "agency_id" in meta:
            agency_id = meta['agency_id']
        elif "agency_id" in meta_out:
            agency_id = meta_out['agency_id']
        else:
            raise ValueError(f"station_id is specified for inference from agency_id in {name} but agency_id not specified in metadata or inferred sections")

        station_id = slookup[slookup['agency_id'] == agency_id].index[0]
        print("index",station_id)

    if meta_out is None: meta_out = {}        
    meta_out["param"] = meta["param"]
    source = meta["source"]
    meta_out["agency"] = meta["agency"] if "agency" in meta else slookup.loc[station_id, "agency"]
    meta_out["source"] = source
    meta_out["station_id"] = station_id
    station_name = slookup.loc[station_id, "name"]
    meta_out["station_name"] = station_name
    meta_out["sublocation"] = (
        meta["sublocation"] if meta["sublocation"] is not None else "default"
    )
    meta_out["agency_id"] = slookup.loc[station_id, "agency_id"]
    meta_out["latitude"] = float(slookup.loc[station_id, "lat"])
    meta_out["longitude"] = float(slookup.loc[station_id, "lon"])
    meta_out["projection_x_coordinate"] = float(slookup.loc[station_id, "x"])
    meta_out["projection_y_coordinate"] = float(slookup.loc[station_id, "y"])
    meta_out["projection_authority_id"] = "epsg:26910"
    meta_out["crs_note"] = (
        "Reported lat-lon are agency provided. Projected coordinates may have been revised based on additional information."
    )
    return meta_out


def infer_meta(fpath, listing,fail="none"):
    print(listing)
    meta_string = listing["metadata_infer"]["regex"]
    print(meta_string)
    meta_re = re.compile(meta_string)
    extractables = listing["metadata_infer"]["groups"]
    meta = {}
    for key,val in extractables.items():
        ndx = int(key)
        print(key,val)
        try:
            m = meta_re.match(fpath)
            meta[val] = m.group(ndx)
        except:
            meta[val] = None
    return meta
        


def get_data(spec):
    
    dropbox_home = spec["dropbox_home"]
    never_skip = False
    
    for listing in spec["data"]:
        if "skip" in listing and not never_skip:
            """skip the item, possibly because it is securely archived already"""
            if listing["skip"] in ["True",True]: 
                continue

        item = listing["collect"]
        metadata = listing["metadata"]


        name = item["name"]
        file_pattern = item["file_pattern"].format(dropbox_home=dropbox_home)
        location = item["location"].format(dropbox_home=dropbox_home)
        recursive = bool(item["recursive_search"])
        

        collector = DataCollector(name, location, file_pattern, recursive)
        allfiles = collector.data_file_list()
        print("all files")
        print(allfiles)
         
        for fpath in allfiles:
            print("Working on", fpath)
            reader = reader_for(item["reader"])
            if "selector" in item: 
                selector = item["selector"]
            ts = reader(fpath, selector=selector)
            ts.columns = ["value"]
            
            inferring_meta = "metadata_infer" in listing
            if inferring_meta:
                metadata = infer_meta(fpath, listing)
            else: 
                metadata = {}


            meta_out = populate_meta(fpath, listing, metadata)
            

            if "sublocation" not in meta_out or meta_out["sublocation"] is None:
                meta_out["sublocation"] = "default"  # check not just "subloc"

            
            fname_out = (meta_out["source"] + "_" + meta_out["station_id"] 
                        + "_" + meta_out["agency_id"] + "_" + meta_out["param"] + ".csv")
            
            fname_out = os.path.join("formatted", fname_out)
            print(fname_out)
            write_ts_csv(ts, fname_out, meta_out, chunk_years=True)


def dropbox_data(spec_fname):
    spec = get_spec(spec_fname)
    get_data(spec)


if __name__ == "__name__" or True:
    dropbox_data("dropbox_spec.yaml")
