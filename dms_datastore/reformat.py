#!/usr/bin/env python
# -*- coding: utf-8 -*-
import concurrent.futures
import glob
import os
import traceback
import argparse
import pandas as pd
import matplotlib.pyplot as plt
from dms_datastore.read_ts import *
from dms_datastore.write_ts import *
from dms_datastore.populate_repo import interpret_fname
from dms_datastore.dstore_config import config_file, station_dbase


def block_bounds(ts, block_size):
    firstyr = ts.first_valid_index().year
    lastyr = ts.last_valid_index().year
    # Get a number that is "neat" w.r.t. block size
    neat_lower_bound = int(block_size * (firstyr // block_size))
    neat_upper_bound = int(block_size * (lastyr // block_size))
    bounds = []
    for bound in range(neat_lower_bound, neat_upper_bound + 1, block_size):
        lo = max(firstyr, bound)
        hi = min(lastyr, bound + block_size - 1)
        hi = bound + block_size - 1
        bounds.append((lo, hi))
    return bounds


def test_block_size():
    dr = pd.date_range(pd.Timestamp(2003, 10, 1), freq="H", periods=200000)
    data = np.arange(0., 200000.)
    df = pd.DataFrame(data=data, index=dr)
    trial = df["20090301":"20130101"]
    print("***")
    print(trial)
    print(block_bounds(trial, 5))
    print("***")
    trial = df["20100301":"20110101"]
    print(trial)
    print(block_bounds(trial, 1))
    print("***")
    trial = df["20100301":"20150201"]
    print(trial)
    print(block_bounds(trial, 1))


variable_mappings = None


def infer_unit(fname, param, src):
    global variable_mappings
    if variable_mappings is None:
        variablemapfile = config_file("variable_mappings")
        variable_mappings = pd.read_csv(variablemapfile, header=0, comment="#")
    thisvar = variable_mappings.loc[(variable_mappings.var_name == param)
                                     & (variable_mappings.src_name == src), :]
    print(thisvar)


def ncro_header(fname):
    header = []
    with open(fname, "r") as infile:
        for i, line in enumerate(infile):
            if i == 2:
                if len(line.split()) == 3:
                    return ""
                blanks_in_row = 0
            elif i > 2 and i < 100:
                item = line.split(",")[-1]
                blanks_in_row = blanks_in_row + 1 if len(item.strip()) == 0 else 0
                if blanks_in_row == 2:
                    return "\n".join(header)
                # print(header)
                header.append(item.strip())


def cdec_unit(fname):
    unit_reformat = {"FEET": "feet", "MG/L": "mg/l", "CFS": "ft^3/s", "PH": "pH", "PSU": "psu",
        "DEG F": "deg_f", "DEG C": "deg_c", "FT/SEC": "ft/s", "uS/cm": "uS/cm", "NTU": "NTU", "FNU": "FNU"}
    ts = pd.read_csv(fname, header=0, nrows=1)
    agency_unit = ts.UNITS.iloc[0]
    if agency_unit in unit_reformat:
        unit = unit_reformat[agency_unit]
    else:
        unit = "unmapped"
    return unit, agency_unit


def ncro_unit(header_text, param):
    unit_reformat = {"feet": "feet",
                     "microsiemens/cm": "microS/cm",
                     "mg/l": "mg/l",
                     "milligrams/litre": "mg/l", "cfs": "ft^3/s",
                     "cubic feet/second": "ft^3/s",
                     "ph": "pH", "PSU": "psu",
                     "degrees farenheit": "deg_f",
                     "degrees celsius": "deg_c",
                     "feet/second": "ft/s",
                     "us/cm": "uS/cm",
                     "ntu": "NTU", "fnu": "FNU",
                     "degrees c": "deg_c",
                     "micrograms/litre": "ug/l",
                     "pss-15": "psu",
                     "quinine sulfate unit": "ug/l"  # for fdom
                     }
    var_to_unit = {"pH": "pH", "Flow": "ft^3/s", "Conductivity": "uS/cm", }
    param_defaults = {"ec": "uS/cm", "turbidity": "NTU",
                     "elev": "feet",
                     "temp": "deg_c",
                     "velocity": "ft/s",
                     "flow": "ft^3/s",
                     "fdom": "ug/l",
                     "do": "mg/l"}

    # 860.00 - pH () " header_text comes in without comments \s\((\.?)\)
    var = re.compile("[0-9.]+\s-\s(.+)\((.*)\)")
    parsing = False
    if header_text is None:
        # No choice but to guess based on probable units for the parameter
        return param_defaults[param]  # No guarantee this will work
    for line in header_text.split("\n"):
        if "Variables:" in line:
            parsing = True
        elif parsing:
            varmatch = var.match(line)
            agency_variable = varmatch.group(1).strip()
            agency_unit = varmatch.group(2).strip()
            parsing = False
            if agency_unit.lower() in unit_reformat:
                return unit_reformat[agency_unit.lower()]
            elif agency_variable in var_to_unit:
                return var_to_unit[agency_variable]
            else:
                raise ValueError(f"unrecognized variable/unit {agency_variable}, {agency_unit}")


def infer_internal_meta_for_file(fpath):
    slookup = station_dbase()
    fname = os.path.split(fpath)[1]
    meta = interpret_fname(fname)
    meta_out = {}
    meta_out["param"] = meta["param"]
    source = meta["agency"]
    meta_out["source"] = source
    station_id = meta["station_id"]
    meta_out["station_id"] = meta["station_id"]
    station_name = slookup.loc[station_id, 'name']
    meta_out["station_name"] = station_name
    meta_out["sublocation"] = meta["subloc"] if meta["subloc"] is not None else "default"
    meta_out["agency_id"] = meta["agency_id"]
    meta_out["latitude"] = slookup.loc[station_id, 'lat']
    meta_out["longitude"] = slookup.loc[station_id, 'lon']
    meta_out["projection_x_coordinate"] = slookup.loc[station_id, 'x']
    meta_out["projection_y_coordinate"] = slookup.loc[station_id, 'y']
    meta_out["projection_authority_id"] = "epsg:26910"
    meta_out["crs_note"] = "Reported lat-lon are agency provided. Projected coordinates may be revised."
    original_hdr = ncro_header(
        fpath) if meta_out["source"] == "ncro" else original_header(fpath, "#")
    if source == "cdec":
        unit, agency_unit = cdec_unit(fpath)
        meta_out["unit"] = unit
        meta_out["agency_unit"] = agency_unit
    elif source == "ncro":
        try:
            unit = ncro_unit(original_hdr, meta["param"])
        except:
            print(f"Unit not parsed in {fpath}, original header:")
            print(str(original_hdr))
            unit = "Unknown"
        meta_out["unit"] = unit

    meta_out["original_header"] = original_hdr
    return meta_out


def reformat():
    allfiles = glob.glob("raw/*.csv") + glob.glob("raw/*.rdb")
    #allfiles = glob.glob("raw/des_bdl_49_temp_2009_2018.csv")
    allfiles.sort()
    block_size = 1
    startupstr = "ncro_blp"  # file name fragment for resuming or None if start from beginning
    # below doesn't have to be the case, but hard wire
    single_year_label = (block_size == 1)

    if startupstr is None:
        at_start = True
        startupstr = ''
    else:
        at_start = False
    for fpath in allfiles:
        if startupstr in fpath:
            at_start = True
        if not at_start:
            continue
        hdr_meta = infer_internal_meta_for_file(fpath)

        df = read_ts(fpath, force_regular=False)
        df.index.name = "datetime"
        df.sort_index(inplace=True)  # possibly non-monotonic
        #nonmonotone = df.loc[df.index.to_series().diff() < pd.to_timedelta('0 seconds')]

        # This names things uniformally
        if not ("usgs_" in fpath and df.shape[1] > 1):
            df.columns = ["value"]

        newfname = os.path.join(f"formatted", os.path.split(fpath)[1])
        newfname = newfname[:-14] + ".csv"
        content = ""
        for item in hdr_meta:
            if item == "original_header":
                if (hdr_meta[item] is None) or (len(hdr_meta[item]) <= 1):
                    content = content + "original_header: None"
                else:
                    content = content + "original_header: |\n"
                    content = content + hdr_meta[item]
            else:
                content = content + (item + ": " + hdr_meta[item] + "\n")
        write_ts_csv(df, newfname, content, chunk_years=True)


def test_ncro_header(fname):
    print(ncro_header(fname))
    print(infer_internal_meta_for_file(fname))
    print(cdec_unit(fname))


def test_cdec_units(fname):
    import cfunits
    fname = "raw/ncro_tpi_b9542100_temp_2014_2021.csv"
    fname = "raw/cdec_oad_b95366_ec_2021_9999.csv"
    # test_ncro_header(fname)
    all_files = glob.glob("raw/cdec*9999.csv")
    for fname in all_files:
        cdu = cdec_unit(fname)
        x = cfunits.Units(cdu)
        print(x)


def reformat_source(inpath, src, outpath):
    glob0 = os.path.join(inpath, f"{src}*.csv")
    glob1 = os.path.join(inpath, f"{src}*.rdb")
    allfiles = glob.glob(glob0) + glob.glob(glob1)
    allfiles.sort()
    block_size = 1
    startupstr = None  # file name fragment for resuming or None if start from beginning
    # below doesn't have to be the case, but hard wire
    single_year_label = (block_size == 1)

    if startupstr is None:
        at_start = True
        startupstr = ''
    else:
        at_start = False
    for fpath in allfiles:
        if startupstr in fpath:
            at_start = True
        if not at_start:
            continue
        hdr_meta = infer_internal_meta_for_file(fpath)

        df = read_ts(fpath, force_regular=False)
        df.index.name = "datetime"
        df.sort_index(inplace=True)  # possibly non-monotonic
        if df.first_valid_index() is None:
            print(f"Skipping {fpath} because no valid data found")
            continue
        #nonmonotone = df.loc[df.index.to_series().diff() < pd.to_timedelta('0 seconds')]

        # This names things uniformally
        if not ("usgs_" in fpath and df.shape[1] > 1):
            df.columns = ["value"]

        newfname = os.path.join(outpath, os.path.split(fpath)[1])
        newfname = newfname[:-14] + ".csv"
        content = ""
        for item in hdr_meta:
            if item == "original_header":
                if (hdr_meta[item] is None) or (len(hdr_meta[item]) <= 1):
                    content = content + "original_header: None"
                else:
                    content = content + "original_header: |\n"
                    content = content + hdr_meta[item]
            else:
                content = content + f"{item}: {hdr_meta[item]}\n"
        write_ts_csv(df, newfname, content, chunk_years=True)


def reformat_main(inpath="raw", outpath="formatted", agencies=["usgs", "des", "cdec", "noaa", "ncro"]):
    all_agencies = agencies
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_agency = {executor.submit(reformat_source, inpath, agency, outpath):
                            agency for agency in all_agencies}

    for future in concurrent.futures.as_completed(future_to_agency):
        agency = future_to_agency[future]
        try:
            data = future.result()
        except Exception as exc:
            trace = traceback.format_exc()
            print(f'{agency} generated an exception: {exc} with traceback:\n{trace}')


def create_arg_parser():
    parser = argparse.ArgumentParser('Delete files contained in a list')

    parser.add_argument('--raw', dest="raw", default=None,
                        help='Directory where files will be stored. ')
    parser.add_argument('--formatted', dest="formatted", default=None,
                        help='Directory where files will be stored. ')
    parser.add_argument('--agencies', nargs='+', default=[],
                        help='Agencies to process. If not specified, does ["usgs","des","cdec","noaa","ncro"].')
    return parser


def main():
    parser = create_arg_parser()
    args = parser.parse_args()
    raw_dir = args.raw
    formatted_dir = args.formatted
    agencies = args.agencies
    if agencies is None or len(agencies) == 0:
        agencies = ["usgs", "des", "cdec", "noaa", "ncro"]

    reformat_main(inpath=raw_dir, outpath=formatted_dir, agencies=agencies)


if __name__ == "__main__":
    main()
