#!/usr/bin/env python
# -*- coding: utf-8 -*

import argparse
import glob
import os
import random
import yaml
import copy
import pandas as pd
import matplotlib.pyplot as plt
from dms_datastore.read_ts import *
from vtools.functions.error_detect import *
from vtools.data.timeseries import to_dataframe
from vtools.data.gap import *
from dms_datastore.read_multi import *
from dms_datastore.dstore_config import *
from dms_datastore.filename import interpret_fname
from dms_datastore.inventory import *
from dms_datastore.write_ts import *
from schimpy.station import *
import geopandas as gpd
import numpy as np
import seaborn as sns
from shapely.geometry import Point

# Todo: left join regions to station dbase and make this a local rather than global
region_checkers = {}


def screener(
    ts,
    station_id,
    subloc,
    param,
    protocol,
    do_plot=False,
    plot_label=None,
    return_anomaly=False,
    plot_dest="interactive",
):
    """Performs yaml-specified screening protocol on time series"""
    print("screener", station_id, subloc, param)
    # name = protocol['name']
    steps = protocol["steps"]
    full = None
    ts_process = ts.copy()
    nstep = len(steps)
    for step in steps:
        method_name = step["method"]
        label = step["label"] if "label" in step else method_name
        print("Performing:", label)
        method = globals()[method_name]

        args = step["args"]
        for key in args:
            if args[key] == "station_id" and args[key] == "station_id":
                args["station_id"] = station_id
            if args[key] == "subloc" and args[key] == "subloc":
                args["subloc"] = subloc
            if args[key] == "param" and args[key] == "param":
                args["param"] = param
        print(station_id, subloc, param)
        print("step:")
        print(step)
        if len(ts_process.columns) > 1:
            if "value" in ts_process.columns:
                ts_process = ts_process.value.to_frame()
            else:
                raise ValueError(
                    "Multiple columns with no 'value' column to evaluate is unexpected"
                )

        anomaly = method(ts_process, **args)
        if "apply_immediately" in step and step["apply_immediately"]:
            ts_process = ts_process.mask(anomaly)

        # Create column with step label as column name in dataframe of anomaly results
        if full is None:
            try:
                full = anomaly.to_frame()
            except:
                full = anomaly
            full.columns = [label]
        else:
            full[label] = anomaly

    if do_plot:
        plot_anomalies(
            ts_process, full, plot_label, gap_fill_final=3, plot_dest=plot_dest
        )
    # This uses nullable integer so we can leave blank
    ts["user_flag"] = full.any(axis=1).astype(pd.Int64Dtype())  # This is nullable
    ts["user_flag"] = ts["user_flag"].mask(ts["user_flag"] == 0, other=pd.NA)
    if return_anomaly:
        return ts, full
    else:
        return ts


def plot_anomalies(
    ts, anomaly_df, plot_label, gap_fill_final=0, plot_dest="interactive"
):
    mask = anomaly_df.any(axis=1)
    fig, (ax0, ax1) = plt.subplots(2, sharex=True, sharey=True)
    ts.plot(label="series", color="0.7", ax=ax0)
    nstep = len(anomaly_df.columns)
    for i, col in enumerate(anomaly_df.columns):
        subts = ts.loc[anomaly_df[col]]  # anomaly_df is binary, so acts on index
        try:
            enough = subts.squeeze().notnull().sum() > 1
        except:
            enough = False
        if enough:
            subts = to_dataframe(subts)
            try:
                ax0.scatter(
                    subts.index,
                    subts.value,
                    marker="o",
                    label=col,
                    s=20 + 20 * ((nstep - i) / 2),
                )
            except:
                print(f"Problem time series: {subts}")
                raise ValueError("Time series could not be plotted")
    ax0.legend()
    if plot_label is None:
        plot_label = f"Station: {station_id} Subloc: {subloc} Param {param}"
    ax0.set_title(plot_label)
    ts_masked = ts.mask(mask).interpolate(limit=gap_fill_final)
    ts_masked.plot(ax=ax1)
    ff = "_".join(plot_label.split("_")[1:])
    if plot_dest == "interactive":
        plt.show()
    else:
        fname = os.path.join(plot_dest, ff)
        plt.savefig(f"{fname}.png")
    plt.close(fig)


def filter_inventory_(inventory, stations, params):
    if stations is not None:
        if isinstance(stations, str):
            stations = [stations]
        inventory = inventory.loc[
            inventory.index.get_level_values("station_id").isin(stations), :
        ]
    if params is not None:
        if isinstance(params, str):
            params = [params]
            print(params)
        inventory = inventory.loc[
            inventory.index.get_level_values("param").isin(params), :
        ]
    print(inventory)
    return inventory


def auto_screen(
    fpath="formatted",
    config=None,
    dest="screened",
    stations=None,
    params=None,
    plot_dest="plots",
    start_station=None,
):
    """Auto screen all data in directory
    Parameters
    ----------
    config : str
        Yaml global config or path that can be loaded as such

    fpath : str
        Path to files to process

    dest : str
        Path for screened data

    stations : list
        List of stations

    params : list
        List of params to process

    plot_dest : str
        Directory name for diagnostic plots

    start_station : str
        In case of crash, restarts processing at this station
    """
    if os.path.exists(config):
        # file name was given
        screen_config = load_config(config)
        screen_config["config_dir"] = os.path.split(config)[0]
    else:
        # yaml struct
        screen_config = config

    active = start_station is None

    station_db = station_dbase()
    inventory = repo_data_inventory(fpath)
    inventory = filter_inventory_(inventory, stations, params)
    failed_read = []

    for index, row in inventory.iterrows():
        station_id = index[0]
        if not active:
            if station_id == start_station:
                active = True
            else:
                continue
        subloc = index[1]
        if type(subloc) == float:
            subloc = "default"
        param = index[2]
        if subloc is None:
            subloc = "default"
        if np.random.uniform() < 0.0:  # 0.95:
            print(f"Randomly rejecting: {station_id} {subloc} {param}")
            continue
        filename = str(row.filename)
        station_info = station_db.loc[station_id, :]
        agency = row.agency_dbase
        if agency.startswith("dwr_"):
            agency = agency[4:]  # todo: need to take care of des_ vs dwr_des etc

        # Now we have most information, but the time series may be split between sources
        # with low and high priority
        fetcher = custom_fetcher(agency)
        # these may be lists
        try:
            meta_ts = fetcher(fpath, station_id, param, subloc=subloc)
        except:
            print("Read failed for ", station_id, param, subloc)

        if meta_ts is None:
            print(f"No data found for {station_id} {subloc} {param}")
            failed_read.append((station_id, subloc, param))
            print("Cumulative fails:")
            for fr in failed_read:
                print(fr)
            continue
        metas, ts = meta_ts
        meta = metas[0]
        subloc_actual = (
            meta["sublocation"]
            if "sublocation" in meta
            else meta["subloc"]
            if "subloc" in meta
            else "default"
        )
        proto = context_config(screen_config, station_id, subloc, param)
        do_plot = True
        subloc_label = "" if subloc == "default" else subloc
        plot_label = f"{station_info['name']}_{station_id}@{subloc_label}_{param}"
        screened = screener(
            ts,
            station_id,
            subloc_actual,
            param,
            proto,
            do_plot,
            plot_label,
            plot_dest=plot_dest,
        )
        if "value" in screened.columns:
            screened = screened[["value", "user_flag"]]
        meta["screen"] = proto
        if subloc_actual and subloc_actual != "default":
            output_fname = (
                f"{agency}_{station_id}@{subloc_actual}_{row.agency_id}_{param}.csv"
            )
        else:
            output_fname = f"{agency}_{station_id}_{row.agency_id}_{param}.csv"
        output_fpath = os.path.join(dest, output_fname)
        write_ts_csv(screened, output_fpath, meta, chunk_years=True)


def update_steps(proto, x):
    """Modifies the steps in proto with changes in x.
    Changes may be modifications (modfify_steps), additions at the end (add_steps) or removal (omit_steps)
    """
    if x["inherits_global"]:
        omissions = x["omit_steps"] if "omit_steps" in x else []
        adds = x["add_steps"] if "add_steps" in x else []
        newsteps = []
        for step in proto["steps"]:
            steplabel = step["label"] if "label" in step else step["method"]
            if steplabel in omissions:
                omissions.remove(
                    steplabel
                )  # for tracking if there are unused omissions
                continue  # don't include
            newsteps.append(step)
        if len(omissions) > 0:
            print(
                "Omissions listed but not found in inherited specification: ", omissions
            )
        finalsteps = []
        for step in newsteps:
            steplabel = step["label"] if "label" in step else step["method"]
            replace = None
            if "modify_steps" in x:
                for modif in x["modify_steps"]:
                    label = modif["label"] if "label" in modif else modif["method"]
                    if steplabel == label:
                        replace = modif
            finalsteps.append(replace if replace is not None else step)
        proto["steps"] = finalsteps + adds
        return proto
    else:  # Assumes a complete protocol spec is supplied as replacement
        return x


def load_config(config_file):
    with open(config_file, "r") as stream:
        try:
            screen_config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
            raise
    return screen_config


def context_config(screen_config, station_id, subloc, param):
    """Find the screening specification for the given station and param
    Parameters
    ----------
    config : yaml struct
        Global congiguration

    station_id : str
        Station id (from dbase) of station

    subloc : str
        Sublocation of data

    param : str
        Variable/parameter in the file


    """

    station_info = station_dbase()
    print("config")
    print(screen_config)
    region_file = screen_config["regions"]["region_file"]
    print("REgion file: ", region_file)

    if not (os.path.exists(region_file)):
        region_file = os.path.join(screen_config["config_dir"], region_file)

    # Search for applicable region
    print("station_id: ", station_id, " subloc: ", subloc, " param: ", param)
    x = station_info.loc[station_id, "x"]
    y = station_info.loc[station_id, "y"]
    region = spatial_config(region_file, x, y)

    region_name = region.name.item()

    config = copy.deepcopy(screen_config)
    update_global = None
    proto = config["defaults"]["global"]

    # now possibly update the configuration based on just the variable
    # this is the new default for the rest of the operation
    param_config = config["defaults"]["params"]
    if param in param_config:
        update_global = param_config[param]
        proto = update_steps(proto, update_global)  # updates components or replaces

    # next try for region+variable
    region_config = None
    if region_name in config["regions"]:
        region_config = config["regions"][region_name]
        if param in region_config["params"]:
            update_region = region_config["params"][param]
            print("region var\n", proto, "\n", update_region)
            proto = update_steps(proto, update_region)
            print("region var 2\n", proto, "\nafter\n", update_region)

    # first priority: match station and variable
    station_config = None
    if station_id in config["stations"]:
        print("Found station")
        station_config = config["stations"][station_id]
        if param in station_config["params"]:
            update_station = station_config["params"][param]
            print("station var\n", proto, "\nthen\n", update_station)
            proto = update_steps(proto, update_station)
            print("station var 2\n", proto, "\nafter\n", update_station)

    return proto


class RegionChecker(object):
    def __init__(self, fname):
        self.shp = gpd.read_file(fname)  # open the shapefile

    def region_info(self, x, y):
        if np.isscalar(x):
            x = [x]
        if np.isscalar(y):
            y = [y]
        df = pd.DataFrame({"x": x, "y": y})
        df["coords"] = list(zip(df["x"], df["y"]))
        df["coords"] = df["coords"].apply(Point)
        points = gpd.GeoDataFrame(df, geometry="coords", crs=self.shp.crs)
        point_in_polys = gpd.tools.sjoin(points, self.shp, op="within", how="left")
        return point_in_polys


def bounds_test(ts, bounds):
    return threshold(ts, bounds)


def dip_test(ts, low, dip):
    is_miss = ts.isnull()
    bad_dip = ((ts.shift(1) - ts) > dip) | is_miss.shift(1)
    bad_dip &= ((ts.shift(-1) - ts) > dip) | is_miss.shift(-1)
    bad_dip &= ts < low
    return bad_dip


def repeat_test(ts, max_repeat, lower_limit=None, upper_limit=None):
    """Detects anomalies based on too many repeats of a value"""
    nrepeats = nrepeat(ts)

    # Thresholding and repeats are taken care of first
    bad = nrepeats > max_repeat
    if lower_limit is not None:
        bad &= ts >= lower_limit
    if upper_limit is not None:
        bad &= ts <= upper_limit
    return bad


def short_run_test(ts, small_gap_len, min_run_len):
    """Mark very small clumps of allegedly good data sandwiched among big gaps
    as anomalies based on isolation

    """
    # test_gap is a test series with gaps <= smallgaplen filled
    test_gap = gapdist_test_series(
        ts, smallgaplen=small_gap_len
    )  # 1 is for flow, was 2
    # Now compute runs of good data
    newgoodcount = gap_count(test_gap, state="good")
    long_enough_run = 2  # 12 if for flow, was 24
    bad_run = (newgoodcount > 0) & (newgoodcount < long_enough_run)  # was 24
    return bad_run


class DatumAdj(object):
    """Datum adjustment, typically for an NGVD to NAVD88 transition"""

    def __init__(self, adj, adjdate):
        self.adj = adj
        self.adjdate = adjdate

    def __call__(self, x):
        x.loc[: self.adjdate, :] += self.adj
        return x


def spatial_config(configfile, x, y):
    if not configfile in region_checkers:
        region_checkers[configfile] = RegionChecker(configfile)
    checker = region_checkers[configfile]
    return checker.region_info(x, y)


# datum_adj = pd.read_csv("datum_adjusts.csv",sep=",",index_col="station_id")
# usgs_subloc = pd.read_csv("usgs_meta3.csv",sep=",",dtype={"station_id":str,"param":str,"ts_id":str,"var_id":str})


def ncro_fetcher(repo_path, station_id, param, subloc):
    """Reads NCRO data, correctly folding together NCRO and CDEC by priority.
    Celsius is converted to Farenheit
    """
    return read_ts_repo(
        station_id,
        param,
        subloc=subloc,
        src_priority=["ncro", "cdec"],
        repo=repo_path,
        meta=True,
    )


def general_fetcher(repo_path, station_id, param, subloc):
    """Fetches from a well behaved and standard repo"""
    return read_ts_repo(station_id, param, subloc=subloc, repo=repo_path, meta=True)


def custom_fetcher(agency):
    if agency in ["ncro", "dwr_ncro"]:
        return ncro_fetcher
    else:
        return general_fetcher


def test_single(fname):  # not maintained
    ts = read_ts(fname)
    fpart = os.path.split(fname)[1]
    parts = fpart.split("_")
    agency, station_id, param = parts[0], parts[1], parts[3]
    if "@" in station_id:
        station_id, subloc = station_id.split("@")
    else:
        subloc = None
    if ts is None:
        raise ValueError("Series is None")
    proto = context_config(station_id, subloc, param)
    screener(ts, station_id, subloc, param, proto)


def create_argparse():
    parser = argparse.ArgumentParser(
        description="Auto-screen individual files or whole repos."
    )
    parser.add_argument(
        "--config", type=str, help="yaml file containing screening criteria and methods"
    )
    parser.add_argument(
        "--fpath", type=str, default=None, help="directory containing data"
    )
    parser.add_argument(
        "--dest", type=str, help="destination directory for screened data"
    )
    parser.add_argument("--stations", nargs="+", type=str)
    parser.add_argument("--params", nargs="+", type=str)
    parser.add_argument("--plot_dest", default="interactive", type=str)
    parser.add_argument(
        "--start_station",
        type=str,
        help="Station id for starting or restarting the screening process.",
    )
    return parser


def main():
    parser = create_argparse()
    args = parser.parse_args()
    params = args.params
    stations = args.stations
    repo = args.fpath
    dest = args.dest
    global_config = args.config
    plot_dest = args.plot_dest
    start_station = args.start_station
    config = args.config
    if config is None:
        config = config_file("screen_config")
    elif not os.path.exists(config):
        config = config_file(config)

    auto_screen(
        fpath=repo,
        config=config,
        dest=dest,
        stations=stations,
        params=params,
        plot_dest=plot_dest,
        start_station=start_station,
    )


if __name__ == "__main__":
    main()
