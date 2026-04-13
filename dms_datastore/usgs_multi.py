#!/usr/bin/env python
# -*- coding: utf-8 -*-import pandas as pd

import os
import glob
import re
import pandas as pd
import tempfile
import shutil
import matplotlib.pyplot as plt
import click
import yaml
import numpy as np
from dms_datastore.dstore_config import sublocation_df
from dms_datastore.logging_config import configure_logging
from dms_datastore.read_ts import *
from dms_datastore.write_ts import *
from dms_datastore.filename import interpret_fname, meta_to_filename
import logging
from dms_datastore.logging_config import configure_logging, resolve_loglevel
logger = logging.getLogger(__name__)
from pathlib import Path

def _quarantine_file(fname, quarantine_dir="quarantine"):
    if not os.path.exists(quarantine_dir):
        os.makedirs(quarantine_dir)
    shutil.copy(fname, quarantine_dir)



def usgs_scan_series_json(fname):
    hdr = read_yaml_header(fname)

    orig_txt = hdr["original_header"]
    if orig_txt is None:
        raise ValueError("No original_header present")
    orig = parse_yaml_header(orig_txt)
    subs = orig["sublocations"]
    var = orig["variable_code"]
    series = [(str(s["subloc"]), var, s["method_description"]) for s in subs]
    return series


def usgs_scan_series(fname):
    """Scans file and returns a list of time series id, parameter codes and
    description for each series in the file

    Parameters
    ----------
    fname : str
        file to scan

    Returns
    --------

    series : list
        list of (ts_id,param,description)

    """
    try:
        scan = usgs_scan_series_json(fname)
        return scan
    except:
        # This code is the old scanning code for rdb format
        descript_re = re.compile(r"(\#\s+)?\#\s+TS_ID\s+Parameter\s+Description")

        def read_ts_data(line):
            # This method of splitting "gives up" and leaves description intact
            parts = line.strip().split(None, 3)
            if len(parts) < 2 or (len(parts) == 2 and parts[1] == "#"):
                describing = 2
                return describing, (None, None, None)
            if (
                parts[1] == "#"
            ):  # There are two comments, redo split to get description intact
                parts = line.strip().split(None, 4)
            parts = [p for p in parts if p != "#"]
            ts_id, param, descr = parts[0:3]
            describing = 1
            return describing, (ts_id, param, descr)

        series = []
        describing = 0  # state of the parser, =1 when entering descriptionsection and =2 when leaving
        formatted = False
        with open(fname, "r") as g:
            descrline = None
            for line in g:
                if "Parameter" in line:
                    pass
                    # print(fname)
                    # print(line)
                if "original_header" in line:
                    formatted = True  # there may be two comments
                if descript_re.match(line):
                    describing = 1
                    continue
                elif describing == 1:
                    describing, (ts_id, param, descr) = read_ts_data(line)
                    if describing == 1:
                        series.append([ts_id, param, descr])
                elif describing == 2:
                    break
            if describing < 2:
                raise ValueError(
                    f"Time series description section not found in file {fname} using either the json or rdb assumption"
                )
        return series


def usgs_multivariate(pat, outfile):
    """Scans all NWIS-style files matching pattern pat and lists metadata for files that are multivariate

    Parameters
    ----------
    pat : str
        globbing battern to match
    outfile : str
        output file name
    """
    special_cases = [
        ("m13", "306155", "upward"),
        ("m13", "306207", "vertical"),
        ("c24", "287157", "vertical"),
        ("c24", "287159", "upward"),
    ]

    logger.info("Start scanning phase looking for multivariate entries")
    with open(outfile, "w", encoding="utf-8") as out:
        files = glob.glob(pat)
        nfiles = len(files)
        data = []
        for i,fname in enumerate(files,start=1):
            meta = interpret_fname(fname, repo="formatted")
            try:
                ts = read_ts(fname, nrows=4000)
            except:
                logger.warning(f"Failed to read file with read_ts(): {fname}")
                continue

            if i == 1 or i % 500 == 0 or i == nfiles:
                logger.info(
                    f"USGS scan progress: {i}/{nfiles} files, currently on {fname} ")


            multi_cols = ts.shape[1] > 1
            subloc_df = sublocation_df()

            station_id = meta["station_id"]
            param = meta["param"]
            known_multi = (subloc_df["station_id"] == station_id).any()
            random_check = (
                np.random.choice(2, 1, [0.9, 0.1])[0] == 1
            )  # Small chance we will check the file.

            if multi_cols or known_multi or random_check:
                message = f"usgs_meta: file {fname} Columns {ts.columns}"
                logger.debug(message)
                try:
                    series = usgs_scan_series(fname)  # Extract list of series in file
                except:
                    _quarantine_file(fname)
                    logger.warning(
                        f"Quarantined {fname} in usgs_multi. Could not scan USGS file for variables: {fname}"
                    )
                    continue
                try:
                    _ = iter(series)  # Test that the variable is iterable
                except TypeError as te:
                    _quarantine_file(fname)
                    logger.warning(
                        f"Quarantined {fname} in usgs_multi. Scan resulted in a non iterable object"
                    )
                    continue
                for s in series:
                    (ats_id, aparam, adescr) = s
                    out.write(message + "\n")
                    asubloc = "default"
                    for item in special_cases:
                        if ats_id == item[1]:
                            asubloc = item[2]
                    if "upper" in adescr.lower():
                        asubloc = "upper"
                    if "lower" in adescr.lower():
                        asubloc = "lower"
                    if "bottom" in adescr.lower():
                        asubloc = "lower"
                    if "mid" in adescr.lower():
                        asubloc = "mid"

                    if random_check and not known_multi and asubloc != "default":
                        logger.warning(
                            f"Sublocation labeling was detected during spot check in station {station_id} param {param} but no listing in subloc table"
                        )

                    yr = int(meta["year"]) if "year" in meta else int(meta["syear"])
                    data.append(
                        (
                            meta["station_id"],
                            meta["agency_id"],
                            meta["param"],
                            yr,
                            asubloc,
                            ats_id,
                            aparam,
                            adescr,
                        )
                    )
                    sout = ",".join(list(s)) + "\n"
                    out.write(sout)
            del ts

    df = pd.DataFrame(
        data=data,
        columns=[
            "station_id",
            "agency_id",
            "param",
            "syear",
            "asubloc",
            "ts_id",
            "var_id",
            "description",
        ],
    )
    df = df[~df.duplicated(subset=["ts_id"])]
    df.index.name = "id"
    df.to_csv("usgs_subloc_meta.csv", index=False)
    return df


def process_multivariate_usgs(repo="formatted", data_path=None, pat=None, rescan=True):
    """Identify and separate or combine multivariate USGS files.
    Separate sublocations if they are known (typically the vertical ones like upper/lower)
    Otherwise aggregates the columns and adds a value column containing their mean ignoring nans.
    Often only one is active at a time and in this case the treatment is equivalent to selecting
    the one that is active
    """
    logger.info("Entering process_multivariate_usgs")
    actual_fpath = data_path if data_path is not None else repo_root(repo)
    # todo: straighten out fpath and pat stuff
    with tempfile.TemporaryDirectory() as tmpdir:
        if pat is None:
            pat = os.path.join(actual_fpath, "usgs*.csv")
        else:
            pat = os.path.join(actual_fpath, pat)

        # This recreates or reuses  list of multivariate files. Being multivariate is something that has
        # to be assessed over the full period of record
        if rescan:
            df = usgs_multivariate(pat, "usgs_subloc_meta_new.csv")
        else:
            df = pd.read_csv("usgs_subloc_meta.csv", header=0, dtype=str)
        df.reset_index()
        df.index.name = "id"
        filenames = glob.glob(pat)
        set_of_deletions = set()

        logger.info("Begin usgs_multi consolidation and separation phase")
        for i,fn in enumerate(filenames,start=1):
            direct, filepart = os.path.split(fn)

            
            meta = interpret_fname(filepart, repo="formatted")
            station_id = meta["station_id"]
            param = meta["param"]
            logger.info(f"Working on {fn}, {station_id}, {param}")
            subdf = df.loc[(df.station_id == station_id) & (df.param == param), :]
            if subdf.empty:
                logger.debug("No entry in table indicating multivariate content, skipping")
                continue
            # if len(subdf) == 1:
            #    logger.info("Dataset with only one sublocation not expected for station_id {station_id}")

            original_header = read_yaml_header(fn)

            ts = read_ts(fn)
            logger.debug(
                f"Number of sublocation metadata entries for {station_id} {param} = {len(subdf)}"
            )
            vertical_non = [0, 0]  # for counting how many subloc are vertical or not

            # Partition every present source column into semantic sublocation groups,
            # then reduce each group to a single univariate "value" series.
            grouped_cols = {}

            for index, row in subdf.iterrows():
                ts_id = str(row.ts_id)
                asubloc = str(row.asubloc)

                selector = (
                    "value"
                    if len(ts.columns) == 1 and ts.columns[0] == "value"
                    else f"{ts_id}_value"
                )

                if selector not in ts.columns:
                    logger.debug(f"Selector failed: {selector} columns: {ts.columns}")
                    continue

                # Keep existing mapped/lookup semantics from the scan table.
                # Only normalize empty/unknown labels to default here.
                bucket = str(asubloc).strip().lower()
                if bucket in ["", "nan", "none"]:
                    bucket = "default"

                grouped_cols.setdefault(bucket, []).append((selector, row))

            written_any = False

            for bucket, members in grouped_cols.items():
                cols = [col for col, _ in members if col in ts.columns]
                if not cols:
                    continue

                # Collapse this bucket to a single univariate series.
                if len(cols) == 1:
                    out = ts[[cols[0]]].copy()
                else:
                    out = ts[cols].mean(axis=1, skipna=True).to_frame()

                out.columns = ["value"]

                # Skip empty outputs
                if not out["value"].notna().any():
                    logger.debug(
                        f"Grouped output for {station_id} {param} bucket {bucket} is all-NA; skipping"
                    )
                    continue

                meta_out = dict(original_header)

                ts_ids = [str(r.ts_id) for _, r in members]
                var_ids = [str(r.var_id) for _, r in members]

                if len(ts_ids) == 1:
                    meta_out["agency_ts_id"] = ts_ids[0]
                else:
                    meta_out["agency_ts_id"] = ts_ids

                if len(var_ids) == 1:
                    meta_out["agency_var_id"] = var_ids[0]
                else:
                    meta_out["agency_var_id"] = var_ids

                meta_out["subloc"] = bucket

                if len(cols) > 1:
                    meta_out["subloc_comment"] = (
                        f"value averages {len(cols)} source series assigned to sublocation {bucket}"
                    )
                else:
                    meta_out["subloc_comment"] = (
                        "multivariate file separated into sublocation outputs"
                    )

                meta_out["source_columns"] = cols

                meta_file = dict(meta)
                meta_file["subloc"] = bucket

                newfname = meta_to_filename(meta_file, repo="formatted")
                work_dir, newfname_f = os.path.split(newfname)
                newfpath = os.path.join(tmpdir, newfname_f)

                logger.debug(
                    f"Writing grouped output for {station_id} {param} bucket {bucket} "
                    f"from columns {cols} to {newfpath}"
                )
                write_ts_csv(out, newfpath, meta_out, chunk_years=True)
                written_any = True

            if written_any:
                logger.debug(
                    f"Processed multivariate file {fn} into grouped outputs; marking original for deletion"
                )
                set_of_deletions.add(fn)
            else:
                logger.warning(
                    f"Quarantining {fn} in usgs_multi: no non-empty grouped outputs could be formed"
                )
                _quarantine_file(fn)

        for fdname in set_of_deletions:
            logger.debug(f"Removing {fdname}")
            os.remove(fdname)
        shutil.copytree(tmpdir, actual_fpath, dirs_exist_ok=True)

    logger.info("Exiting process_multivariate_usgs")


@click.command()
@click.option("--pat", default="usgs*.csv", help="Pattern of files to process")
@click.option("--repo", default="formatted", help="Configured repo name for naming/parse rules.")
@click.option(
    "--fpath",
    default=None,
    help="Directory containing the files. Defaults to the configured root of --repo.",
)
@click.option("--logdir", type=click.Path(path_type=Path), default=None)
@click.option("--debug", is_flag=True)
@click.option("--quiet", is_flag=True)
@click.help_option("-h", "--help")
def usgs_multi_cli(pat, repo, fpath, logdir=None, debug=False, quiet=False):
    """CLI for processing multivariate USGS files."""
    # recatalogs the unique series. If false an old catalog will be used, which is useful
    # for sequential debugging.
    rescan = True

    level, console = resolve_loglevel(
        debug=debug,
        quiet=quiet,
    )
    configure_logging(
          package_name="dms_datastore",
          level=level,
          console=console,
          logdir=logdir,
          logfile_prefix="usgs_multi"
    )        
    process_multivariate_usgs(repo=repo, data_path=fpath, pat=pat, rescan=True)


if __name__ == "__main__":
    usgs_multi_cli()
