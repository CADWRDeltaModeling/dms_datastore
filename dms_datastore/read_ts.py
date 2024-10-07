# import csv
import pandas as pd
import numpy as np
import datetime as dtm
import glob
import yaml
import re
import warnings  # temporary?
from collections import defaultdict
from vtools.functions.merge import *
from vtools.data.vtime import minutes
from dms_datastore.filename import extract_year_fname

__all__ = ["original_header", "read_yaml_header", "read_ts"]


def read_flagged(
    fpath_pattern,
    start=None,
    end=None,
    apply_flags=True,
    return_flags=False,
    return_meta=False,
):
    """This is an independent reader routine for univariate data in dms_datastore format
    Parameters
    ----------

    fpath_pattern : str
        File path to read, wildcards included

    start : str or Timestamp
        Start of read, saves time if years can be avoided

    end : str or Timestamp
        End of read, saves time if years can be avoided

    apply_flags: bool
        Apply user_flag as a mask, converting data to nan

    return_flags: bool
        If True, the user_flag column is returned

    return_meta: bool
        If True, the yaml header is parsed and returned, such that the return value is a tuple (meta,ts)

    Returns
    -------
        DataFrame if return_meta is false, otherwise tuple(yaml structure, DataFrame)

    """
    fdir, fpat = path_pattern(fpath_pattern)

    if not fdir:
        fdir = "."
    matches = []
    # for root, dirnames, filenames in os.walk(fdir):
    #    for filename in fnmatch.filter(filenames, fpat):
    #        matches.append(os.path.join(root, filename))
    matches = glob.glob(fpath_pattern)
    matches.sort()

    if len(matches) == 0:
        raise IOError("No matches found for pattern: {}".format(fpat))
    if start is None:
        startyr = -9999
    else:
        start = pd.to_datetime(start)
        startyr = start.year
    if end is None:
        endyr = 9999
    else:
        end = pd.to_datetime(end)
        endyr = end.year

    matches = [
        m
        for m in matches
        if extract_year_fname(m) >= startyr and extract_year_fname(m) <= endyr
    ]
    dfs = []
    for m in matches:
        dfs.append(
            pd.read_csv(
                m,
                sep=",",
                comment="#",
                index_col=0,
                parse_dates=[0],
                dtype={"user_flag": str},
            )
        )
    df = pd.concat(dfs, axis=0)
    if apply_flags:
        df.mask(df.user_flag == "1", inplace=True)
    if not return_flags:
        df.drop("user_flag", axis=1, inplace=True)
    if return_meta:
        meta = read_yaml_header(matches[0])
        return meta, df.loc[start:end]
    else:
        return df.loc[start:end]


def original_header(fpath, comment="#"):
    """Scrapes the header from a file assuming all the lines at the top are the header

    Parameters
    ----------
    fpath : str
        Path to the file being parsed

    comment : str
        Comment character, default '#'

    Returns
    -------
    Header as single string, possibly with embedded end lines
    """
    original_head = []
    with open(fpath, "r") as infile:
        for line in infile:
            if line.startswith(comment):
                original_head.append(line.strip().replace("#", "#  "))
            else:
                allheader = original_head
                break
    return "\n".join(original_head) if len(original_head) > 0 else ""


def read_yaml_header(fpath):
    """Reads yaml-based header at top of file

    Parameters
    ----------
    fpath : str
        File with header to read

    Returns
    -------
    Nested yaml data structure (lists and dicts)

    """

    header = original_header(fpath)
    header = "\n".join([x.replace("#", "") for x in header.split("\n")])
    try:
        yamlhead = yaml.safe_load(header)
    except:
        raise ValueError(f"Failure reading header in file {fpath}")
    return yamlhead


def is_dms1_screen(fname):
    if not fname.endswith(".csv"):
        return False
    pattern = re.compile("#\s?format\s?:\s?dwr-dms-1.0")
    with open(fname, "r") as f:
        line = f.readline()
        if pattern.match(line) is None:
            return False
        for i in range(150):
            otherline = f.readline()

            if "screen:" in otherline:
                return True
    return False


def read_dms1_screen(
    fpath_pattern, start=None, end=None, selector=None, force_regular=True, nrows=None
):
    if selector is None:
        selector = "value"
    ts = csv_retrieve_ts(
        fpath_pattern,
        start,
        end,
        force_regular,
        format_compatible_fn=is_dms1_screen,
        selector=selector,
        qaqc_selector="user_flag",
        qaqc_accept=["0"],
        blank_qaqc_good=True,
        parsedates=["datetime"],
        indexcol="datetime",
        sep=",",
        skiprows=0,
        header=0,
        dateformat=None,
        comment="#",
        nrows=nrows,
    )
    return ts


def is_dms1(fname):
    if not fname.endswith(".csv"):
        return False
    pattern = re.compile("#\s?format\s?:\s?dwr-dms-1.0")
    with open(fname, "r") as f:
        line = f.readline()
        if pattern.match(line) is None:
            return False
    return True


def read_dms1(
    fpath_pattern, start=None, end=None, selector=None, force_regular=True, nrows=None
):
    ts = csv_retrieve_ts(
        fpath_pattern,
        start,
        end,
        force_regular,
        format_compatible_fn=is_dms1,
        selector=selector,
        qaqc_selector=None,
        qaqc_accept=[],
        parsedates=["datetime"],
        indexcol="datetime",
        sep=",",
        skiprows=0,
        header=0,
        dateformat=None,
        comment="#",
        nrows=nrows,
    )
    return ts


def is_ncro_std(fname):
    pattern = re.compile("#\s?provider\s?=\s?dwr-ncro")
    with open(fname, "r") as f:
        for i, line in enumerate(f):
            if i > 6:
                return False
            if pattern.match(line.lower()):
                return True


def read_ncro_std(
    fpath_pattern, start=None, end=None, selector=None, force_regular=True, nrows=None
):
    """Based on Hydstra nightly dump. WDL is a separate reader"""
    if selector is not None:
        raise ValueError(
            "selector argument is for API compatability. This is not a multivariate format, selector not allowed"
        )
    ts = csv_retrieve_ts(
        fpath_pattern,
        start,
        end,
        force_regular,
        format_compatible_fn=is_ncro_std,
        selector="value",
        qaqc_selector="qaqc_code",
        qaqc_accept=["", " ", " ", "e", "1", "2"],
        parsedates=["datetime"],
        indexcol="datetime",
        sep=",",
        skiprows=0,
        # column_names=["datetime","value","qaqc_code","qaqc_description","status"],
        column_names=["datetime", "value", "qaqc_code"],
        use_cols=[0, 1, 2, 3],
        header=0,
        dateformat=None,
        comment="#",
        nrows=nrows,
    )
    return ts


def is_des_std(fname):
    import re

    pattern0 = re.compile("#\s?provider\s?=\s?dwr-des")
    pattern1 = re.compile("#\s?provider\s?:\s?dwr-des")
    with open(fname, "r") as f:
        for i, line in enumerate(f):
            if i > 6:
                return False
            if pattern0.match(line.lower()) or pattern1.match(line.lower()):
                return True


def read_des_std(
    fpath_pattern, start=None, end=None, selector=None, force_regular=True, nrows=None
):
    if selector is not None:
        raise ValueError(
            "selector argument is for API compatability. This is not a multivariate format, selector not allowed"
        )
    ts = csv_retrieve_ts(
        fpath_pattern,
        start,
        end,
        force_regular,
        format_compatible_fn=is_des_std,
        selector="value",
        qaqc_selector="qaqc_flag_id",
        qaqc_accept=["U", "G", "A"],
        parsedates=["time"],
        indexcol="time",
        sep=",",
        header=0,
        dateformat=None,
        comment="#",
        extra_na=[""],
        prefer_age="new",
        nrows=nrows,
    )
    return ts


def is_des(fname):
    with open(fname, "r") as f:
        count = 0
        for i, line in enumerate(f):
            if i > 7:
                return False
            linelow = line.lower()
            if "result_id" in linelow:
                count += 1
            if "row#" in linelow or "row #" in linelow:
                count += 1
            if count >= 2:
                return True


def read_des(
    fpath_pattern, start=None, end=None, selector=None, force_regular=True, nrows=None
):
    if selector is not None:
        raise ValueError(
            "selector argument is for API compatability. This is not a multivariate format, selector not allowed"
        )
    ts = csv_retrieve_ts(
        fpath_pattern,
        start,
        end,
        force_regular,
        format_compatible_fn=is_des,
        selector="VALUE",
        qaqc_selector="QAQC Flag",
        qaqc_accept=["U", "G", "A"],
        parsedates=["DATETIME"],
        indexcol="DATETIME",
        skiprows=4,
        sep=",",
        header=0,
        dateformat=None,
        comment=None,
        extra_na=[""],
        prefer_age="new",
        nrows=nrows,
    )
    return ts


################################################33


def is_cdec_csv2(fname):
    with open(fname, "r") as f:
        title_line = f.readline()
        return title_line.lower().startswith("station_id,duration,sensor_number")


# STATION_ID,DURATION,SENSOR_NUMBER,SENSOR_TYPE,DATE TIME,OBS DATE,VALUE,DATA_FLAG,UNITS
def read_cdec2(
    fpath_pattern, start=None, end=None, selector=None, force_regular=True, nrows=None
):
    if selector is not None:
        raise ValueError(
            "selector argument is for API compatability. This is not a multivariate format, selector not allowed"
        )
    dateformat = "%Y%m%d %H%M"
    ts = csv_retrieve_ts(
        fpath_pattern,
        start,
        end,
        force_regular,
        selector="VALUE",
        format_compatible_fn=is_cdec_csv2,
        qaqc_selector="DATA_FLAG",
        qaqc_accept=["", " ", " ", "e", "ART", "BRT"],
        parsedates=["OBS DATE"],
        indexcol="OBS DATE",
        skiprows=0,
        sep=",",
        dtypes={"STATION_ID": str, "DATA_FLAG": str, "UNITS": str, "VALUE": float},
        dateformat="%Y%m%d %H%M",
        comment=None,
        prefer_age="new",
        nrows=nrows,
    )
    return ts


#####################################


def is_cdec_csv1(fname):
    with open(fname, "r") as f:
        title_line = f.readline()
        return title_line.lower().startswith("title:")


def read_cdec1(
    fpath_pattern, start=None, end=None, selector=None, force_regular=True, nrows=None
):
    if selector is not None:
        raise ValueError(
            "selector argument is for API compatability. This is not a multivariate format, selector not allowed"
        )

    def cdec_date_parser(*args):
        if len(args) == 2:
            x = args[0] + args[1]
            return dtm.datetime.strptime(x, "%Y%m%d%H%M")
        else:
            return dtm.datetime.strptime(args, "%Y%m%d%H%M")

    dateformat = "%Y%m%d%H%M"

    ts = csv_retrieve_ts(
        fpath_pattern,
        start,
        end,
        force_regular,
        selector="value",
        format_compatible_fn=is_cdec_csv1,
        qaqc_selector=None,
        column_names=["date", "time", "value"],
        parsedates=[["date", "time"]],
        indexcol="date_time",
        skiprows=2,
        header=None,
        sep=",",
        dateformat="%Y%m%d%H%M",
        comment=None,
        prefer_age="new",
        nrows=nrows,
    )
    return ts


def is_wdl(fname):
    with open(fname, "r") as f:
        first_line = f.readline()
        parts = first_line.split(",")
        if not len(parts) in (3, 4):
            return False
        try:
            pd.to_datetime(parts[0])
            return True
        except:
            return False


def read_wdl(
    fpath_pattern, start=None, end=None, selector=None, force_regular=True, nrows=None
):
    if selector is not None:
        raise ValueError(
            "selector argument is for API compatability. This is not a multivariate format, selector not allowed"
        )

    ts = csv_retrieve_ts(
        fpath_pattern,
        start,
        end,
        force_regular,
        selector="value",
        format_compatible_fn=is_wdl,
        qaqc_selector="qaqc_flag",
        qaqc_accept=["", " ", " ", "e", "1", "2"],
        parsedates=["datetime"],
        indexcol="datetime",
        sep=",",
        skiprows=0,
        column_names=["datetime", "value", "qaqc_flag", "comment"],
        header=None,
        dateformat=None,
        comment=None,
        nrows=nrows,
    )
    return ts


def is_wdl2(fname):
    with open(fname, "r") as f:
        first_line = f.readline()
        second_line = f.readline()
        return '"Date' in first_line and "Site" in second_line


def read_wdl2(
    fpath_pattern, start=None, end=None, selector=None, force_regular=True, nrows=None
):
    if selector is not None:
        raise ValueError(
            "selector argument is for API compatability. This is not a multivariate format, selector not allowed"
        )

    ts = csv_retrieve_ts(
        fpath_pattern,
        start,
        end,
        force_regular,
        selector="value",
        format_compatible_fn=is_wdl2,
        qaqc_selector="qaqc_flag",
        qaqc_accept=["", " ", " ", "e", "1"],
        parsedates=["datetime"],
        indexcol="datetime",
        sep=",",
        skiprows=0,
        column_names=["datetime", "value", "qaqc_flag", "comment"],
        dtypes={"comment": str},
        header=0,
        dateformat=None,
        comment=None,
        nrows=nrows,
    )
    return ts


def is_wdl3(fname):
    with open(fname, "r") as f:
        first_line = f.readline()
        second_line = f.readline()
        ret = ('"Time' in first_line) and ('"and' in second_line)
        return ret


def read_wdl3(
    fpath_pattern, start=None, end=None, selector=None, force_regular=True, nrows=None
):
    """Handles NCRO nightly dump"""
    if selector is not None:
        raise ValueError(
            "selector argument is for API compatability. This is not a multivariate format, selector not allowed"
        )
    try:
        ts = csv_retrieve_ts(
            fpath_pattern,
            start,
            end,
            force_regular,
            selector="value",
            format_compatible_fn=is_wdl3,
            dtypes={"value": float, "qaqc_flag": str},
            qaqc_selector="qaqc_flag",
            qaqc_accept=["", " ", " ", "e", "1"],
            parsedates=["datetime"],
            indexcol="datetime",
            sep=",",
            skiprows=2,
            column_names=["datetime", "value", "qaqc_flag"],
            usecols=[0, 1, 2],
            header=0,
            dateformat=None,
            comment=None,
            nrows=nrows
        )
    except pd.errors.ParserError as e:
        if "Too many columns" in str(e):
            ts = csv_retrieve_ts(
                fpath_pattern,
                start,
                end,
                force_regular,
                selector="value",
                format_compatible_fn=is_wdl3,
                qaqc_selector="qaqc_flag",
                qaqc_accept=["", " ", " ", "e", "1"],
                parsedates=["datetime"],
                indexcol="datetime",
                sep=",",
                skiprows=2,
                column_names=["datetime", "value", "qaqc_flag"],
                dtypes={"value": float, qaqc_flag: str},
                header=0,
                dateformat=None,
                comment=None,
                nrows=nrows
            )

    return ts


def is_usgs_json1(fname):
    with open(fname, "r") as f:
        line0 = f.readline()
        line1 = f.readline()
    
    if 'parse-usgs-json' in line1:
        return True 
    return False

def usgs_data_columns_json1(fname):
    scan_data = pd.read_csv(fname,
                            sep=",",
                            comment="#",
                            nrows=20,
                            index_col=0)
    cols = [col for col in scan_data.columns if "value" in col]
    return(cols)

def read_usgs_json1(fpath_pattern, start=None, end=None, selector=None, force_regular=True, nrows=None):
    if selector is None:
        selector = usgs_data_columns_json1
        qaselect = lambda x, y: x.replace("_value","_qualifiers")

    else:
        selector = listify(selector)
        qaselect = [x.replace("_value","_qualifiers") for x in selector]

    
    # See https://help.waterdata.usgs.gov/codes-and-parameters/instantaneous-and-daily-value-status-codes
    status_codes = [
        "Fld",
        "Eqp",
        "Dis",
        "Mnt",
        "Ssn",
        "Dry",
        "Pr",
        "Pmp",
        "Rat",
        "Ssn",
        "Zfl",
        "Tst",
        "***",
    ]

    # Now tack on time zone at the end
    ts = csv_retrieve_ts(
        fpath_pattern,
        start,
        end,
        force_regular,
        selector=selector,
        format_compatible_fn=is_usgs_json1,
        qaqc_selector=qaselect,
        qaqc_accept=[
            "",
            " ",
            " ",
            "A",
            "P",
            "A:[99]","A,[99]",
            "A:[91]","A,[91]",
            "A:[92]","A,[92]",
            "A:[93]","A,[93]",
            "A:R","A,R",
            "Approved",
            "e",
        ],
        extra_na=status_codes,
        parsedates=["datetime"],
        indexcol="datetime",
        header=0,
        sep=",",
        skiprows=0,
        dateformat=None,
        comment="#",
        nrows=nrows,
    )
    # Get rid of redundant entries caused by the time zone
    # f = ts.index.freq
    ts = ts.loc[~ts.index.duplicated(keep="first")]

    return ts



############################################################
def is_usgs1(fname):
    MAX_SCAN_LINE = 220
    tzline = False
    usgsline = False
    with open(fname, "r") as f:
        for i, line in enumerate(f):
            if i > MAX_SCAN_LINE:
                return False
            linelower = line.lower()
            if "waterdata.usgs.gov" in linelower:
                usgsline = True
            if "nwisweb" in linelower:
                usgsline = True
            if linelower.startswith("usgs"):
                usgsline = True
            if "tz_cd" in linelower:
                tzline = True
            if tzline and usgsline:
                return True

    return False


def usgs_data_columns1(fname):
    MAX_SCAN_LINE = 60
    import re

    description_re = re.compile(r"#\s+(ts|ts_id|dd)\s*(parameter)\s*description")
    description_re = re.compile(
        r"#\s+(ts|ts_id|dd)\s*(parameter)\s*?(statistic)?\s*?description"
    )
    colnames = []
    description = {}
    with open(fname, "r") as f:
        reading_cols = False
        use_statistic = False
        for i, line in enumerate(f):
            if i > MAX_SCAN_LINE:
                return False
            linelower = line.lower().strip()
            if not linelower.startswith("#"):
                raise ValueError(
                    "Column names could not be inferred in file: {}".format(fname)
                )
            if description_re.match(linelower):
                reading_cols = True
                if "statistic" in linelower:
                    use_statistic = True
                continue
            if reading_cols:
                import string

                if use_statistic:
                    try:
                        comment, ts_id, param, statistic, describe = linelower.split(
                            maxsplit=4
                        )
                        col_id = ts_id + "_" + param + "_" + statistic
                        colnames.append(col_id)
                        description[col_id] = describe.strip()
                    except:
                        return colnames
                else:
                    try:
                        comment, ts_id, param, describe = linelower.split(maxsplit=3)
                        col_id = ts_id + "_" + param
                        colnames.append(col_id)
                        description[col_id] = describe.strip()
                    except:
                        return colnames


def read_usgs1(
    fpath_pattern, start=None, end=None, selector=None, force_regular=True, nrows=None
):
    TZCOL = "tz_cd"
    if selector is None:
        selector = usgs_data_columns1
        qaselect = lambda x, y: x + "_cd"

    else:
        selector = listify(selector)
        qaselect = [x + "_cd" for x in selector]

    dtypes = {TZCOL: str}

    # See https://help.waterdata.usgs.gov/codes-and-parameters/instantaneous-and-daily-value-status-codes
    status_codes = [
        "Fld",
        "Eqp",
        "Dis",
        "Mnt",
        "Ssn",
        "Dry",
        "Pr",
        "Pmp",
        "Rat",
        "Ssn",
        "Zfl",
        "Tst",
        "***",
    ]

    # Now tack on time zone at the end
    ts = csv_retrieve_ts(
        fpath_pattern,
        start,
        end,
        force_regular,
        selector=selector,
        format_compatible_fn=is_usgs1,
        qaqc_selector=qaselect,
        qaqc_accept=[
            "",
            " ",
            " ",
            "A",
            "P",
            "A:[99]",
            "A:[91]",
            "A:R",
            "Approved",
            "e",
        ],
        extra_na=status_codes,
        extra_cols="tz_cd",
        parsedates=["datetime"],
        indexcol="datetime",
        header=0,
        sep="\t",
        skiprows="count",
        dateformat=None,
        comment="#",
        dtypes=dtypes,
        nrows=nrows,
    )
    f_orig = ts.index.freq
    # todo: hardwired from PST (though the intent is easily generalized)
    # note there is some bugginess to this. See SO post:
    # https://stackoverflow.com/questions/57714830/convert-from-naive-local-daylight-time-to-naive-local-standard-time-in-pandas
    dst = ts[TZCOL] == "PDT"
    if dst.any():
        # todo: is backward a good choice?
        ts.index = ts.index.tz_localize(
            "US/Pacific", ambiguous=ts["tz_cd"] == "PDT", nonexistent="shift_backward"
        ).tz_convert("Etc/GMT+8")
        ts.index = ts.index.tz_localize(None)

    # Get rid of the time zone column
    ts = ts.drop(TZCOL, axis=1)
    # Get rid of redundant entries caused by the time zone
    # f = ts.index.freq
    ts = ts.loc[~ts.index.duplicated(keep="first")]
    if f_orig is not None:
        ts = ts.asfreq(f_orig)
    return ts


##############################################################
def is_usgs1_daily(fname):
    MAX_SCAN_LINE = 220

    tzline = False
    usgsline = False
    agencyline = False
    with open(fname, "r") as f:
        for i, line in enumerate(f):
            if i > MAX_SCAN_LINE:
                return False
            linelower = line.lower()
            if "waterdata.usgs.gov" in linelower:
                usgsline = True
            if "nwisweb" in linelower:
                usgsline = True
            if linelower.startswith("usgs"):
                usgsline = True
            if "agency_cd" in linelower:
                agencyline = True
            if "tz_cd" in linelower:
                return False  # Have to bail on this if it is daily before we test for presence of others
            if usgsline and agencyline:
                return True

    return False


def read_usgs1_daily(
    fpath_pattern, start=None, end=None, selector=None, force_regular=True, nrows=None
):
    if selector is None:
        selector = usgs_data_columns1
        qaselect = lambda x, y: x + "_cd"

    else:
        selector = listify(selector)
        qaselect = [x + "_cd" for x in selector]

    # Now tack on time zone at the end
    ts = csv_retrieve_ts(
        fpath_pattern,
        start,
        end,
        force_regular,
        selector=selector,
        format_compatible_fn=is_usgs1_daily,
        qaqc_selector=qaselect,
        qaqc_accept=[
            "",
            " ",
            " ",
            "A",
            "P",
            "A:[99]",
            "A:[91]",
            "A:R",
            "Approved",
            "e",
            "A:e",
        ],
        parsedates=["datetime"],
        indexcol="datetime",
        header=0,
        sep="\t",
        skiprows="count",
        dateformat=None,
        comment="#",
        nrows=nrows,
    )

    return ts


################################################################
def is_usgs2(fname):
    MAX_SCAN_LINE = 210
    tzline = False
    usgsline = False
    with open(fname, "r") as f:
        for i, line in enumerate(f):
            if i > MAX_SCAN_LINE:
                return False
            linelower = line.lower()
            if "# //united states geological survey" in linelower:
                usgsline = True
            if "nwis-i unit-values" in linelower:
                usgsline = True
            if "tzcd" in linelower:
                tzline = True
            if tzline and usgsline:
                return True
    return False


def read_usgs2(
    fpath_pattern, start=None, end=None, selector=None, force_regular=True, nrows=None
):
    tzcol = "TZCD"
    if selector is None:
        selector = "VALUE"
    selector = listify(selector)
    qaselect = listify("QA")
    dtypes = dict.fromkeys(selector, float)
    dtypes.update(dict.fromkeys(qaselect, str))
    selector.append(tzcol)
    dtypes[tzcol] = "str"

    # Now tack on time zone at the end
    ts = csv_retrieve_ts(
        fpath_pattern,
        start,
        end,
        force_regular,
        selector=selector,
        format_compatible_fn=is_usgs2,
        qaqc_selector=qaselect,
        qaqc_accept=["", " ", " ", "A", "P", "A:[99]", "A:R", "Approved"],
        parsedates=[["DATE", "TIME"]],
        indexcol="DATE_TIME",
        header=0,
        sep="\t",
        skiprows="count",
        comment="#",
        dtypes=dtypes,
        nrows=nrows,
    )
    # todo: hardwired from PST (though the intent is easily generalized)
    # note there is some bugginess to this. See SO post:
    # https://stackoverflow.com/questions/57714830/convert-from-naive-local-daylight-time-to-naive-local-standard-time-in-pandas
    dst = ts[tzcol] == "PDT"
    if dst.any():
        # todo: hardwire
        ts.index = ts.index.tz_localize(
            "US/Pacific", ambiguous=ts[tzcol] == "PDT"
        ).tz_convert("Etc/GMT+8")
        ts.index = ts.index.tz_localize(None)

    # Get rid of the time zone column
    ts = ts.drop(tzcol, axis=1)

    # Get rid of redundant entries caused by the time zone
    ts = ts.loc[~ts.index.duplicated(keep="first")]

    return ts


######################
def is_usgs_csv1(fname):
    MAX_SCAN = 32
    sampleline = False
    with open(fname, "r") as f:
        for i, line in enumerate(f):
            if i > MAX_SCAN and not sampleline:
                return False
            linelower = line.lower()
            commented = linelower.startswith("#")
            if not commented:
                if linelower.startswith("iso"):
                    return sampleline
                else:
                    return False
            if "csv data starts at line" in linelower:
                sampleline = True
    return sampleline


def read_usgs_csv1(
    fpath_pattern, start=None, end=None, selector=None, force_regular=True, nrows=None
):
    if selector is None:
        selector = "Value"
    qaselect = ["Approval Level"]

    # Extra
    sep_eng = [(",", "c"), (", ", "python")]
    ts = None
    for sep, eng in sep_eng:
        try:
            ts = csv_retrieve_ts(
                fpath_pattern,
                start,
                end,
                force_regular,
                selector=selector,
                format_compatible_fn=is_usgs_csv1,
                qaqc_selector=qaselect,
                qaqc_accept=[
                    "",
                    " ",
                    " ",
                    "A",
                    "P",
                    "Approved",
                    "Working",
                    "A:[99]",
                    "A:R",
                ],
                parsedates=[1],
                indexcol=1,
                header=0,
                sep=sep,  # used to be sep=", " which broke one file
                engine=eng,
                skiprows=0,
                comment="#",
                dtypes={"Value": float, "Approval Level": str, "Qualifiers": str},
                nrows=nrows,
            )
            return ts
        except IndexError as e:
            if sep == ", ":
                raise
    return None  # should not actually get this far


##############################################


def is_noaa_file(fname):
    MAX_SCAN_LINE = 14
    with open(fname, "r") as f:
        for i, line in enumerate(f):
            if i > MAX_SCAN_LINE:
                return False
            linelower = line.lower()
            if "agency" in linelower and "noaa" in linelower:
                return True
            if "x, n, r" in linelower or "o, f, r, l" in linelower:
                return True
    return False


def noaa_data_column(fname):
    MAX_SCAN_LINE = 60
    with open(fname, "r") as f:
        reading_cols = False
        for i, line in enumerate(f):
            if i > MAX_SCAN_LINE:
                raise ValueError(
                    "Could not determine data columns within MAX_SCAN lines"
                )
            if not line.startswith("#"):
                parts = [p.strip() for p in line.split(",")]
                return [parts[1]]


def noaa_qaqc_selector(selector, fname):
    MAX_SCAN_LINE = 60
    with open(fname, "r") as f:
        reading_cols = False
        for i, line in enumerate(f):
            if i > MAX_SCAN_LINE:
                raise ValueError(
                    "Could not determine qaqc columns within MAX_SCAN lines"
                )
            if not line.startswith("#"):
                if line.startswith("Date"):
                    if "Prediction" in line:
                        return None
                    parts = [p.strip() for p in line.split(",")]
                    if len(parts) < 2:
                        continue
                    if parts[2] == "X":
                        return "X"
                    if parts[-1] == "Quality":
                        return "Quality"
                else:
                    raise ValueError(
                        "Quality labels in file not known: {}".format(fname)
                    )


def read_noaa(
    fpath_pattern, start=None, end=None, selector=None, force_regular=True, nrows=None
):
    ts = csv_retrieve_ts(
        fpath_pattern,
        start,
        end,
        force_regular,
        selector=noaa_data_column,
        format_compatible_fn=is_noaa_file,
        qaqc_selector=noaa_qaqc_selector,
        qaqc_accept=["", " ", "0", "p", "v"],
        parsedates=["Date Time"],
        indexcol="Date Time",
        header=0,
        sep=",",
        comment="#",
        nrows=nrows,
    )
    return ts

# This reader must be last
def read_last_resort_csv(fpath_pattern, start=None, end=None, selector=None, force_regular=False,nrows=None):
    ts = csv_retrieve_ts(
        fpath_pattern,
        start,
        end,
        force_regular,
        selector=None,
        format_compatible_fn=lambda x: True,
        qaqc_selector=None,
        parsedates=[0],
        indexcol=0,
        header=0,
        sep=",",
        comment="#",
        nrows=nrows,
    )
    return ts        
    



def vtide_date_parser(*args):
    x = args[0] + "T" + args[1] if len(args) == 2 else args
    return dtm.datetime.strptime(x, "%Y%m%dT%H%M")


def read_vtide(fpath_pattern, start=None, end=None, selector=None, force_regular=False):
    ts = csv_retrieve_ts(
        fpath_pattern,
        start,
        end,
        force_regular,
        selector=selector,
        format_compatible_fn=lambda x: True,
        qaqc_selector=None,
        parsedates=[0],
        indexcol=0,
        header=None,
        sep="\s+",
        comment="#",
    )

    return ts


def read_ts(
    fpath,
    start=None,
    end=None,
    force_regular=True,
    nrows=None,
    selector=None,
    hint=None,
):
    """Read a time series from a text file in various formats.
    This function asks readers for different file formats to attempt to read the file.
    The first reader that confirms its appropriateness will be attempted. The order of this
    is not guaranteed..

    Parameters
    ----------
    fpath: str
        a file path to read in
    start: datetime.datetime, optional
        time to start reading
    end: datetime.datetime, optional
        time to end reading
    force_regular : bool
        coerce the series to regular, which will align times neatly and discard oversampling
    nrows : int
        max number of rows to read. this is good for testing big files
    selector : str
        column label with file to extract
    hint : str
        first few characters of reader to use, such as 'cdec' or 'nwis'. Speeds the read.

    Returns
    -------
    ts : pandas.DataFrame
    Dataframe representing the series. If you want to squeeze univariate, need to do this on return

    """
    from os.path import split as op_split

    readers = [
        read_usgs_json1,  # must precede dms
        read_dms1_screen,
        read_dms1,
        read_usgs1,
        read_usgs2,
        read_usgs_csv1,
        read_usgs1_daily,
        read_noaa,
        read_des,
        read_des_std,
        read_cdec1,
        read_cdec2,
        read_ncro_std,
        read_wdl3,
        read_wdl2,
        read_wdl,
        read_last_resort_csv,
    ]
    ts = None
    reader_count = 0
    last_reader_tried = None
    for reader in readers:
        last_reader_tried = reader.__name__
        if hint is not None:
            if hint not in reader.__name__:
                continue
        try:
            ts = reader(
                fpath, start, end, selector, force_regular=force_regular, nrows=nrows
            )
            return ts
        except IOError as e:
            if str(e).startswith("No match"):
                print(e)
                break
            continue
        except Exception as e0:
            raise

    if ts is None:
        if last_reader_tried == readers[-1].__name__:
            last_reader_tried = (
                last_reader_tried
                + " (this is the last on the list, so it may not be meaningful)"
            )
        files = glob.glob(fpath)
        found_some = len(files) > 0
        found_txt = f"Can confirm found file(s) from pattern (false negative possible): {found_some}"
        raise ValueError(
            "File not found, format not supported or error during read: {}. Last reader tried = {}\n{}\n".format(
                fpath, last_reader_tried, found_txt
            )
        )


def write_ts():
    pass


def write_vtide():
    pass


def listify(inp):
    if isinstance(inp, str):
        inp = [inp]
    else:
        try:
            iter(inp)
        except TypeError:
            inp = [inp]
        else:
            inp = list(inp)
    return inp


def remove_isolated(ts, thresh):
    goodloc = np.where(np.isnan(ts.data), 0, 1)
    repeatgood = np.apply_along_axis(rep_size, 0, goodloc)
    isogood = (goodloc == 1) & (repeatgood < thresh)
    out = ts.copy()
    out.data[isogood] = np.nan
    return out


def count_comments(fname, comment):
    ncomment = 0
    with open(fname, "r") as f:
        while f.readline().startswith("#"):
            ncomment += 1
    return ncomment


def path_pattern(path_pattern):
    from os.path import split as opsplit

    if isinstance(path_pattern, str):
        fdir, fpat = opsplit(path_pattern)
    else:
        fdir, fpat = path_pattern
    return fdir, fpat


def infer_freq_robust(index, preferred=["h", "15min", "6min", "10min", "h", "d"]):
    index = index.round("1min")

    if len(index) < 8:
        # not enough to quibble, use the 8 points
        f = pd.infer_freq(index)
    else:
        f = pd.infer_freq(index[-7:-1])

        if f is None:
            index = index.round("1min")
            istrt = 3 * len(index) // 4
            f = pd.infer_freq(index[istrt : istrt + 7])
        if f is None:
            # Give it one more shot halfway through
            istrt = len(index) // 2
            f = pd.infer_freq(index[istrt : istrt + 7])
        if f is None:
            f = pd.infer_freq(index[0:7])
        if f is None:
            for p in preferred:
                freq = pd.tseries.frequencies.to_offset(p)
                tester = index.round(p)
                diff = (index - tester) < (freq / 5)
                frac = diff.mean()
                if frac > 0.98:
                    return p
        if f is None:
            raise ValueError(
                "read_ts set to infer frequency, but multiple attempts failed. Set to string to manually "
            )
        return f


def csv_retrieve_ts(
    fpath_pattern,
    start,
    end,
    force_regular=True,
    selector=None,
    format_compatible_fn=lambda x: False,
    qaqc_selector=None,
    qaqc_accept=["G", "U"],
    extra_cols=None,
    parsedates=None,
    indexcol=0,
    skiprows=0,
    header=0,
    dateformat=None,
    comment=None,
    sep="/s+",
    extra_na=[
        "m",
        "---",
        "",
        " ",
        "Eqp",
        "Ssn",
        "Dis",
        "Mnt",
        "***",
        "ART",
        "BRT",
        "ZFL",
    ],
    blank_qaqc_good=True,
    prefer_age="new",
    column_names=None,
    replace_names=None,
    dtypes=None,
    freq="infer",
    nrows=None,
    **kwargs,
):
    import os
    import fnmatch
    import glob

    fdir, fpat = path_pattern(fpath_pattern)

    if not fdir:
        fdir = "."
    matches = []
    # for root, dirnames, filenames in os.walk(fdir):
    #    for filename in fnmatch.filter(filenames, fpat):
    #        matches.append(os.path.join(root, filename))
    matches = glob.glob(fpath_pattern)
    matches.sort()

    if len(matches) == 0:
        raise IOError("No matches found for pattern: {}".format(fpat))

    if not format_compatible_fn(matches[0]):
        raise IOError(
            "Format not compatible for file {} ({})".format(
                matches[0], format_compatible_fn.__name__
            )
        )

    # parsetime = lambda x: pd.datetime.strptime(x, '%Y-%m-%d%H%M')
    tsm = []

    if prefer_age != "new":
        raise NotImplementedError("Haven't implemented prefer = 'old' yet")

    if callable(selector):
        selector = selector(matches[0])
    if selector is not None:
        selector = listify(selector)

    # Most implementations try hard not to have selector==None, but dms1 uses it
    if callable(qaqc_selector):
        # todo: this is not efficient for noaa because it keep opening files
        qaqc_selector = [qaqc_selector(x, matches[0]) for x in selector]
        if not any(qaqc_selector):
            qaqc_selector = None
    elif qaqc_selector is not None:
        qaqc_selector = listify(qaqc_selector)
    if extra_cols is not None:
        selector.append(extra_cols)

    # This essentially forces the client code to define dtypes for all
    # complex cases. It correctly handles the situation where all the
    # items in selector are floats, all the items in qaqc_selector are alphanumeric
    coltypes = {} if dtypes is None else dtypes.copy()
    if len(coltypes.keys()) == 0 and selector is None:
        # None provided. Seems like this might fail for non-data columns if included
        coltypes = float
    else:
        if qaqc_selector is None:
            for s in selector:
                if not s in coltypes:
                    coltypes[s] = float
        else:
            # Both selector and qaqc_selector provided
            # This behaves OK if these are paired selectors and the pairs come first
            # before any extras in selector as with USGS and tz_cd
            for s, qs in zip(selector, qaqc_selector):
                if not s in coltypes:
                    coltypes[s] = float
                if not qs in coltypes:
                    coltypes[qs] = str

    # The matches are in lexicographical order. Reversing them puts the newer ones
    # higher priority than the older ones for merging
    matches.reverse()
    if len(matches) == 0:
        raise ValueError(
            "No matches to file pattern {} in directory {}".format(fpat, fdir)
        )
    for m in matches:
        dargs = kwargs.copy()
        if comment is not None:
            dargs["comment"] = comment
        # if not na_values is None: dargs["na_values"] = na_values

        if skiprows == "count":
            ncomment = count_comments(m, comment)
            skiprows_spec = list(range(ncomment)) + [ncomment + 1]
        else:
            skiprows_spec = skiprows

        dset = None # to prevent holdover in memory
        if column_names is None:
            # warnings.filterwarnings('error')
            # try:
            dset = pd.read_csv(
                m,
                index_col=indexcol,
                header=header,
                skiprows=skiprows_spec,
                sep=sep,
                parse_dates=parsedates,
                date_format=dateformat,
                na_values=extra_na,
                keep_default_na=True,
                dtype=coltypes,
                skipinitialspace=True,
                nrows=nrows,
                **dargs,
            )
            # print(dset.tail())
            # except:
            #    print(f"Warning for: {m}")
            #    # reformat --inpath raw --outpath formatted

            if header is None:
                # This is essentially a fixup for vtide, which I'm not
                # too happy about ... we should have a header and only one
                # choice of format for this tool
                # Assume the parser could not assign column names
                # and used Int64Index
                dset.columns = [str(x - 1) for x in dset.columns]
                dset.index.name = "datetime"
                if selector is None or selector == [None]:
                    selector = dset.columns
            dset.columns = [x.strip() for x in dset.columns]

        else:
            dset = pd.read_csv(
                m,
                index_col=indexcol,
                header=header,
                skiprows=skiprows_spec,
                sep=sep,
                parse_dates=parsedates,
                date_format=dateformat,
                na_values=extra_na,
                keep_default_na=True,
                dtype=coltypes,
                names=column_names,
                skipinitialspace=True,
                nrows=nrows,
                **dargs,
            )
        
        if qaqc_selector is not None:
            # It is costly to try to handle blanks differently for both data
            # (for which we usually want blanks to be NaN and alphanumeric flags.
            if blank_qaqc_good:
                if np.nan not in qaqc_accept:
                    qaqc_accept.append(np.nan)
                if "" not in qaqc_accept:
                    qaqc_accept.append("")
                
            for v, f in zip(selector, qaqc_selector):
                accept = dset[f].astype(str).str.strip().isin(qaqc_accept)
                if blank_qaqc_good:
                    accept |=  dset[f].isnull()
                    dset.loc[~accept, v] = np.nan
                   
        if selector is None:
            tsm.append(dset)
        else:
            tsm.append(dset[selector])
    big_ts = ts_merge(tsm)  # pd.concat(tsm)
    if force_regular:
        if freq == "infer":
            f = infer_freq_robust(big_ts.index)

        else:
            f = pd.tseries.frequencies.to_offset(freq)
        if f is None:
            raise NotImplementedError("force_regular but could not discover freq")
        # Round to neat times, which may cause duplicates or gaps
        big_ts.index = big_ts.index.round(f)
        big_ts = big_ts.loc[~big_ts.index.duplicated(keep="first")]
        # Now everything is on an expected timestamp, so subsample leaving uncovered times NaN
        big_ts = big_ts.asfreq(f)

    # This try/except Ensures frame rather than Series
    num_null_index = big_ts.index.isnull().sum()
    if num_null_index > 1:
           raise ValueError("Multiple nan values found in index")
    else:
        if num_null_index == 1: 
            big_ts = big_ts.loc[big_ts.index.notnull(),:]   
    if start is None:
        start = big_ts.index[0]
    if end is None:
        end = big_ts.index[-1]
    try:
        retval = big_ts.to_frame().loc[start:end, :]
    except:
        retval = big_ts.loc[start:end, :]
    # if hasattr(big_ts,"freq"): retval = retval.asfreq(big_ts.freq)
    return retval
