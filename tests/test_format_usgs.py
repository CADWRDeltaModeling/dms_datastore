from dms_datastore.reformat import *
from dms_datastore.read_ts import *
from dms_datastore.download_nwis import *
from dms_datastore.usgs_multi import *


def test_nwis_download():
    stations = ["mok"]
    dest_dir = "data"
    start = pd.Timestamp(2020, 1, 1)
    param = "flow"
    overwrite = True
    stationfile = stationfile_or_stations(None, stations)
    slookup = dstore_config.config_file("station_dbase")
    vlookup = dstore_config.config_file("variable_mappings")
    df = process_station_list(
        stationfile,
        param=param,
        station_lookup=slookup,
        agency_id_col="agency_id",
        param_lookup=vlookup,
        source="usgs",
    )
    nwis_download(df, dest_dir, start, end=None, param=None, overwrite=False)


def test_reformat_usgs_json():
    return
    inpath = "w:/repo_staging/continuous/raw"
    # W:\repo_staging\continuous\quarantine
    outpath = "./data/out"
    pattern = ["usgs_fpt_11447650_temp_2020_*.csv"]
    pattern = ["usgs_dsj_11313433_flow*.csv"]
    pattern = ["usgs_benbr_11455780_turbidity_2000_2019.csv"]
    pattern = ["usgs_sjj_11337190_elev_2020_9999.csv"]
    outpath = "./data/out"
    reformat(inpath, outpath, pattern)


def test_reformat_cdec():
    return
    inpath = "w:/repo_staging/continuous/raw"
    pattern = ["cdec_*"]
    outpath = "./data/out"
    reformat(inpath, outpath, pattern)


def test_parse_json():
    return
    fpath = "W:/repo_staging/continuous/quarantine/usgs_benbr_11455780_turbidity_2000_2019.csv"
    parse_usgs_json(fpath, "junk.csv")


def test_usgs_multi():
    return
    fpath = "W:/repo_staging/continuous/formatted_usgs"
    process_multivariate_usgs(fpath, pat=None, rescan=True)


if __name__ == "__main__":
    # test_reformat_usgs_json()
    # test_parse_json()
    # test_nwis_download()
    # test_usgs_multi()
    # test_reformat_cdec()
    pass
