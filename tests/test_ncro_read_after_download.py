from dms_datastore.reformat import *
from dms_datastore.read_ts import *
from dms_datastore.download_nwis import *
from dms_datastore.usgs_multi import *


def test_ncro_read_after_download():
    fpath = "W:/repo_staging/continuous/raw/ncro_*.csv"
    files = glob.glob(fpath)
    for fname in files:
        try:
            ts = read_ts(fname)
        except Exception as ex:
            print(f"Failed on {fname}")
            print(str(ex))

if __name__=='__main__':
    test_ncro_read_after_download()