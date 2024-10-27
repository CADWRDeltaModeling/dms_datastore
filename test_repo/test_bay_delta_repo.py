import os
import glob
import pytest

# The purpose of this is to be able to retune the tests easily by 
# posing an impossible standard (say _ridiculous_increment = 100000)
# For regular testing set this to zero
_ridiculous_increment = 0


def test_raw_num_files(repo_raw):
    global _ridiculous_increment
    num_files = len(os.listdir(repo_raw))
    expected = 2000
    
    assert num_files > (expected+_ridiculous_increment),f"Expected > {expected} total files in /raw repository, found {num_files}"
    
@pytest.mark.parametrize("source,expected", [("cdec", 478),("ncro", 440), ("noaa", 25),("des",384),("usgs",680)])
def test_raw_agency_num_files(repo_raw,source,expected):
    global _ridiculous_increment
    num_files = len(glob.glob(os.path.join(repo_raw,f"{source}_*")))
    assert num_files >= (expected+_ridiculous_increment), f"Expected a minimum of {expected} files from source {source} in /raw repository, found {num_files}"
    
@pytest.mark.parametrize("source,expected", [("cdec", 2666), ("ncro",6900), ("noaa", 229),("des",2935),("usgs",5010)])
def test_formatted_agency_num_files(repo_formatted,source,expected):
    global _ridiculous_increment
    num_files = len(glob.glob(os.path.join(repo_formatted,f"{source}_*")))
    assert num_files >= (expected+_ridiculous_increment), f"Expected a minimum of {expected} files from source {source} in /formatted repository, found {num_files}"
    
    
    