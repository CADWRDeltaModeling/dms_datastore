
################
Downloading Data
################


Station Lists
=============







Specific Command Line Interfaces
================================

CDEC
^^^^
.. click:: dms_datastore.download_cdec:download_cdec_cli
    :prog: download_cdec



USGS (NWIS)
^^^^^^^^^^^

.. click:: dms_datastore.download_nwis:download_nwis_cli
    :prog: download_nwis

NOAA
^^^^

.. click:: dms_datastore.download_noaa:download_noaa_cli
    :prog: download_noaa

DWR-NCRO
^^^^^^^^

It has traditionally been difficult to write robots to scrape data from Water Data Library. 
The utility download_wdl is included but is not maintained and may fail to function or to retrieve
recent files. There are newer web services (public) that we are testing, but NCRO has 
expressed some concern about overuse during peak hours when the services are used by staff to work on data.
The current interface shown here is for downloading period of record files, 
so its interface is simpler, less flexible and different from the others.

#.. argparse::
#    :module: dms_datastore.download_ncro
#    :func: create_arg_parser
#    :prog: download_ncro.py
    
.. click:: dms_datastore.download_des:download_des_cli
    :prog: download_des
DWR-DES (DISE)
^^^^^^^^^^^^^^

DWR Division of Environmental Services has been renamed but the name has not yet been updated here. 
These web services are internal to DWR. External users can get reformatted/screened data from our group or request data 
from DWR liasons who are identified on CDEC on a per-station basis.
