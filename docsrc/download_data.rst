
######################
Downloading Utilities
######################



CDEC
====
.. click:: dms_datastore.download_cdec:download_cdec_cli
    :prog: download_cdec



USGS (NWIS)
===========

.. click:: dms_datastore.download_nwis:download_nwis_cli
    :prog: download_nwis

NOAA
====

.. click:: dms_datastore.download_noaa:download_noaa_cli
    :prog: download_noaa

DWR-NCRO
========

This utility allows downloading data from the DWR-NCRO web services. They have expressed a concern about overuse during peak hours when the services are used by staff to work on data.

.. click:: dms_datastore.download_ncro:download_ncro_cli
    :prog: download_ncro

DWR-DES (DISE)
==============

DWR Division of Environmental Services has been renamed but the name has not yet been updated here. 
These web services are internal to DWR. External users can get reformatted/screened data from our group or request data 
from DWR liasons who are identified on CDEC on a per-station basis.

.. click:: dms_datastore.download_des:download_des_cli
    :prog: download_des

