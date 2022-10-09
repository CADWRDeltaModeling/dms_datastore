.. dms_datastore documentation master file, created by
   sphinx-quickstart on Sat Oct  8 13:51:22 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

dms_datastore!
==================

.. toctree::
   :maxdepth: 2
   :caption: Contents:

NOTE: THE DMS_DATASTORE IS STILL UNDER CONSTRUCTION. NO RELEASE HAS BEEN MADE

dms_datastore is a package for downloading and managing a repository of csv files of continuous time series data, 
mostly focused on environmental data for the Bay-Delta. It provides definitions of the concept of stations, methods of access, 
units and names in a way that can encapsulate many of the quirks of individual providers. 

The main functionality includes:

* Automatic downloading scripts along a mostly uniform interface for major data providers, most of which are public.
* Station lists and a command line utility to lookup information from the station lists.
* A multithreaded populating routine that orchestrates comprehensive downloads for the Bay-Delta into a repository.
* Unified routines for reading time series in multiple downloaded 
  formats while reconciling differences between providers (time zones, QA flags, timestamping conventions, etc). 
* Reformatting and alignment to repackage time series in a single Pandas-compatible common csv format with metadata headers.
* Screening routines that coordinate tools from vtools and elsewhere to provide basic checks on data.

Installation
------------

[NOTE: THE CONDA PART ISN'T TRUE YET] dms_datastore is on GitHub and available from Conda on the cadwrdms channel. 
... . Background is available here [REF].


Some Useful Things You Can Do Quickly with dms_datastore
---------------------------------------------------------------

Get station info
''''''''''''''''

Lookup information on a station using a fragment of its name, standard id.::

  $ station_info francisco
  Matches:
        station_id agency        agency_id                                                  name         x          y        lat         lon
  id
  alk          alk   usgs  374938122251801  San Francisco Bay at Northeast Shore Alcatraz Island  550895.2  4186802.8  37.827222 -122.421667
  dum          dum   usgs  373015122071000           South San Francisco Bay at Dumbarton Bridge  577828.8  4151167.7  37.504000 -122.119000
  dumbr      dumbr   usgs  373025122065901             San Francisco Bay at Old Dumbarton Bridge  578096.0  4151478.4  37.506944 -122.116389
  richb      richb   usgs  375607122264701       San Francisco Bay at Richmond-San Rafael Bridge  548648.5  4198778.5  37.935278 -122.446389
  sffpx      sffpx   noaa          9414290                                         San Francisco  547094.8  4184503.1  37.806700 -122.465000
  sfp17      sfp17   usgs  374811122235001                          San Francisco Bay at Pier 17  553143.4  4184169.8  37.803000 -122.397000

Download data
'''''''''''''

If you know the station id and agency (see above), you can get data for individual or groups of stations or even from a list::

  $ download_nwis --start 2022-01-01 --end 2022-06-01 --stations osj hol --param flow --dest .

Of course this can be done programatically as well. A discussion can be found here: [REF]

Read data
'''''''''

There are three main routines [actually 2 one in dev], `read_ts`, `read_multi.ts_multifile_read` and `ts_**`. All are based on how they deal with 
filenames that are wildcards to represent time sharding by years (file name ends in *_2020.csv) or blocks of years (*_2015_2019.csv):

* read_ts is mostly designed around heterogeneous legacy formats. It can read wildcard if the files are otherwise the same basic format.
* ts_multifile_read is a wrapper around read_ts that allows mixes of different formats. This would be useful if, for instance, historical 
  data comes from one source/format and real-time data from another.
* read_ts_multi assumes data is all in dms_datastore standard format and constructs multi-column views quickly.







You can download data quickly from 








Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
