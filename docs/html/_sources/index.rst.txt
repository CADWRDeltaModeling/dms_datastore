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







Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
