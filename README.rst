===============================
dms_datastore
===============================

Downloading tools and data repository management. This repository is a work in progress. It is not recommended for any purpose while it is under construction.



There are improvements that are needed to the downloading system tools.

1. The downloading scripts should be called download_XXX.py where XXX is noaa, nwis, cdec etc.
2. The API should be made uniform between these scripts. 
   a. It should be able to use our new id or the agency_id with the new id preferred
   b. It should be able to use our variable name or the agency variable code
   c. It should produce files that are named according to the file naming convention in the data plan: http://msb-confluence/display/DMKB/Strawman+Data+Organization+Plan:

usgs_sjj@bgc_11337190_turbidity_2021.csv

This is all potentially destabilizing, so perhaps it should be done on a shortlived branch

The station files don't have a uniform format. I prefer all look like this:
id,agency_id,subloc,variable
sjj,11337190,bgc,turbidity

The agency_id column is optional. 

===============================
Installation
===============================

```
git clone https://github.com/CADWRDeltaModeling/dms_datastore
conda env create -f environment.yml # should create a dms_datastore and pip install the package
# alternatively, pip install -e . after running the above command if you want to develop the package
conda activate dms_datastore
```

