from setuptools_scm import get_version
from setuptools import setup

# setup.py only needed for conda to resolve versioning
# DO NOT ADD ANYTHING ELSE HERE

setup(
    name="dms_datastore",
    version=get_version(),
)
