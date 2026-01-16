__author__ = """Eli Ateljevich, Nicky Sandhu"""
__email__ = "Eli.Ateljevich@water.ca.gov, psandhu@water.ca.gov"

try:
    from importlib.metadata import version, PackageNotFoundError
except ImportError:
    from pkg_resources import get_distribution, DistributionNotFound

    def version(pkg):
        return get_distribution(pkg).version

    PackageNotFoundError = DistributionNotFound

try:
    from ._version import version as __version__
except ImportError:
    from setuptools_scm import get_version
    __version__ = get_version(root='..', relative_to=__file__)

from dms_datastore.read_multi import read_ts_repo
from dms_datastore.read_ts import *
from dms_datastore.write_ts import write_ts_csv
import logging
logging.getLogger(__name__).addHandler(logging.NullHandler())