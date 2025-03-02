from ._version import get_versions
__version__ = get_versions()['version']
del get_versions

from . import _version
__version__ = _version.get_versions()['version']

from dms_datastore.read_ts import *
from dms_datastore.write_ts import write_ts_csv
from dms_datastore.read_multi import *