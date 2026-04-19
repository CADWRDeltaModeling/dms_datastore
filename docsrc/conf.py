# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
# import os
# import sys
# sys.path.insert(0, os.path.abspath('.'))


# -- Project information -----------------------------------------------------
import sys
import os
from unittest.mock import MagicMock

# Mock heavy/unavailable dependencies before importing dms_datastore,
# so conf.py works in the CI docs environment without the full conda env.
_MOCK_MODULES = [
    "vtools", "vtools.data", "vtools.data.indexing", "vtools.data.vtime",
    "vtools.data.timeseries", "vtools.data.gap", "vtools.data.duplicate_index",
    "vtools.functions", "vtools.functions.merge", "vtools.functions.filter",
    "vtools.functions.coarsen", "vtools.functions.error_detect",
    "vtools.functions.unit_conversions",
    "schimpy", "schimpy.station", "schimpy.unit_conversions",
    "tabula",
    "geopandas",
    "shapely", "shapely.geometry",
    "diskcache",
    "seaborn",
    "dask", "dask.dataframe",
    "paramiko",
    "boto3",
    "cfgrib",
    "eccodes",
    "numba",
]
for _mod in _MOCK_MODULES:
    sys.modules.setdefault(_mod, MagicMock())

import dms_datastore

project = 'dms_datastore'
copyright = '2022, Eli Ateljevich, CA DWR'
author = 'Eli Ateljevich, CA DWR'

# The full version, including alpha/beta/rc tags
release = '0.0.1'

if not os.path.exists("docs"): 
    os.makedirs("docs")

html_static_path = ['_static']
#html_css_files = ['theme_overrides.css']
#html_context = {
#    'css_files': [
#        '_static/theme_overrides.css',  # override wide tables in RTD theme
#        ],
#     }



# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
extensions = [
    'sphinx_rtd_theme',
    'nbsphinx',
    'sphinxcontrib.mermaid',
    'sphinx.ext.mathjax',
    'sphinx.ext.autodoc',
    'sphinx.ext.viewcode',
    'matplotlib.sphinxext.mathmpl',
    'matplotlib.sphinxext.plot_directive',
    'sphinx.ext.intersphinx',
    'sphinx.ext.autodoc',
    #'sphinx_argparse_cli',
    'sphinxarg.ext',
    'sphinx_click',
    'sphinx.ext.doctest',
    'numpydoc',
]

autodoc_member_order = 'alphabetical'

# Mock heavy/optional dependencies so docs can build without the full environment
autodoc_mock_imports = [
    "tabula",
    "vtools",
    "schimpy",
    "geopandas",
    "shapely",
    "diskcache",
    "seaborn",
    "dask",
    "paramiko",
    "boto3",
    "cfgrib",
    "eccodes",
    "numba",
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

intersphinx_mapping = {'pandas': ('http://pandas.pydata.org/pandas-docs/stable',None),
 'python': ('http://docs.python.org/', None),
 'xarray' : ('http://xarray.pydata.org/en/stable',None),
 'vtools3' : ('https://cadwrdeltamodeling.github.io/vtools3/html/',None)
 }
source_suffix = '.rst'
master_doc = 'index'
version = dms_datastore.__version__
release = dms_datastore.__version__
# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
exclude_patterns = ['*test*','_build','**.ipynb_checkpoints']
add_module_names = False
# This pattern also affects html_static_path and html_extra_path.
pygments_style = 'sphinx'
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
#html_theme = 'alabaster'
html_theme = "sphinx_rtd_theme"
#html_theme_path = ["_themes", ]

#html_theme_options = {
#    'logo': 'dwrsmall.gif'}

#html_logo = 'dwrsmall.gif'
# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']
html_domain_indices = ['py-modindex']
htmlhelp_basename = 'dms_datastore_doc'
latex_documents = [
    (master_doc, 'dms_datastore.tex',
     u'vtools Documentation',
     u'Eli Ateljevich, Kijin Nam, Nicky Sandhu', 'manual'),
]
