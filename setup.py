from setuptools import setup, find_packages
import versioneer

with open("README.rst") as readme_file:
    readme = readme_file.read()
requirements = [
    "vtools3",
    "yaml",
    "beautifulsoup4",
    "requests",
    "lxml",
    "pandas>=2",
    "xarray",
    "dask",
    "scikit-learn",
    "matplotlib",
    "matplotlib",
    "cfgrib",
    "diskcache",
]
setup_requirements = ["pytest-runner", "pandoc"]
test_requirements = ["pytest", "pandoc"]

setup(
    name="dms_datastore",
    version=versioneer.get_version(),
    license="MIT",
    cmdclass=versioneer.get_cmdclass(),
    description="Downloading tools and data repository management",
    long_description=readme,
    install_requires=requirements,
    # extras_require=extras,
    include_package_data=True,
    packages=find_packages(),
    package_data={
        "dms_datastore": [
            "config_data/*.csv",
            "config_data/*.yaml",
            "usgs_parameter_cd_query.txt",
            "config_data/*.sh*",
            "config_data/*.prj",
            "config_data/*.dbf",
            "config_data/*.txt",
        ]
    },
    # setup_requires=setup_requirements,
    test_suite="tests",
    # tests_require=test_requirements,
    author="Eli Ateljevich",
    author_email="Eli.Ateljevich@water.ca.gov",
    url="https://github.com/water-e/dms_datastore",
    keywords=["dms_datastore", "data", "repository", "download"],
    classifiers=[
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    entry_points={
        "console_scripts": [
            "download_hycom=dms_datastore.download_hycom:main",
            "download_hrrr=dms_datastore.download_hrrr:main",
            "download_noaa=dms_datastore.download_noaa:main",
            "download_cdec=dms_datastore.download_cdec:main",
            "download_wdl=dms_datastore.download_wdl:main",
            "download_nwis=dms_datastore.download_nwis:main",
            "download_des=dms_datastore.download_des:main",
            "download_ncro=dms_datastore.download_ncro:main",
            "download_mokelumne=dms_datastore.download_mokelumne:main",
            "compare_directories=dms_datastore.compare_directories:main",
            "populate_repo=dms_datastore.populate_repo:main",
            "station_info=dms_datastore.station_info:main",
            "reformat=dms_datastore.reformat:main",
            "auto_screen=dms_datastore.auto_screen:main",
            "inventory=dms_datastore.inventory:main",
            "usgs_multi=dms_datastore.usgs_multi:main",
            "delete_from_filelist=dms_datastore.delete_from_filelist:main",
            "data_cache=dms_datastore.caching:main",
        ]
    },
)
