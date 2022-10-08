from setuptools import setup, find_packages
import versioneer

with open('README.rst') as readme_file:
    readme = readme_file.read()
requirements = [ "vtools3", 
                 "pandas",
]

setup(
    name='dms_datastore',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="Downloading tools and data repository management",
    license="BSD",
    long_description=readme,
    install_requires=requirements,
    #extras_require=extras,
    include_package_data=True,
    packages=find_packages(),
    author="Eli Ateljevich",
    author_email='Eli.Ateljevich@water.ca.gov',
    url='https://github.com/water-e/dms_datastore',
    keywords=['dms_datastore','data','repository','download'],
    classifiers=[
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9'			
    ],
    entry_points = { 'console_scripts' : ['download_noaa=dms_datastore.download_noaa:main',
                                          'download_cdec=dms_datastore.download_cdec:main',
                                          'download_wdl=dms_datastore.download_wdl:main',
                                          'download_nwis=dms_datastore.download_nwis:main',
                                          'download_des=dms_datastore.download_des:main',
                                          'compare_directories=dms_datastore.compare_directories:main',
                                          'populate_repo=dms_datastore.populate_repo:main',
                                          'station_info=dms_datastore.station_info:main']
										   }
)
