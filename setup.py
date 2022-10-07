from setuptools import setup
import versioneer

requirements = [ "vtools3", 
                 "pandas",
]

setup(
    name='dms_datastore',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="Downloading tools and data repository management",
    license="BSD",
    author="Eli Ateljevich",
    author_email='Eli.Ateljevich@water.ca.gov',
    url='https://github.com/water-e/dms_datastore',
    packages=['dms_datastore'],
    entry_points={
        'console_scripts': [
            'download_des=dms_data_tools.download_des:main'
        ]
    },
    install_requires=requirements,
    keywords='dms_datastore',
    classifiers=[
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',				
    ]
)
