from setuptools import setup, find_packages

setup(
    name="gncutils",
    version="1.0",
    packages=find_packages(),
    author="John Kerfoot",
    author_email="johnkerfoot@gmail.com",
    license="GPLv3",
    description="Python utilities for writing trajectory DSG NetCDF files from raw Slocum glider data files",
    install_requires=["netcdf4",
        "shapely >= 1.5.6",
        "numpy",
        "scipy",
        "python-dateutil",
        "gsw",
        "matplotlib"
    ],
)
