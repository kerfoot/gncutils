from setuptools import setup, find_packages
from gncutils import __version__

def readme():
    with open('README.md') as f:
        return f.read()

reqs = [line.strip() for line in open('requirements.txt')]

setup(
    name="gncutils",
    version=__version__,
    description="Python utilities for writing trajectory DSG NetCDF files from raw Slocum glider data files",
    long_description=readme(),
    packages=find_packages(),
    author="John Kerfoot",
    author_email="johnkerfoot@gmail.com",
    license="GPLv3",
    install_requires=reqs,
    tests_require=['pytest'],
)
