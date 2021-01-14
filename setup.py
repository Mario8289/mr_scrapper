#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup
from setuptools import find_packages

requirements = [
    'pandas',
    'bs4',
    'lxml'
    'pyarrow',
    'numpy',
    'requests',
    'PyMySQL',
    'sqlalchemy',
    'click',
    's3fs',
    'pyyaml',
    'regex',
    'PyMySQL',
    'click',
    'Sphinx',
    'coverage',
    'awscli',
    'flake8',
    'python-dotenv>=0.5.1',
    'scipy',
    'bokeh',
    'matplotlib',
    'pyspark',
    'pyyaml',
    'ipyleaflet',
    'fiona',
    'rtree',
    'shapely',
    'pyproj',
    'geopandas',
    'scikit-learn',
    'scipy',
    'networkx',
    'rasterio',
    'rasterstats',
    'folium',
    'osmnx',
    'jupyterlab',
    'geopy'
]

setup_requirements = []


setup(
    name='mr_scrapper',
    version='0.0.1',
    description='This package provides an interface to scrap and write date using scraping jobs',
    url='https://git.yb.lmax/analytics/lmax_analytics_notebooks.git',
    author="Anthony Marriott",
    author_email='amarriott289@gmail.com',
    packages=find_packages(include=['mr_scrapper', 'mr_scrapper.*']),
    install_requires=requirements,
    zip_safe=False,
)