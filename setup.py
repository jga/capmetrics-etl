#!/usr/bin/env python3
import os
from setuptools import setup, find_packages


def get_readme():
    return open(os.path.join(os.path.dirname(__file__), 'README.rst')).read()

setup(
    author="Julio Gonzalez Altamirano",
    author_email='devjga@gmail.com',
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3 :: Only',
    ],
    description="ETL for CapMetro raw data.",
    entry_points={
        'console_scripts': [
            'etl=capmetrics-etl.cli:run',
        ],
    },
    install_requires=['click', 'pytz', 'sqlalchemy', 'xlrd'],
    keywords="python etl transit",
    license="MIT",
    long_description=get_readme(),
    name='capmetrics-etl',
    package_data={
        'capmetrics-etl': ['templates/*.html'],
    },
    packages=find_packages(include=['capmetrics-etl', 'capmetrics-etl.*'],
                           exclude=['test', 'test.*']),
    platforms=['any'],
    url='https://github.com/jga/capmetrics-etl',
    version='0.1.0'
)