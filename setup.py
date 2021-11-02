#!/usr/bin/env python

from setuptools import setup

setup(
    name='target-netsuite',
    version='1.0.0',
    description='hotglue target for exporting data to NetSuite',
    author='hotglue',
    url='https://hotglue.xyz',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['target_netsuite'],
    install_requires=[
        'requests==2.20.0',
        'pandas==1.1.3',
        'argparse==1.4.0'
    ],
    entry_points='''
        [console_scripts]
        target-netsuite=target_netsuite:main
    ''',
    packages=['target_netsuite']
)
