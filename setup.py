#!/usr/bin/env python

from setuptools import find_packages, setup

setup(
    name='target-netsuite',
    version='1.0.23',
    description='hotglue target for exporting data to NetSuite',
    author='hotglue',
    url='https://hotglue.xyz',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['target_netsuite'],
    install_requires=[
        'requests==2.31.0',
        'pandas==1.3.4',
        'argparse==1.4.0',
        'netsuitesdk==2.7.0',
        'singer-python==5.12.2'
    ],
    entry_points='''
        [console_scripts]
        target-netsuite=target_netsuite:main
    ''',
    packages=find_packages(exclude=['tests']),
    package_data={
        'target_netsuite.netsuite': ['schemas/*.json']
    },
    include_package_data=True,
)
