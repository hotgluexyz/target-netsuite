#!/usr/bin/env python

from setuptools import find_packages, setup

setup(
    name='target-netsuite',
    version='1.0.43',
    description='hotglue target for exporting data to NetSuite',
    author='hotglue',
    url='https://hotglue.xyz',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    python_requires='>=3.7.1',
    py_modules=['target_netsuite'],
    install_requires=[
        # 3.7: legacy pins. 3.8+ (incl. 3.10/3.14): modern stack.
        # Legacy numpy/pandas wheels segfault on import under macOS 26 arm64 on 3.10.
        'requests==2.20.0; python_version < "3.12"',
        'numpy>=1.17.3,<1.22; python_version < "3.8"',
        'pandas==1.3.4; python_version < "3.8"',
        
        'requests>=2.32.4; python_version >= "3.12"',
        'numpy>=1.26.0; python_version >= "3.8"',
        'pandas>=2.3.3; python_version >= "3.8"',
        'argparse==1.4.0',
        'netsuitesdk==3.1.2',
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
